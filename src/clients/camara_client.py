import asyncio
import os
import random
import time
import aiohttp
from urllib.parse import urlparse, parse_qs
from email.utils import parsedate_to_datetime
from tenacity import retry, wait_exponential, retry_if_exception_type


# Configuração otimizada:
# - total: 90s para cada requisição individual
# - connect: 30s para estabelecer conexão
_TIMEOUT = aiohttp.ClientTimeout(total=90, connect=30)

# Status codes que justificam retry
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

# A API da Câmara limita a 10 req/s por IP (documentado em
# github.com/CamaraDosDeputados/dados-abertos/issues/251). Não há API key que
# aumente esse teto. Ficamos abaixo dele de propósito: medições mostraram que
# ultrapassar não aumenta throughput — colapsa. Com 2 tasks concorrentes o
# pipeline rendia 2,04x MENOS que serial, e gastava 6x mais 429s para isso.
_RATE_LIMIT_RPS = float(os.getenv("CAMARA_RATE_LIMIT_RPS", 8))

# O rate limiter é quem faz cumprir o contrato da API; o semáforo só evita um
# número ilimitado de requisições em voo. Para sustentar R req/s com latência
# L, é preciso concorrência >= R*L — com R=8 e L~2s, um semáforo de 6 vira o
# gargalo real e segura a taxa em ~3 req/s, sem o limiter sequer atuar.
_MAX_CONCURRENCY = int(os.getenv("CAMARA_MAX_CONCURRENCY", 16))

# 5xx consecutivos indicam que o backend está descartando carga (a tempestade
# de 504 em votacoes vinha da nossa própria pressão). Pausa curta para deixar
# respirar.
_SERVER_ERROR_TRIP_THRESHOLD = int(os.getenv("CAMARA_5XX_TRIP_THRESHOLD", 5))
_SERVER_ERROR_TRIP_SECONDS = 5.0

# Teto para a pausa global, evitando que um Retry-After absurdo trave tudo.
_MAX_PAUSE_SECONDS = 45.0


class CamaraRateLimitError(aiohttp.ClientError):
    """Exceção para rate-limit (429) ou respostas com Retry-After."""
    def __init__(self, message: str, retry_after: float = None):
        super().__init__(message)
        self.retry_after = retry_after


class CamaraServerError(aiohttp.ClientError):
    """Exceção para erros de servidor (5xx)."""
    pass


def _parse_retry_after(header_value: str = None) -> float:
    """Parse Retry-After header (seconds as int ou HTTP-date)."""
    if not header_value:
        return None

    try:
        # Tenta interpretar como número de segundos
        return float(header_value)
    except ValueError:
        pass

    try:
        # Tenta interpretar como HTTP-date
        dt = parsedate_to_datetime(header_value)
        now = __import__('datetime').datetime.now(__import__('datetime').timezone.utc)
        delta = (dt - now).total_seconds()
        return max(delta, 0)
    except (TypeError, ValueError):
        pass

    return None


def _camara_wait(retry_state) -> float:
    """Backoff local curto, com full jitter.

    A espera de verdade agora é feita uma única vez, globalmente, pelo
    CircuitBreaker. Aqui só evitamos que as corrotinas liberadas pelo breaker
    saiam todas no mesmo instante.

    Full jitter (`uniform(0, base)`) em vez do antigo `base + uniform(0, 2)`:
    com janela de 30s e jitter de apenas 2s, todas as corrotinas acordavam
    dentro da mesma janela de 2 segundos e colidiam de novo — foi o que
    produziu rajadas de até 108 rejeições consecutivas.
    """
    base = wait_exponential(multiplier=1, min=1, max=8)(retry_state)
    return random.uniform(0, base)


def _camara_stop(retry_state) -> bool:
    """Stop strategy com limite diferenciado para rate-limit vs erros de servidor."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None

    # 5 tentativas para rate-limit (esperado sob carga, vale insistir)
    # 3 tentativas para outros erros (falha real de servidor)
    limit = 5 if isinstance(exc, CamaraRateLimitError) else 3

    return retry_state.attempt_number >= limit


class RateLimiter:
    """Token bucket que espaça as requisições no tempo.

    O semáforo limita quantas requisições ficam *em voo*; isto limita a que
    *taxa* elas partem. Sem o segundo controle, 6 requisições em voo com
    respostas rápidas ainda estouram os 10 req/s.
    """

    def __init__(self, rate: float, burst: int = None):
        self.rate = rate
        self.burst = burst if burst is not None else max(1, int(rate))
        self._tokens = float(self.burst)
        self._updated = time.monotonic()
        self._lock = None

    async def acquire(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(
                    self.burst, self._tokens + (now - self._updated) * self.rate
                )
                self._updated = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                await asyncio.sleep((1 - self._tokens) / self.rate)


class CircuitBreaker:
    """Pausa global de todos os workers quando a API sinaliza sobrecarga.

    Antes, cada corrotina fazia seu próprio backoff independentemente, e o
    sleep acontecia *fora* do semáforo — o pool de corrotinas dormindo era
    ilimitado e todas voltavam juntas. Um 429 vira aqui uma pausa única e
    compartilhada.
    """

    def __init__(self):
        self._gate = None
        self._open_until = 0.0

    def _ensure_gate(self):
        if self._gate is None:
            self._gate = asyncio.Event()
            self._gate.set()
        return self._gate

    async def wait(self):
        gate = self._ensure_gate()
        while not gate.is_set():
            await gate.wait()

    def trip(self, seconds: float):
        """Abre o circuito por `seconds`. Só estende, nunca encurta."""
        gate = self._ensure_gate()
        seconds = min(max(seconds, 0.0), _MAX_PAUSE_SECONDS)
        deadline = time.monotonic() + seconds

        if deadline <= self._open_until:
            return  # já pausado por mais tempo
        self._open_until = deadline

        if gate.is_set():
            gate.clear()
            asyncio.create_task(self._reopen())  # exatamente um reopener

    async def _reopen(self):
        while True:
            delay = self._open_until - time.monotonic()
            if delay <= 0:
                break
            await asyncio.sleep(delay)  # re-checa: trip() pode ter estendido
        self._gate.set()


class AsyncCamaraClient:
    def __init__(self, url='https://dadosabertos.camara.leg.br/api/v2/',
                 rate_limit_rps: float = None, max_concurrency: int = None):
        self.url = url
        self._semaphore = None
        self._max_concurrency = max_concurrency or _MAX_CONCURRENCY
        self._limiter = RateLimiter(rate_limit_rps or _RATE_LIMIT_RPS)
        self._breaker = CircuitBreaker()
        self._consecutive_server_errors = 0

    @property
    def semaphore(self):
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrency)
        return self._semaphore

    @retry(
        wait=_camara_wait,
        stop=_camara_stop,
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True,
    )
    async def get(self, session: aiohttp.ClientSession, endpoint: str, params: dict = None):
        if not endpoint:
            raise ValueError('The endpoint parameter must be not empty.')

        url = f'{self.url}{endpoint}'

        # Ordem importa: esperar a pausa global ANTES de consumir um token e
        # ocupar um slot do semáforo, para não segurar capacidade à toa.
        await self._breaker.wait()
        await self._limiter.acquire()

        async with self.semaphore:
            async with session.get(url, params=params, timeout=_TIMEOUT) as response:
                if response.status == 404:
                    self._consecutive_server_errors = 0
                    return {}

                if response.status in _RETRYABLE_STATUSES:
                    retry_after = _parse_retry_after(response.headers.get('Retry-After'))

                    if response.status == 429 or retry_after is not None:
                        pause = retry_after if retry_after is not None else 30.0
                        self._breaker.trip(pause)
                        raise CamaraRateLimitError(
                            f"HTTP {response.status}: {response.reason}", retry_after=retry_after
                        )

                    self._consecutive_server_errors += 1
                    if self._consecutive_server_errors >= _SERVER_ERROR_TRIP_THRESHOLD:
                        print(
                            f'[client] {self._consecutive_server_errors} erros 5xx consecutivos — '
                            f'pausando {_SERVER_ERROR_TRIP_SECONDS}s.'
                        )
                        self._breaker.trip(_SERVER_ERROR_TRIP_SECONDS)
                        self._consecutive_server_errors = 0
                    raise CamaraServerError(f"HTTP {response.status}: {response.reason}")

                response.raise_for_status()
                self._consecutive_server_errors = 0

                text = await response.text()
                if not text:
                    return {}

                return await response.json()

    async def get_all_pages(self, session: aiohttp.ClientSession, endpoint: str, params: dict = None,
                            itens: int = 100, page_chunk: int = 50):
        """
        Fetch all pages of a paginated endpoint using links metadata.

        As páginas são buscadas em blocos em vez de um único gather sobre todas
        elas: um endpoint com milhares de páginas criava milhares de corrotinas
        de uma vez, todas competindo pelo mesmo semáforo.

        Args:
            session: aiohttp ClientSession
            endpoint: API endpoint (e.g., 'deputados')
            params: Query parameters (optional)
            itens: Items per page (default 100)
            page_chunk: Quantas páginas disparar por vez

        Returns:
            Combined list of all records from all pages
        """
        # Start with page 1 to discover total pages
        page1_params = {**(params or {}), 'itens': itens, 'pagina': 1}
        page1_params = {k: v for k, v in page1_params.items() if v is not None}

        page1_response = await self.get(session, endpoint, params=page1_params)
        all_data = page1_response.get('dados', [])

        # Extract total pages from links metadata
        links = page1_response.get('links', [])
        last_page = 1

        for link in links:
            if link.get('rel') == 'last':
                href = link.get('href', '')
                # Parse the href to extract pagina param
                parsed = urlparse(href)
                query_params = parse_qs(parsed.query)
                if 'pagina' in query_params:
                    try:
                        last_page = int(query_params['pagina'][0])
                    except (ValueError, IndexError):
                        pass
                break

        if last_page <= 1:
            return all_data

        for chunk_start in range(2, last_page + 1, page_chunk):
            chunk_end = min(chunk_start + page_chunk, last_page + 1)
            tasks = []
            for page_num in range(chunk_start, chunk_end):
                page_params = {**(params or {}), 'itens': itens, 'pagina': page_num}
                page_params = {k: v for k, v in page_params.items() if v is not None}
                tasks.append(self.get(session, endpoint, params=page_params))

            for result in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(result, Exception):
                    print(f'[client] Página falhou em {endpoint}: {result}')
                    continue
                all_data.extend(result.get('dados', []))

        return all_data
