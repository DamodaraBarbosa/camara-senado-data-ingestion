import asyncio
import aiohttp
import random
from urllib.parse import urlparse, parse_qs
from email.utils import parsedate_to_datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError


# Configuração otimizada:
# - total: 90s para cada requisição individual
# - connect: 30s para estabelecer conexão
_TIMEOUT = aiohttp.ClientTimeout(total=90, connect=30)

# Status codes que justificam retry
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

# Limite de tempo total para uma operação de extração (evita loops infinitos)
# Se exceder esse tempo mesmo com retries, a task falha e Airflow faz retry
_MAX_OPERATION_TIME = 600  # 10 minutos


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
    """Wait strategy que respeita Retry-After header."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None

    if isinstance(exc, CamaraRateLimitError) and exc.retry_after is not None:
        # Clamp entre 1s e 45s para não estourar o budget de 600s
        base = min(max(exc.retry_after, 1), 45)
    else:
        # Fallback para exponencial (min=2, max=30)
        base = wait_exponential(multiplier=1, min=2, max=30)(retry_state)

    # Adicionar jitter para evitar retry sincronizado
    return base + random.uniform(0, 2)


def _camara_stop(retry_state) -> bool:
    """Stop strategy com limite diferenciado para rate-limit vs erros de servidor."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None

    # 5 tentativas para rate-limit (esperado sob carga, vale insistir)
    # 3 tentativas para outros erros (falha real de servidor)
    limit = 5 if isinstance(exc, CamaraRateLimitError) else 3

    return retry_state.attempt_number >= limit


class AsyncCamaraClient:
    def __init__(self, url='https://dadosabertos.camara.leg.br/api/v2/'):
        self.url = url
        self._semaphore = None

    @property
    def semaphore(self):
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(15)
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

        async with self.semaphore:
            async with session.get(url, params=params, timeout=_TIMEOUT) as response:
                if response.status == 404:
                    return {}

                if response.status in _RETRYABLE_STATUSES:
                    retry_after = _parse_retry_after(response.headers.get('Retry-After'))
                    if response.status == 429 or retry_after is not None:
                        print(f'[client] HTTP {response.status} — retry strategy will handle backoff (Retry-After: {retry_after}s).')
                        raise CamaraRateLimitError(f"HTTP {response.status}: {response.reason}", retry_after=retry_after)
                    else:
                        print(f'[client] HTTP {response.status} — server error detected.')
                        raise CamaraServerError(f"HTTP {response.status}: {response.reason}")

                response.raise_for_status()

                text = await response.text()
                if not text:
                    return {}

                return await response.json()

    async def get_all_pages(self, session: aiohttp.ClientSession, endpoint: str, params: dict = None, itens: int = 100):
        """
        Fetch all pages of a paginated endpoint in parallel using links metadata.

        Args:
            session: aiohttp ClientSession
            endpoint: API endpoint (e.g., 'deputados')
            params: Query parameters (optional)
            itens: Items per page (default 100)

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

        # If there are more pages, fetch them in parallel
        if last_page > 1:
            tasks = []
            for page_num in range(2, last_page + 1):
                page_params = {**(params or {}), 'itens': itens, 'pagina': page_num}
                page_params = {k: v for k, v in page_params.items() if v is not None}
                task = self.get(session, endpoint, params=page_params)
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            for result in results:
                page_data = result.get('dados', [])
                all_data.extend(page_data)

        return all_data
