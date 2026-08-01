import asyncio
import os
import random
import time
import aiohttp
from urllib.parse import urlparse, parse_qs
from email.utils import parsedate_to_datetime
from tenacity import retry, wait_exponential, retry_if_exception_type


# Optimized configuration:
# - total: 90s per individual request
# - connect: 30s to establish connection
_TIMEOUT = aiohttp.ClientTimeout(total=90, connect=30)

# Status codes that warrant retry
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

# The Câmara API limits to 10 req/s per IP (documented at
# github.com/CamaraDosDeputados/dados-abertos/issues/251). No API key increases this ceiling.
# We stay below it on purpose: measurements showed exceeding it doesn't increase throughput — it collapses.
# With 2 concurrent tasks the pipeline yielded 2.04x LESS than serial and spent 6x more 429s doing so.
_RATE_LIMIT_RPS = float(os.getenv("CAMARA_RATE_LIMIT_RPS", 8))

# The rate limiter enforces the API contract; the semaphore only prevents unlimited
# concurrent requests. To sustain R req/s with latency L, concurrency >= R*L is needed —
# with R=8 and L~2s, a semaphore of 6 becomes the real bottleneck and holds the rate at ~3 req/s,
# without the limiter even acting.
_MAX_CONCURRENCY = int(os.getenv("CAMARA_MAX_CONCURRENCY", 16))

# Consecutive 5xx errors indicate the backend is dropping load (the 504 storm
# in votacoes came from our own pressure). Brief pause to let it breathe.
_SERVER_ERROR_TRIP_THRESHOLD = int(os.getenv("CAMARA_5XX_TRIP_THRESHOLD", 5))
_SERVER_ERROR_TRIP_SECONDS = 5.0

# Ceiling for global pause, preventing an absurd Retry-After from blocking everything.
_MAX_PAUSE_SECONDS = 45.0


class CamaraRateLimitError(aiohttp.ClientError):
    """Exception for rate-limit (429) or responses with Retry-After."""
    def __init__(self, message: str, retry_after: float = None):
        super().__init__(message)
        self.retry_after = retry_after


class CamaraServerError(aiohttp.ClientError):
    """Exception for server errors (5xx)."""
    pass


def _parse_retry_after(header_value: str = None) -> float:
    """Parse Retry-After header (seconds as int or HTTP-date)."""
    if not header_value:
        return None

    try:
        # Try to interpret as number of seconds
        return float(header_value)
    except ValueError:
        pass

    try:
        # Try to interpret as HTTP-date
        dt = parsedate_to_datetime(header_value)
        now = __import__('datetime').datetime.now(__import__('datetime').timezone.utc)
        delta = (dt - now).total_seconds()
        return max(delta, 0)
    except (TypeError, ValueError):
        pass

    return None


def _camara_wait(retry_state) -> float:
    """Short local backoff with full jitter.

    The actual waiting is now done once, globally, by the CircuitBreaker.
    Here we only prevent coroutines released by the breaker from all starting at once.

    Full jitter (`uniform(0, base)`) instead of the old `base + uniform(0, 2)`:
    with a 30s window and only 2s of jitter, all coroutines would wake within
    the same 2-second window and collide again — that's what produced bursts
    of up to 108 consecutive rejections.
    """
    base = wait_exponential(multiplier=1, min=1, max=8)(retry_state)
    return random.uniform(0, base)


def _camara_stop(retry_state) -> bool:
    """Stop strategy with different limits for rate-limit vs server errors."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None

    # 5 attempts for rate-limit (expected under load, worth persisting)
    # 3 attempts for other errors (true server failure)
    limit = 5 if isinstance(exc, CamaraRateLimitError) else 3

    return retry_state.attempt_number >= limit


class RateLimiter:
    """Token bucket that spaces requests over time.

    The semaphore limits how many requests are *in flight*; this limits
    the *rate* at which they depart. Without this second control, 6 requests in flight
    with quick responses still exceed the 10 req/s limit.
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
    """Global pause for all workers when the API signals overload.

    Before, each coroutine did its own backoff independently, and the
    sleep happened *outside* the semaphore — the pool of sleeping coroutines was
    unlimited and they all returned together. A 429 becomes here a single,
    shared pause.
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
        """Open the circuit for `seconds`. Only extends, never shortens."""
        gate = self._ensure_gate()
        seconds = min(max(seconds, 0.0), _MAX_PAUSE_SECONDS)
        deadline = time.monotonic() + seconds

        if deadline <= self._open_until:
            return  # Already paused for longer
        self._open_until = deadline

        if gate.is_set():
            gate.clear()
            asyncio.create_task(self._reopen())  # Exactly one reopener

    async def _reopen(self):
        while True:
            delay = self._open_until - time.monotonic()
            if delay <= 0:
                break
            await asyncio.sleep(delay)  # Re-check: trip() may have extended
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

        # Order matters: wait for the global pause BEFORE consuming a token and
        # occupying a semaphore slot, to avoid holding capacity unnecessarily.
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
                            f'[client] {self._consecutive_server_errors} consecutive 5xx errors — '
                            f'pausing {_SERVER_ERROR_TRIP_SECONDS}s.'
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

        Pages are fetched in chunks instead of a single gather over all of them:
        an endpoint with thousands of pages would create thousands of coroutines
        at once, all competing for the same semaphore.

        Args:
            session: aiohttp ClientSession
            endpoint: API endpoint (e.g., 'deputados')
            params: Query parameters (optional)
            itens: Items per page (default 100)
            page_chunk: Number of pages to dispatch at once

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
