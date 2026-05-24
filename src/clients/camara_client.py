import asyncio
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


_TIMEOUT = aiohttp.ClientTimeout(total=60, connect=10)
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


class AsyncCamaraClient:
    def __init__(self, url='https://dadosabertos.camara.leg.br/api/v2/'):
        self.url = url
        self._semaphore = None

    @property
    def semaphore(self):
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(5)
        return self._semaphore

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(7),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    )
    async def get(self, session: aiohttp.ClientSession, endpoint: str, params: dict = None):
        if not endpoint:
            raise ValueError('The endpoint parameter must be not empty.')

        url = f'{self.url}{endpoint}'

        async with self.semaphore:
            async with session.get(url, params=params, timeout=_TIMEOUT) as response:
                try:
                    if response.status in _RETRYABLE_STATUSES:
                        wait_time = int(response.headers.get('Retry-After', '30'))
                        print(f'[client] HTTP {response.status} — aguardando {wait_time}s antes de tentar novamente.')
                        await asyncio.sleep(wait_time)

                    response.raise_for_status()

                    text = await response.text()
                    if not text:
                        return {}

                    return await response.json()

                except Exception as e:
                    print(f'Error: {e}')
                    raise
