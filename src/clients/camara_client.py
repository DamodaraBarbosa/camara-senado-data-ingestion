import asyncio
import aiohttp
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


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
        wait=wait_exponential(multiplier=1, min=2, max=20),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(aiohttp.ClientError),
    )
    async def get(self, session: aiohttp.ClientSession, endpoint: str, params: dict = None):
        if not endpoint:
            raise ValueError('The endpoint parameter must be not empty.')

        url = f'{self.url}{endpoint}'

        async with self.semaphore:
            async with session.get(url, params=params, timeout=20) as response:
                try:
                    response.raise_for_status()
                    if response.status == 429:
                        wait_time = response.headers.get('Retry-After', '30')
                        await asyncio.sleep(int(wait_time))

                    text = await response.text()
                    if not text:
                        return {}

                    return await response.json()

                except Exception as e:
                    print(f'Error: {e}')
                    return {}
