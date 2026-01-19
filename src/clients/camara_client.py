import asyncio
import aiohttp
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

class AsyncCamaraClient:
    def __init__(self, url='https://dadosabertos.camara.leg.br/api/v2/'):
        self.url = url
        self.semaphore = asyncio.Semaphore(5)


    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=12)
    )

    async def get(self, session: aiohttp.ClientSession, endpoint: str, params: dict = None):
        if not endpoint:
            raise ValueError('The endpoint parameter must be not empty.')
        
        url = f'{self.url}{endpoint}'

        async with self.semaphore:
            async with session.get(url, params=params, timeout=30) as response:
                response.raise_for_status()
        
                text = await response.text()
                if not text:
                    return {}

                return await response.json()