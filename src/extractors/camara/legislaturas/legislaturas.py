from extractors.camara.base import CamaraBaseExtractor
import asyncio
import aiohttp


class AsyncLegislaturaExtractor(CamaraBaseExtractor):
    ENDPOINT = 'legislaturas'

    async def extract(
        self,
        init_legislatura: int = None
    ):
        all_legislaturas = []

        async with aiohttp.ClientSession() as session:
            legislaturas = await self.client.get(session, self.ENDPOINT)
            legislaturas = legislaturas.get('dados', [])

            current_legislatura = max(legislatura.get('id') for legislatura in legislaturas if legislatura.get('id'))
            start = init_legislatura if init_legislatura is not None else current_legislatura

            tasks = []

            for legislatura in range(start, current_legislatura + 1):
                task = self.client.get(session, self.ENDPOINT, params={'id': legislatura})
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            for result in results:
                data = result.get('dados', [])
                all_legislaturas.extend(data)

            return all_legislaturas
