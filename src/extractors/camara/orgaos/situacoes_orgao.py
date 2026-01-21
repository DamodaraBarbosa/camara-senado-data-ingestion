from extractors.camara.base import CamaraBaseExtractor
import asyncio
import aiohttp

class SituacoesOrgaoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/situacoesOrgao'

    async def extract(self):
        all_situacoes = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            task = self.client.get(session, self.ENDPOINT)
            tasks.append(task)

            results = await asyncio.gather(*tasks)

            for result in results:
                codigo = result.get('dados', [])
                all_situacoes.append(codigo)

            return all_situacoes