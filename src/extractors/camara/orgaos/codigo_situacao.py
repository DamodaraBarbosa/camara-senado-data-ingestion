from extractors.camara.base import CamaraBaseExtractor
import asyncio
import aiohttp

class CodigoSituacaoOrgaoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/orgaos/codSituacao'

    async def extract(self):
        all_codigos = []


        async with aiohttp.ClientSession() as session:
            tasks = []

            task = self.client.get(session, self.ENDPOINT)
            tasks.append(task)

            results = await asyncio.gather(*tasks)

            for result in results:
                codigo = result.get('dados', [])
                all_codigos.append(codigo)

            return all_codigos