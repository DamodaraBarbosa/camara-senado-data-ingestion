from extractors.camara.base import CamaraBaseExtractor
import json
import aiohttp

class AsyncIdsExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/'

    async def extract(self, deputados: json):
        session = aiohttp.ClientSession()
        deputados_ids = []
        all_ids = []

        for deputado in deputados:
            deputado_id = deputado.get('id')
            if deputado_id not in deputados_ids:
                deputados_ids.append(deputado_id)

        for id in deputados_ids:
            response = await self.client.get(session, f'{self.ENDPOINT}{id}')
            data = response.get('dados', {})
            all_ids.append(data)

        await session.close()
        return all_ids

        # response = self.client.get(self.ENDPOINT)
        # return response.get('dados', [])
