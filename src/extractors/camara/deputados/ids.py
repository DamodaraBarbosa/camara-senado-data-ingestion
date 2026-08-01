from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
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

        tasks = []
        for deputado_id in deputados_ids:
            task = self.client.get(session, f'{self.ENDPOINT}{deputado_id}')
            tasks.append(task)

        results, coverage, _errors = await gather_aligned(tasks, label='deputados/ids')

        for result in results:
            if result is None:
                continue
            data = result.get('dados', {})
            all_ids.append(data)

        await session.close()
        self.partial = coverage < 0.99
        return all_ids

        # response = self.client.get(self.ENDPOINT)
        # return response.get('dados', [])
