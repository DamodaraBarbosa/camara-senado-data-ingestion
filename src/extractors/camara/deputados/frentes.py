from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import json
import aiohttp


class AsyncFrentesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/frentes'

    async def extract(self, deputados: json):
        session = aiohttp.ClientSession()
        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_frentes = []

        try:
            tasks = []
            for deputado_id in deputados_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=deputado_id))
                tasks.append((deputado_id, task))

            results, coverage, _errors = await gather_aligned(
                [task for _, task in tasks], label='deputados/frentes')

            for (deputado_id, _), data in zip(tasks, results):
                if data is None:
                    continue
                frentes_data = data.get('dados', [])
                for frente in frentes_data:
                    frente['deputado_id'] = deputado_id
                all_frentes.extend(frentes_data)

        except Exception as e:
            print(f'Error while extracting frentes: {e}')

        await session.close()
        self.partial = coverage < 0.99
        return all_frentes
