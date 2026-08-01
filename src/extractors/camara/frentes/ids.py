from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import aiohttp


class AsyncFrentesIdsExtractor(CamaraBaseExtractor):
    ENDPOINT = 'frentes/{id}'

    async def extract(
        self,
        frentes
    ):
        frentes_ids = list(frente.get('id') for frente in frentes if frente.get('id'))
        all_ids = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for frente in frentes_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=frente))
                tasks.append(task)

            results, coverage, _errors = await gather_aligned(tasks, label='frentes/ids')

            for result in results:
                if result is None:
                    continue
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        self.partial = coverage < 0.99
        return all_ids
