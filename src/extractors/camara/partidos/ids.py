from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import json
import aiohttp


class AsyncPartidosIdsExtractor(CamaraBaseExtractor):
    ENDPOINT = 'partidos/{id}'

    async def extract(
        self,
        partidos: json
    ):
        partidos_ids = list(dict.fromkeys(partido.get('id') for partido in partidos if partido.get('id')))
        all_ids = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for partido_id in partidos_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=partido_id))
                tasks.append(task)

            results, coverage, _errors = await gather_aligned(tasks, label='partidos/ids')

            for result in results:
                if result is None:
                    continue
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        self.partial = coverage < 0.99
        return all_ids
