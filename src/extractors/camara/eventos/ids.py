from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import aiohttp
import asyncio


class AsyncEventosIdsExtractor(CamaraBaseExtractor):
    ENDPOINT = 'eventos/{id}'

    async def extract(
        self,
        eventos
    ):
        eventos_ids = list(evento.get('id') for evento in eventos if evento.get('id'))
        all_ids = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for evento in eventos_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=evento))
                tasks.append(task)

            results, coverage, _errors = await gather_aligned(tasks, label='eventos/ids')

            for result in results:
                if result is None:
                    continue
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        self.partial = coverage < 0.99
        return all_ids
