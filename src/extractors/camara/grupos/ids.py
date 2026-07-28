from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import aiohttp
import asyncio


class AsyncGruposIdsExtractor(CamaraBaseExtractor):
    ENDPOINT = 'grupos/{id}'

    async def extract(
        self,
        grupos
    ):
        grupos_ids = list(grupo.get('id') for grupo in grupos if grupo.get('id'))
        all_ids = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for grupo in grupos_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=grupo))
                tasks.append(task)

            results, coverage, _errors = await gather_aligned(tasks, label='grupos/ids')

            for result in results:
                if result is None:
                    continue
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        self.partial = coverage < 0.99
        return all_ids
