from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import aiohttp


class AsyncBlocosIdsExtractor(CamaraBaseExtractor):
    ENDPOINT = 'blocos/{id}'

    async def extract(
        self,
        blocos: list
    ):
        blocos_ids = list(bloco.get('id') for bloco in blocos if bloco.get('id'))
        all_ids = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for bloco in blocos_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=bloco))
                tasks.append(task)

            results, coverage, _errors = await gather_aligned(tasks, label='blocos/ids')

            for result in results:
                if result is None:
                    continue
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        self.partial = coverage < 0.99
        return all_ids
