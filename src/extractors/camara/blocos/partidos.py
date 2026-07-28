from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import asyncio
import aiohttp


class AsyncBlocosPartidosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'blocos/{id}/partidos'

    async def extract(
        self,
        blocos: list
    ):
        blocos_ids = list(bloco.get('id') for bloco in blocos if bloco.get('id'))
        all_partidos = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for bloco in blocos_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=bloco))
                tasks.append(task)

            results, coverage, _errors = await gather_aligned(tasks, label='blocos/partidos')

            for i, result in enumerate(results):
                partidos_data = result.get('dados', [])
                bloco_id = blocos_ids[i]
                for partido in partidos_data:
                    partido['idBloco'] = bloco_id
                all_partidos.extend(partidos_data)

        self.partial = coverage < 0.99
        return all_partidos
