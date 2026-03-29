from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
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

            results = await asyncio.gather(*tasks)

            for result in results:
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        return all_ids
