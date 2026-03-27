from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import asyncio


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

            results = await asyncio.gather(*tasks)

            for result in results:
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        return all_ids
