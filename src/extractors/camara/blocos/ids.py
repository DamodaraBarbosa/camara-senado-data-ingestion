from extractors.camara.base import CamaraBaseExtractor
import asyncio
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

            results = await asyncio.gather(*tasks)

            for result in results:
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        return all_ids