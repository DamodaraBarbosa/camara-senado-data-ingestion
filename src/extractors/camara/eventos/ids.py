from extractors.camara.base import CamaraBaseExtractor
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

            results = await asyncio.gather(*tasks)

            for result in results:
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        return all_ids