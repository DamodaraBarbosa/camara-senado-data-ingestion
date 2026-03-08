from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import asyncio

class AsyncEventosDeputadosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'eventos/{id}/deputados'

    async def extract(
            self,
            eventos
        ):
        eventos_ids = list(evento.get('id') for evento in eventos if evento.get('id'))
        all_deputados = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for evento in eventos_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=evento))
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            for result in results:
                deputados_data = result.get('dados', [])
                all_deputados.extend(deputados_data)

        return all_deputados