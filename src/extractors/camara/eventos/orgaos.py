from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import asyncio


class AsyncEventosOrgaosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'eventos/{id}/orgaos'

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

            for evento_id, result in zip(eventos_ids, results):
                deputados_data = result.get('dados', [])
                for deputado in deputados_data:
                    deputado['idEvento'] = evento_id
                all_deputados.extend(deputados_data)

        return all_deputados
