from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import asyncio


class AsyncFrentesMembrosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'frentes/{id}/membros'

    async def extract(
        self,
        frentes
    ):
        frentes_ids = list(frente.get('id') for frente in frentes if frente.get('id'))
        all_membros = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for frente_id in frentes_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=frente_id))
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            for index, result in enumerate(results):
                membros_data = result.get('dados', [])
                frente_id = frentes_ids[index]
                for membro in membros_data:
                    membro['idFrente'] = frente_id
                all_membros.extend(membros_data)

        return all_membros
