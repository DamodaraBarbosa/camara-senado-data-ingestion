from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import asyncio


class AsyncGruposMembrosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'grupos/{id}/membros'

    async def extract(
        self,
        grupos
    ):
        grupos_ids = list(grupo.get('id') for grupo in grupos if grupo.get('id'))
        all_membros = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for grupo in grupos_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=grupo))
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            for index, result in enumerate(results):
                membros_data = result.get('dados', [])
                grupo_id = grupos_ids[index]
                for membro in membros_data:
                    membro['idGrupo'] = grupo_id
                all_membros.append(membros_data)

        return all_membros
