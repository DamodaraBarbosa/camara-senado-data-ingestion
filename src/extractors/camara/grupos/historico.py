from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import aiohttp
import asyncio


class AsyncGruposHistoricoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'grupos/{id}/historico'

    async def extract(
        self,
        grupos
    ):
        grupos_ids = list(grupo.get('id') for grupo in grupos if grupo.get('id'))
        all_historico = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for grupo_id in grupos_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=grupo_id))
                tasks.append(task)

            results, coverage, _errors = await gather_aligned(tasks, label='grupos/historico')

            for index, result in enumerate(results):
                historico_data = result.get('dados', [])
                grupo_id = grupos_ids[index]
                for historico in historico_data:
                    historico['idGrupo'] = grupo_id
                all_historico.extend(historico_data)

        self.partial = coverage < 0.99
        return all_historico
