from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import assert_usable, gather_aligned
import aiohttp


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

            results, coverage, errors = await gather_aligned(tasks, label='grupos/historico')

            for index, result in enumerate(results):
                # gather_aligned devolve None onde a requisicao falhou; a cobertura ja
                # contabilizou isso. Sem esta guarda, um unico 504 derruba o extractor.
                if result is None:
                    continue
                historico_data = result.get('dados', [])
                grupo_id = grupos_ids[index]
                for historico in historico_data:
                    historico['idGrupo'] = grupo_id
                all_historico.extend(historico_data)

        self.partial = coverage < 0.99
        assert_usable(all_historico, coverage, errors, label='grupos/historico')
        return all_historico
