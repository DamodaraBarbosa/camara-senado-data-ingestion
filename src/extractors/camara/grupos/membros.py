from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import assert_usable, gather_aligned
import aiohttp


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

            results, coverage, errors = await gather_aligned(tasks, label='grupos/membros')

            for index, result in enumerate(results):
                # gather_aligned devolve None onde a requisicao falhou; a cobertura ja
                # contabilizou isso. Sem esta guarda, um unico 504 derruba o extractor.
                if result is None:
                    continue
                membros_data = result.get('dados', [])
                grupo_id = grupos_ids[index]
                for membro in membros_data:
                    membro['idGrupo'] = grupo_id
                # `extend`, nao `append`: as linhas 32-33 marcam cada membro com
                # idGrupo, o que so faz sentido achatando a lista. Com append, cada
                # linha do NDJSON virava um array JSON — que o SerDe do Glue nao le —
                # a contagem de registros contava grupos em vez de membros, e grupo
                # sem membro produzia uma linha `[]`. `grupos/historico`, de forma
                # identica, sempre usou extend.
                all_membros.extend(membros_data)

        self.partial = coverage < 0.99
        assert_usable(all_membros, coverage, errors, label='grupos/membros')
        return all_membros
