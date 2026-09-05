from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import assert_usable, gather_aligned
import aiohttp


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

            results, coverage, errors = await gather_aligned(tasks, label='eventos/deputados')

            for evento_id, result in zip(eventos_ids, results):
                # gather_aligned devolve None onde a requisicao falhou; a cobertura ja
                # contabilizou isso. Sem esta guarda, um unico 504 derruba o extractor.
                if result is None:
                    continue
                deputados_data = result.get('dados', [])
                for deputado in deputados_data:
                    deputado['idEvento'] = evento_id
                all_deputados.extend(deputados_data)

        self.partial = coverage < 0.99
        assert_usable(all_deputados, coverage, errors, label='eventos/deputados')
        return all_deputados
