from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import aiohttp
import asyncio


class AsyncEventosVotacoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'eventos/{id}/votacoes'

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

            results, coverage, _errors = await gather_aligned(tasks, label='eventos/votacoes')

            for evento_id, result in zip(eventos_ids, results):
                deputados_data = result.get('dados', [])
                for deputado in deputados_data:
                    deputado['idEvento'] = evento_id
                all_deputados.extend(deputados_data)

        self.partial = coverage < 0.99
        return all_deputados
