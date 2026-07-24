from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncTramitacoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/tramitacoes'

    async def extract(
        self,
        proposicoes: json,
        data_inicio: str = None,
    ):
        session = aiohttp.ClientSession()
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id')
                               for proposicao in proposicoes if proposicao.get('id')))
        all_tramitacoes = []

        tasks = []
        for proposicao_id in proposicoes_ids:
            task = self.client.get(session, self.ENDPOINT.format(id=proposicao_id))
            tasks.append((proposicao_id, task))

        try:
            results = await asyncio.gather(*[task for _, task in tasks])

            for (proposicao_id, _), data in zip(tasks, results):
                tramitacoes_data = data.get('dados', [])
                for tramitacao in tramitacoes_data:
                    tramitacao['idProposicao'] = proposicao_id
                all_tramitacoes.extend(tramitacoes_data)

        except Exception as e:
            print(f'Error while extracting tramitacoes: {e}')

        await session.close()
        return all_tramitacoes
