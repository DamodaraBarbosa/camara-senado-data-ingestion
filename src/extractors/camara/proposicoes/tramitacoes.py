from extractors.camara.base import CamaraBaseExtractor
import json
import aiohttp

class AsyncTramitacoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/tramitacoes'

    async def extract(
            self, 
            proposicoes: json,
            data_inicio: str = None,
        ):
        session = aiohttp.ClientSession()
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id') for proposicao in proposicoes if proposicao.get('id')))
        print(proposicoes_ids)
        all_tramitacoes = []

        for proposicao_id in proposicoes_ids:
            try:
                response = await self.client.get(session, self.ENDPOINT.format(id=proposicao_id))
                data = response.get('dados', [])

                for tramitacao in data:
                    tramitacao['idProposicao'] = proposicao_id

                all_tramitacoes.extend(data)

            except Exception as e:
                print(f'Error while extracting tramitacoes for proposicao {proposicao_id}: {e}')

        await session.close()
        return all_tramitacoes