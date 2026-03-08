from extractors.camara.base import CamaraBaseExtractor
import json
import aiohttp

class AsyncVotacoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/votacoes'

    async def extract(
            self, 
            proposicoes: json,
        ):
        session = aiohttp.ClientSession()
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id') for proposicao in proposicoes if proposicao.get('id')))
        print(proposicoes_ids)
        all_votacoes = []

        for proposicao_id in proposicoes_ids:
            try:
                response = await self.client.get(session, self.ENDPOINT.format(id=proposicao_id))
                print(f'Retrieved response: {response}')
                data = response.get('dados', [])

                for votacao in data:
                    votacao['idProposicao'] = proposicao_id

                all_votacoes.extend(data)

            except Exception as e:
                print(f'Error while extracting votacoes for proposicao {proposicao_id}: {e}')

        await session.close()
        return all_votacoes