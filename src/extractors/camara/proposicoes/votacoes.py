from extractors.camara.base import CamaraBaseExtractor
import json

class VotacoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/votacoes'

    def extract(
            self, 
            proposicoes: json,
        ):
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id') for proposicao in proposicoes if proposicao.get('id')))
        print(proposicoes_ids)
        all_votacoes = []

        for proposicao_id in proposicoes_ids:
            try:
                response = self.client.get(self.ENDPOINT.format(id=proposicao_id))
                print(f'Retrieved response: {response}')
                data = response.get('dados', [])

                for votacao in data:
                    votacao['idProposicao'] = proposicao_id

                all_votacoes.extend(data)

            except Exception as e:
                print(f'Error while extracting votacoes for proposicao {proposicao_id}: {e}')

        return all_votacoes