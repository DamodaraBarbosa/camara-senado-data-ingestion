from extractors.camara.base import CamaraBaseExtractor
import json

class TramitacoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/tramitacoes'

    def extract(
            self, 
            proposicoes: json,
            data_inicio: str = None,
        ):
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id') for proposicao in proposicoes if proposicao.get('id')))
        print(proposicoes_ids)
        all_tramitacoes = []

        for proposicao_id in proposicoes_ids:
            try:
                response = self.client.get(self.ENDPOINT.format(id=proposicao_id))
                data = response.get('dados', [])

                for tramitacao in data:
                    tramitacao['proposicao_id'] = proposicao_id

                all_tramitacoes.extend(data)

            except Exception as e:
                print(f'Error while extracting tramitacoes for proposicao {proposicao_id}: {e}')

        return all_tramitacoes