from extractors.camara.base import CamaraBaseExtractor
import json

class AutoresExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/autores'

    def extract(
            self, 
            proposicoes: json
        ):
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id') for proposicao in proposicoes if proposicao.get('id')))
        all_autores = []

        for proposicao_id in proposicoes_ids:
            try:
                response = self.client.get(self.ENDPOINT.format(id=proposicao_id))
                data = response.get('dados', [])

                for autor in data:
                    autor['idProposicao'] = proposicao_id

                all_autores.extend(data)

            except Exception as e:
                print(f'Error while extracting autores for proposicao {proposicao_id}: {e}')

        return all_autores