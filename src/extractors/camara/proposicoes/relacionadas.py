from extractors.camara.base import CamaraBaseExtractor
import json

class RelacionadasExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/relacionadas'

    def extract(
            self, 
            proposicoes: json
        ):
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id') for proposicao in proposicoes if proposicao.get('id')))
        all_relacionadas = []

        for proposicao_id in proposicoes_ids:
            try:
                response = self.client.get(self.ENDPOINT.format(id=proposicao_id))
                data = response.get('dados', [])

                for relacionada in data:
                    relacionada['relacionadoProposicao'] = proposicao_id

                all_relacionadas.extend(data)

            except Exception as e:
                print(f'Error while extracting relacionadas for proposicao {proposicao_id}: {e}')

        return all_relacionadas