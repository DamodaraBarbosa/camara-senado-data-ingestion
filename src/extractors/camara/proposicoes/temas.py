from extractors.camara.base import CamaraBaseExtractor
import json

class TemasExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/temas'

    def extract(
            self, 
            proposicoes: json
        ):
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id') for proposicao in proposicoes if proposicao.get('id')))
        all_temas = []

        for proposicao_id in proposicoes_ids:
            try:
                response = self.client.get(self.ENDPOINT.format(id=proposicao_id))
                data = response.get('dados', [])

                for tema in data:
                    tema['idProposicao'] = proposicao_id

                all_temas.extend(data)

            except Exception as e:
                print(f'Error while extracting temas for proposicao {proposicao_id}: {e}')

        return all_temas