from extractors.camara.base import CamaraBaseExtractor
import json

class FrentesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/frentes'

    def extract(self, deputados: json):
        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_frentes = []

        try:
            for deputado_id in deputados_ids:
                response = self.client.get(self.ENDPOINT.format(id=deputado_id))
                data = response.get('dados', [])
                
                for frente in data:
                    frente['deputado_id'] = deputado_id
                
                all_frentes.extend(data)

        except Exception as e:
            print(f'Error while extracting frentes for deputado {deputado_id}: {e}')

        return all_frentes