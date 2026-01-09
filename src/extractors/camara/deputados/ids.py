from extractors.camara.base import CamaraBaseExtractor
import json

class IdsExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/'

    def extract(self, deputados: json):
        deputados_ids = []
        all_ids = []

        for deputado in deputados:
            deputado_id = deputado.get('id')
            if deputado_id not in deputados_ids:
                deputados_ids.append(deputado_id)

        for index, id in enumerate(deputados_ids):
            response = self.client.get(f'{self.ENDPOINT}{id}')
            data = response.get('dados', {})
            all_ids.append(data)

        return all_ids

        # response = self.client.get(self.ENDPOINT)
        # return response.get('dados', [])
