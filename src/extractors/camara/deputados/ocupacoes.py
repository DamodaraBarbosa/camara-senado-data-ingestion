from extractors.camara.base import CamaraBaseExtractor
import json

class OcupacoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/ocupacoes'

    def extract(self, deputados: json):
        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_ocupacoes = []

        try:
            for deputado_id in deputados_ids:
                response = self.client.get(self.ENDPOINT.format(id=deputado_id))
                data = response.get('dados', [])
                
                for ocupacao in data:
                    ocupacao['deputado_id'] = deputado_id
                
                all_ocupacoes.extend(data)

        except Exception as e:
            print(f'Error while extracting ocupacoes for deputado {deputado_id}: {e}')

        return all_ocupacoes