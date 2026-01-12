from extractors.camara.base import CamaraBaseExtractor
import json

class ProfissoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/profissoes'

    def extract(self, deputados: json):
        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_profissoes = []

        try:
            for deputado_id in deputados_ids:
                response = self.client.get(self.ENDPOINT.format(id=deputado_id))
                data = response.get('dados', [])
                
                for profissao in data:
                    profissao['deputado_id'] = deputado_id
                
                print(f'ID: {deputado_id} | Profissões Extracted: {data}')
                all_profissoes.extend(data)

        except Exception as e:
            print(f'Error while extracting profissoes for deputado {deputado_id}: {e}')

        return all_profissoes
