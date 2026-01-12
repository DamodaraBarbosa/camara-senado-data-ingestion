from extractors.camara.base import CamaraBaseExtractor
import json

class MandatosExternosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/mandatosExternos'

    def extract(self, deputados: json):
        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_mandatos_externos = []

        try:
            for deputado_id in deputados_ids:
                response = self.client.get(self.ENDPOINT.format(id=deputado_id))
                data = response.get('dados', [])
                print(deputado_id)
                for mandato in data:
                    mandato['deputado_id'] = deputado_id

                all_mandatos_externos.extend(data)

        except Exception as e:
            print(f'Error while extracting mandatos externos for deputado {deputado_id}: {e}')

        return all_mandatos_externos