from extractors.camara.base import CamaraBaseExtractor
import json

class EventosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/eventos'

    def extract(
            self, 
            deputados: json, 
            init_legislatura: int = None,
            items: int = 50,
            request_tries: int = 4
        ):
        legislatura = self.client.get('legislaturas', params={'id': init_legislatura})
        start_legislatura_date = legislatura['dados'][0].get('dataInicio', None)

        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_eventos = []

        for deputado_id in deputados_ids:
            page = 1
            empty_count = 0

            while empty_count < request_tries:
                try:
                    params = {
                        'itens': items,
                        'dataInicio': start_legislatura_date,
                        'pagina': page
                    }

                    params = {k: v for k, v in params.items() if v is not None}

                    response = self.client.get(self.ENDPOINT.format(id=deputado_id), params=params)
                    data = response.get('dados', [])

                    if not data:
                        empty_count += 1
                        page += 1
                        continue

                    empty_count = 0

                    for evento in data:
                        evento['deputado_id'] = deputado_id
                    
                    all_eventos.extend(data)
                    page += 1

                except Exception as e:
                    print(f'Error while extracting eventos for deputado {deputado_id}: {e}')

        return all_eventos