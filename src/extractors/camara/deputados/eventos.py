from extractors.camara.base import CamaraBaseExtractor
import json

class EventosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/eventos'

    def extract(
            self, 
            deputados: json, 
            items: int = 50,
            request_tries: int = 4
        ):
        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_eventos = []

        for deputado_id in deputados_ids:
            page = 1
            empty_count = 0

            while empty_count < request_tries:
                try:
                    params = {
                        'itens': items,
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
                    
                    print(f'ID: {deputado_id} | Page: {page} | Data ID: {data[0].get("deputado_id", None)} | Data type: {type(data)}')
                    all_eventos.extend(data)
                    page += 1

                except Exception as e:
                    print(f'Error while extracting eventos for deputado {deputado_id}: {e}')

        return all_eventos