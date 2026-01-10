from extractors.camara.base import CamaraBaseExtractor
from datetime import datetime
import json

class DespesasExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/despesas'

    def extract(
            self, 
            deputados: json, 
            init_year: int = None, 
            month: int = None, 
            items: int = 50,
            request_tries: int = 4
        ):
    
        deputados_ids = []
        all_expenses = []

        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        
        current_year = datetime.now().year
        start_year = init_year if init_year is not None else current_year
        years_range = [range(start_year, current_year + 1)]
 
        for  deputado_id in deputados_ids:
            page = 1
            empty_count = 0

            while empty_count < request_tries:
                try:
                    params = {
                        'ano': years_range,
                        'mes': month,
                        'pagina': page,
                        'itens': items
                    }

                    params = {k: v for k, v in params.items() if v is not None}

                    response = self.client.get(self.ENDPOINT.format(id=deputado_id), params=params)
                    data = response.get('dados', [])

                    if not data:
                        empty_count += 1
                        page += 1
                        continue

                    empty_count = 0

                    for despesa in data:
                        despesa['deputado_id'] = deputado_id

                    all_expenses.extend(data)
                    page += 1

                except Exception as e:
                    print(f'Error while extracting despesas for deputado {deputado_id}: {e}')

        return all_expenses