from extractors.camara.base import CamaraBaseExtractor

class DeputadosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados'
    LEGISLATURAS = 'legislaturas'

    def extract(
            self,
            init_legislatura: int | None = None,
            sigla_partido: str = None,
            sigla_sexo: str = None,
            sigla_uf: str = None,
            order_by: str = None,
            order: str = None,
            items: int = 1000,
            request_tries: int = 4
        ):  
        legislaturas = self.client.get(self.LEGISLATURAS)['dados']
        current_legislatura = max(legislatura['id'] for legislatura in legislaturas)
        
        start = init_legislatura if init_legislatura is not None else current_legislatura
        all_deputados = []

        for legislatura in range(start, current_legislatura + 1):
            page = 1
            empty_count = 0

            while empty_count < request_tries:
                try:
                    params = {
                        'idLegislatura': legislatura,
                        'siglaUf': sigla_uf,
                        'siglaPartido': sigla_partido,
                        'siglaSexo': sigla_sexo,
                        'ordernarPor': order_by,
                        'ordem': order,
                        'itens': items,
                        'pagina': page
                    }

                    params = {k: v for k, v in params.items() if v is not None}
                
                    response = self.client.get(self.ENDPOINT, params=params)
                    data = response.get('dados', [])

                    if not data:
                        empty_count += 1
                        page += 1
                        continue
                    
                    print(f'Legislatura ID: {legislatura}, Page: {page}, Data Length: {len(data)}')
                    all_deputados.extend(data)
                    page += 1

                except Exception as e:
                    print(f'Error while extracting deputados for legislatura {legislatura}, page {page}: {e}')
        
        return all_deputados