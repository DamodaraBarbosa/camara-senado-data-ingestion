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
            items: int | str = None,
        ):  
        legislaturas = self.client.get(self.LEGISLATURAS)['dados']
        current_legislatura = max(legislatura['id'] for legislatura in legislaturas)
        
        start = init_legislatura if init_legislatura is not None else current_legislatura
        all_deputados = []

        for legislatura in range(start, current_legislatura + 1):
            page = 1

            while True:
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
                    break

                all_deputados.extend(data)
                page += 1
        
        return all_deputados