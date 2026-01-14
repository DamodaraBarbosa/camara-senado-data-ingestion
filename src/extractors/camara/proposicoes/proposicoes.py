from extractors.camara.base import CamaraBaseExtractor
from datetime import datetime

class ProposicoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes'
    LEGISLATURA = 'legislaturas'

    def extract(
            self, 
            autor: str = None, 
            init_legislatura: int = None, 
            sigla_partido_autor: list = None,
            sigla_uf_autor: list = None,
            tramitacao_senado: bool = None,
            itens: int = 100,
            request_tries: int = 4
        ):
        legislatura = self.client.get(self.LEGISLATURA, params={'id': init_legislatura})
        start_legislatura_date = legislatura['dados'][0].get('dataInicio', None)
        start_legislatura_year = int(start_legislatura_date.split('-')[0]) if start_legislatura_date else None

        current_year = datetime.now().year
        start_year = start_legislatura_year if start_legislatura_year is not None else current_year
        years_range = range(start_year, current_year + 1)
                       
        all_proposicoes = []

        for ano in years_range:
            page = 1
            empty_count = 0
        
            while empty_count < request_tries:
                try:
                    params = {
                        'autor': autor,
                        'ano': ano,
                        'siglaPartidoAutor': sigla_partido_autor,
                        'siglaUfAutor': sigla_uf_autor,
                        'tramitacaoSenado': tramitacao_senado,
                        'itens': itens,
                        'pagina': page
                    }

                    params = {k: v for k, v in params.items() if v is not None}

                    response = self.client.get(self.ENDPOINT, params=params)
                    data = response.get('dados', [])

                    if not data:
                        empty_count += 1
                        page += 1
                        continue
                    
                    empty_count = 0
                    all_proposicoes.extend(data)
                    page += 1

                except Exception as e:
                    print(f'Error while extracting proposicoes: {e}')

        return all_proposicoes[:100]