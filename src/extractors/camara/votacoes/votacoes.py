from extractors.camara.base import CamaraBaseExtractor
from datetime import datetime

class VotacoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'votacoes'
    LEGISLATURA = 'legislaturas'

    def extract(
            self,
            init_legislatura: int = None,
            id_proposicao: list = None,
            id_evento: list = None,
            id_orgao: list = None,
            itens: int = 100,
            request_tries: int = 4
        ):
        legislatura = self.client.get(self.LEGISLATURA, params={'id': init_legislatura})
        start_legislatura_date = legislatura['dados'][0].get('dataInicio', None)
        start_legislatura_year = int(start_legislatura_date.split('-')[0]) if start_legislatura_date else None

        current_year = datetime.now().year
        start_year = start_legislatura_year if start_legislatura_year is not None else current_year
        years_range = range(start_year, current_year + 1)
        
        all_votacoes = []

        for index, ano in enumerate(years_range):
            page = 1
            empty_count = 0
            id_legislatura = init_legislatura + (index // 4) 

            while empty_count < request_tries:
                try:
                    params = {
                        'idProposicao': id_proposicao,
                        'idEvento': id_evento,
                        'idOrgao': id_orgao,
                        'dataInicio': f'{ano}-02-01' if index % 4 == 0 else f'{ano}-01-01',
                        'itens': itens,
                        'pagina': page
                    }

                    params = {k: v for k, v in params.items() if v is not None}

                    response = self.client.get(self.ENDPOINT, params=params)
                    data = response.get('dados', [])

                    for votacao in data:
                        votacao['idLegislatura'] = id_legislatura

                    if not data:
                        empty_count += 1
                        page += 1
                        continue
                
                    print(f'Data length: {(len(data))} | Year: {ano} | Start date: {params['dataInicio']} | Max date : {max([date.get('data') for date in data ])} | idLegislatura: {id_legislatura} | page : {page}')
                    empty_count = 0
                    all_votacoes.extend(data)
                    page += 1

                except Exception as e:
                    print(f'Error while extracting votacoes {e}')

        return all_votacoes