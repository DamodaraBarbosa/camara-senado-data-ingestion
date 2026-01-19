from extractors.camara.base import CamaraBaseExtractor
from datetime import datetime, date
from utils.utils import add_months
import requests

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
        start_year = start_legislatura_year if start_legislatura_year else current_year
        years_range = range(start_year, current_year + 1)

        all_votacoes = []

        for index, ano in enumerate(years_range):
            id_legislatura = init_legislatura + (index // 4)

            # data inicial do ano
            if index % 4 == 0 and id_legislatura == init_legislatura:
                current_start_date = date(ano, 2, 1)
            else:
                current_start_date = date(ano, 1, 1)

            if current_start_date > date.today():
                break

            while current_start_date.year == ano:
                page = 1
                empty_count = 0

                while empty_count < request_tries:
                    try:
                        params = {
                            'idProposicao': id_proposicao,
                            'idEvento': id_evento,
                            'idOrgao': id_orgao,
                            'dataInicio': current_start_date.isoformat(),
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

                        print(
                            f'Data length: {len(data)} | '
                            f'Year: {ano} | '
                            f'Start date: {current_start_date} | '
                            f'Max date: {max(v["data"] for v in data)} | '
                            f'idLegislatura: {id_legislatura} | '
                            f'page: {page}'
                        )

                        empty_count = 0
                        all_votacoes.extend(data)
                        page += 1

                    except requests.exceptions.RequestException as e:
                        print(f'Error fetching votacoes from API with params {params}. Error: {e}')
                        break
                    except Exception as e:
                        print(f'An unexpected error occurred while processing votacoes with params {params}. Error: {e}')
                        break

                # ⏭️ Avança 3 meses APÓS esgotar tentativas
                current_start_date = add_months(current_start_date, 3)

        return all_votacoes
