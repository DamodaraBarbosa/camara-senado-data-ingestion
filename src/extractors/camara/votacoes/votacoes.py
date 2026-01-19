from extractors.camara.base import CamaraBaseExtractor
import asyncio
import aiohttp
from datetime import datetime, date
from utils.utils import add_months

class AsyncVotacoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'votacoes'
    LEGISLATURA = 'legislaturas'

    async def _fetch_period_pages(self, session, id_legislatura, current_start_date, request_tries, id_proposicao, id_evento, id_orgao, itens):
        """
        Busca todas as páginas de votações para um determinado período.
        """
        base_params = {
            'idProposicao': id_proposicao,
            'idEvento': id_evento,
            'idOrgao': id_orgao,
            'itens': itens
        }
        
        extracted_data = []
        page = 1
        empty_count = 0
        current_params = {}  # Definir antes do try para garantir que exista no except

        while empty_count < request_tries:
            try:
                current_params = base_params.copy()
                current_params.update({
                    'dataInicio': current_start_date.isoformat(),
                    'pagina': page
                })

                current_params = {k: v for k, v in current_params.items() if v is not None}

                response = await self.client.get(session, self.ENDPOINT, params=current_params)
                data = response.get('dados', [])

                if not data:
                    empty_count += 1
                    page += 1
                    continue

                for votacao in data:
                    votacao['idLegislatura'] = id_legislatura

                extracted_data.extend(data)
                empty_count = 0
                page += 1

            except Exception as e:
                print(f'Error fetching votacoes from API with params {current_params}. Error: {e}')
                break
        
        # A instrução de retorno deve estar fora do loop 'while'
        return extracted_data

    async def extract(
        self,
        init_legislatura: int = None,
        id_proposicao: list = None,
        id_evento: list = None,
        id_orgao: list = None,
        itens: int = 100,
        request_tries: int = 4
    ):
        async with aiohttp.ClientSession() as session:
            legilslatura = await self.client.get(session, self.LEGISLATURA, params={'id': init_legislatura})
            start_legislatura_date = legilslatura['dados'][0].get('dataInicio', None)
            start_legislatura_year = int(start_legislatura_date.split('-')[0]) if start_legislatura_date else None

            current_year = datetime.now().year
            start_year = start_legislatura_year if start_legislatura_year else current_year
            # years_range = range(start_year, current_year + 1)
            years_range = [2022]

            tasks = []

            for index, ano in enumerate(years_range):
                id_legislatura = init_legislatura + (index // 4)

                if index % 4 == 0 and id_legislatura == init_legislatura:
                    current_start_date = date(ano, 2, 1)
                else:
                    current_start_date = date(ano, 1, 1)

                if current_start_date > date.today():
                    break
            
                temp_date = current_start_date
                while temp_date.year == ano:
                    task = self._fetch_period_pages(
                        session=session,
                        id_legislatura=id_legislatura,
                        current_start_date=temp_date,
                        request_tries=request_tries,
                        # Passar os parâmetros que faltavam
                        id_proposicao=id_proposicao,
                        id_evento=id_evento,
                        id_orgao=id_orgao,
                        itens=itens
                    )
                    tasks.append(task)

                    temp_date = add_months(temp_date, 3)
                    if temp_date > date.today():
                        break

            print(f'Starting parallels {len(tasks)} tasks')
            results = await asyncio.gather(*tasks)

            # Aplaina a lista de listas em uma única lista
            all_votacoes = [item for sublist in results if sublist for item in sublist]

            return all_votacoes