from extractors.camara.base import CamaraBaseExtractor
import asyncio
import aiohttp
from datetime import datetime, date
from utils.utils import add_months


class AsyncVotacoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'votacoes'
    LEGISLATURA = 'legislaturas'

    async def _fetch_period_pages(
        self,
        session,
        id_legislatura,
        current_start_date,
        id_proposicao,
        id_evento,
        id_orgao,
        itens
    ):
        params = {
            'idProposicao': id_proposicao,
            'idEvento': id_evento,
            'idOrgao': id_orgao,
            'dataInicio': current_start_date.isoformat(),
        }
        params = {k: v for k, v in params.items() if v is not None}

        extracted_data = await self.client.get_all_pages(
            session,
            self.ENDPOINT,
            params=params,
            itens=itens
        )
        for votacao in extracted_data:
            votacao['idLegislatura'] = id_legislatura

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
            params = {}
            if init_legislatura is not None:
                params['id'] = init_legislatura
            legilslatura = await self.client.get(session, self.LEGISLATURA, params=params)
            start_legislatura_date = legilslatura['dados'][0].get('dataInicio', None) if legilslatura.get('dados') else None
            start_legislatura_year = int(start_legislatura_date.split('-')[0]) if start_legislatura_date else None

            current_year = datetime.now().year
            start_year = start_legislatura_year if start_legislatura_year else current_year
            years_range = range(start_year, current_year + 1)

            tasks = []

            for index, ano in enumerate(years_range):
                id_legislatura = init_legislatura + (index // 4) if init_legislatura is not None else None

                if init_legislatura is not None and index % 4 == 0 and id_legislatura == init_legislatura:
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
                        id_proposicao=id_proposicao,
                        id_evento=id_evento,
                        id_orgao=id_orgao,
                        itens=itens
                    )
                    tasks.append(task)

                    temp_date = add_months(temp_date, 3)
                    if temp_date > date.today():
                        break

            results = await asyncio.gather(*tasks)

            all_votacoes = [item for sublist in results if sublist for item in sublist]

            return all_votacoes
