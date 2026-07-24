from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp
from datetime import datetime, date
from utils.utils import add_months


class AsyncEventosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'orgaos/{id}/eventos'
    LEGISLATURA_ENDPOINT = 'legislaturas'

    async def _fetch_period_pages(
        self,
        session,
        id_legislatura,
        current_start_date,
        id_orgao,
        itens
    ):
        params = {
            'dataInicio': current_start_date.isoformat(),
        }
        params = {k: v for k, v in params.items() if v is not None}

        extracted_data = await self.client.get_all_pages(
            session,
            self.ENDPOINT.format(id=id_orgao),
            params=params,
            itens=itens
        )
        for evento in extracted_data:
            evento['idLegislatura'] = id_legislatura
            evento['idOrgao'] = id_orgao

        return extracted_data

    async def extract(
        self,
        init_legislatura: int = None,
        orgaos: json = None,
        itens: int = 100,
        request_tries: int = 4
    ):
        async with aiohttp.ClientSession() as session:
            orgaos_id = list(
                dict.fromkeys(
                    [orgao.get('id') for orgao in orgaos if orgao.get('id')]
                )
            )
            params = {}
            if init_legislatura is not None:
                params['id'] = init_legislatura
            start_legislatura = await self.client.get(
                session,
                self.LEGISLATURA_ENDPOINT,
                params=params,
            )
            start_legislatura_date = start_legislatura['dados'][0].get('dataInicio', None) if start_legislatura.get('dados') else None
            start_legislatura_year = int(start_legislatura_date.split('-')[0]) if start_legislatura_date else None

            current_year = datetime.now().year
            start_year = start_legislatura_year if start_legislatura_year else current_year
            years_range = range(start_year, current_year + 1)

            tasks = []

            for orgao_id in orgaos_id:
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
                            id_orgao=orgao_id,
                            itens=itens
                        )
                        tasks.append(task)

                        temp_date = add_months(temp_date, 3)
                        if temp_date > date.today():
                            break

            results = await asyncio.gather(*tasks)

            all_eventos = [item for sublist in results if sublist for item in sublist]

            return all_eventos
