from extractors.camara.base import CamaraBaseExtractor
import asyncio
import aiohttp
from datetime import datetime, date
from utils.utils import add_months


class AsyncPartidosMembrosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'partidos/{id}/membros'
    LEGISLATURA = 'legislaturas'

    async def _fetch_period_pages(
        self,
        session,
        id_legislatura,
        id_partido,
        current_start_date,
        itens
    ):
        params = {
            'dataInicio': current_start_date.isoformat(),
        }
        params = {k: v for k, v in params.items() if v is not None}

        extracted_data = await self.client.get_all_pages(
            session,
            self.ENDPOINT.format(id=id_partido),
            params=params,
            itens=itens
        )
        for membro in extracted_data:
            membro['idLegislatura'] = id_legislatura
            membro['idPartido'] = id_partido

        return extracted_data

    async def extract(
        self,
        init_legislatura: int = None,
        partidos: list = None,
        itens: int = 100,
        request_tries: int = 4
    ):
        partidos_id = list(dict.fromkeys(partido.get('id') for partido in partidos if partido.get('id')))

        async with aiohttp.ClientSession() as session:
            params = {}
            if init_legislatura is not None:
                params['id'] = init_legislatura
            legislatura = await self.client.get(session, self.LEGISLATURA, params=params)
            start_legislatura_date = legislatura['dados'][0].get('dataInicio', None) if legislatura.get('dados') else None
            start_legislatura_year = int(start_legislatura_date.split('-')[0]) if start_legislatura_date else None

            current_year = datetime.now().year
            start_year = start_legislatura_year if start_legislatura_year else current_year
            years_range = range(start_year, current_year + 1)

            tasks = []

            for id_partido in partidos_id:
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
                            id_partido=id_partido,
                            current_start_date=temp_date,
                            itens=itens
                        )
                        tasks.append(task)

                        temp_date = add_months(temp_date, 3)
                        if temp_date > date.today():
                            break

            results = await asyncio.gather(*tasks)

            all_membros = [item for sublist in results if sublist for item in sublist]

            return all_membros
