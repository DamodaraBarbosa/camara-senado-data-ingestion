from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import json
import asyncio
import aiohttp
from datetime import datetime, date
from utils.utils import add_months


class AsyncMesaExtractor(CamaraBaseExtractor):
    ENDPOINT = 'legislaturas/{id}/mesa'
    LEGISLATURA = 'legislaturas'

    async def extract(
        self,
        legislaturas: json
    ):
        legislaturas_id = list(legislatura.get('id') for legislatura in legislaturas if legislatura.get('id'))
        all_mesas = []

        async with aiohttp.ClientSession() as session:
            legislatura_data = await self.client.get(session, self.LEGISLATURA, params={'id': legislaturas_id[0]})
            start_legislatura_date = legislatura_data['dados'][0].get('dataInicio', None)
            start_legislatura_year = int(start_legislatura_date.split('-')[0]) if start_legislatura_date else None

            current_year = datetime.now().year
            start_year = start_legislatura_year if start_legislatura_year else current_year
            years_range = range(start_year, current_year + 1)

            tasks = []

            for legislatura in legislaturas_id:
                for index, ano in enumerate(years_range):
                    id_legislatura = legislatura + (index // 4)
                    if index % 4 == 0 and id_legislatura == legislatura:
                        current_start_date = date(ano, 2, 1)
                    else:
                        current_start_date = date(ano, 1, 1)

                    if current_start_date > date.today():
                        break

                    temp_date = current_start_date
                    while temp_date.year == ano:
                        task = self.client.get(session, self.ENDPOINT.format(id=legislatura))
                        tasks.append(task)

                        temp_date = add_months(temp_date, 1)
                        if temp_date > date.today():
                            break

            results, coverage, _errors = await gather_aligned(tasks, label='legislaturas/mesa')

            for result in results:
                if result is None:
                    continue
                data = result.get('dados', [])
                all_mesas.extend(data)

            self.partial = coverage < 0.99
        return all_mesas
