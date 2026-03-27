from extractors.camara.base import CamaraBaseExtractor
from datetime import datetime
import json
import aiohttp


class AsyncDespesasExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/despesas'

    async def extract(
        self,
        deputados: json,
        init_legislatura: int = None,
        month: int = None,
        items: int = 1000,
        request_tries: int = 4
    ):
        session = aiohttp.ClientSession()
        legislatura = await self.client.get(session, 'legislaturas', params={'id': init_legislatura})
        start_legislatura_date = legislatura['dados'][0].get('dataInicio', None)
        start_legislatura_year = int(start_legislatura_date.split('-')[0]) if start_legislatura_date else None

        current_year = datetime.now().year
        start_year = start_legislatura_year if start_legislatura_year is not None else current_year
        years_range = [range(start_year, current_year + 1)]

        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))

        all_expenses = []

        for deputado_id in deputados_ids:
            page = 1
            empty_count = 0

            while empty_count < request_tries:
                try:
                    params = {
                        'ano': years_range,
                        'mes': month,
                        'pagina': page,
                        'itens': items
                    }

                    params = {k: v for k, v in params.items() if v is not None}

                    response = await self.client.get(session, self.ENDPOINT.format(id=deputado_id), params=params)
                    data = response.get('dados', [])

                    if not data:
                        empty_count += 1
                        page += 1
                        continue

                    empty_count = 0

                    for despesa in data:
                        despesa['deputado_id'] = deputado_id

                    all_expenses.extend(data)
                    page += 1

                except Exception as e:
                    print(f'Error while extracting despesas for deputado {deputado_id}: {e}')

        await session.close()
        return all_expenses
