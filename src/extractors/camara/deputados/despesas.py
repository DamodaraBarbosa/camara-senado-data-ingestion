from extractors.camara.base import CamaraBaseExtractor
from datetime import datetime
import asyncio
import json
import aiohttp
import time


class AsyncDespesasExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/despesas'

    async def extract(
        self,
        deputados: json,
        init_legislatura: int = None,
        month: int = None,
        items: int = 1000,
        request_tries: int = 4,
        batch_size: int = 40
    ):
        self.partial = False
        start_time = time.monotonic()
        budget_seconds = 540  # 540s de 600s do handler, margem de 60s

        async with aiohttp.ClientSession() as session:
            params = {}
            if init_legislatura is not None:
                params['id'] = init_legislatura
            legislatura = await self.client.get(session, 'legislaturas', params=params)
            start_legislatura_date = legislatura['dados'][0].get('dataInicio', None) if legislatura.get('dados') else None
            start_legislatura_year = int(start_legislatura_date.split('-')[0]) if start_legislatura_date else None

            current_year = datetime.now().year
            start_year = start_legislatura_year if start_legislatura_year is not None else current_year
            years_range = range(start_year, current_year + 1)

            deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))

            print(f'[despesas] Iniciando extração para {len(deputados_ids)} deputados | anos: {list(years_range)}')

            all_expenses = []
            for batch_start in range(0, len(deputados_ids), batch_size):
                elapsed = time.monotonic() - start_time
                if elapsed >= budget_seconds:
                    print(f'[despesas] Orçamento de tempo esgotado ({elapsed:.0f}s >= {budget_seconds}s), retornando dados parciais: {batch_start}/{len(deputados_ids)} deputados')
                    self.partial = True
                    break

                batch_ids = deputados_ids[batch_start:batch_start + batch_size]
                tasks = [
                    self._fetch_deputado(session, deputado_id, years_range, month, items)
                    for deputado_id in batch_ids
                ]
                batch_results = await asyncio.gather(*tasks)
                batch_expenses = [despesa for expenses in batch_results for despesa in expenses]
                all_expenses.extend(batch_expenses)
                print(f'[despesas] Lote {batch_start // batch_size + 1} concluído: {len(batch_expenses)} despesas')

            print(f'[despesas] Extração concluída | total de despesas: {len(all_expenses)}')

            return all_expenses

    async def _fetch_deputado(self, session, deputado_id, years_range, month, items):
        all_expenses = []
        for ano in years_range:
            params = {
                'ano': ano,
                'mes': month,
            }
            params = {k: v for k, v in params.items() if v is not None}

            expenses = await self.client.get_all_pages(
                session,
                self.ENDPOINT.format(id=deputado_id),
                params=params,
                itens=items
            )
            for despesa in expenses:
                despesa['deputadoId'] = deputado_id
            all_expenses.extend(expenses)

        return all_expenses
