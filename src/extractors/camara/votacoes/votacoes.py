from extractors.camara.base import CamaraBaseExtractor
import asyncio
import aiohttp
import time
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
        request_tries: int = 4,
        batch_size: int = 50
    ):
        self.partial = False
        start_time = time.monotonic()
        budget_seconds = 540  # 540s de 600s do handler, margem de 60s

        async with aiohttp.ClientSession() as session:
            params = {}
            if init_legislatura is not None:
                params['id'] = init_legislatura
            legilslatura = await self.client.get(session, self.LEGISLATURA, params=params)

            if not legilslatura.get('dados'):
                start_legislatura_year = None
            else:
                # Get most recent legislature (max id) for deterministic behavior
                legislaturas = legilslatura['dados']
                current_legislatura = max((l['id'] for l in legislaturas), default=None)
                current_leg_data = next((l for l in legislaturas if l['id'] == current_legislatura), None)
                start_legislatura_date = current_leg_data.get('dataInicio', None) if current_leg_data else None
                start_legislatura_year = int(start_legislatura_date.split('-')[0]) if start_legislatura_date else None

            current_year = datetime.now().year
            start_year = start_legislatura_year if start_legislatura_year else current_year
            years_range = range(start_year, current_year + 1)

            # Build list of periods to fetch
            period_specs = []

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
                    period_specs.append({
                        'id_legislatura': id_legislatura,
                        'current_start_date': temp_date,
                    })

                    temp_date = add_months(temp_date, 3)
                    if temp_date > date.today():
                        break

            all_votacoes = []
            total_periods = len(period_specs)

            # Process em lotes com orçamento de tempo
            for batch_start in range(0, total_periods, batch_size):
                # Checar orçamento antes de disparar novo lote
                elapsed = time.monotonic() - start_time
                if elapsed >= budget_seconds:
                    print(f'[votacoes] Orçamento de tempo esgotado ({elapsed:.0f}s >= {budget_seconds}s), retornando dados parciais: {batch_start}/{total_periods} períodos')
                    self.partial = True
                    break

                batch_specs = period_specs[batch_start:batch_start + batch_size]
                tasks = [
                    self._fetch_period_pages(
                        session=session,
                        id_legislatura=spec['id_legislatura'],
                        current_start_date=spec['current_start_date'],
                        id_proposicao=id_proposicao,
                        id_evento=id_evento,
                        id_orgao=id_orgao,
                        itens=itens
                    )
                    for spec in batch_specs
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for spec, result in zip(batch_specs, results):
                    if isinstance(result, Exception):
                        print(f'[votacoes] Período {spec["current_start_date"]} falhou: {result}')
                        continue

                    all_votacoes.extend(result)

                batch_num = batch_start // batch_size + 1
                print(f'[votacoes] Lote {batch_num} concluído: {len(all_votacoes)} registros')

            return all_votacoes
