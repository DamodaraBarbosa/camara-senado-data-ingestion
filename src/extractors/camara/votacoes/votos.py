from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp
import time


class AsyncVotosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'votacoes/{id}/votos'

    async def extract(
        self,
        votacoes: json,
        batch_size: int = 100
    ):
        self.partial = False
        start_time = time.monotonic()
        budget_seconds = 540  # 540s de 600s do handler, margem de 60s

        votacoes_ids = list(dict.fromkeys(votacao.get('id') for votacao in votacoes if votacao.get('id')))
        all_votos = []

        async with aiohttp.ClientSession() as session:
            for batch_start in range(0, len(votacoes_ids), batch_size):
                elapsed = time.monotonic() - start_time
                if elapsed >= budget_seconds:
                    print(f'[votos] Orçamento de tempo esgotado ({elapsed:.0f}s >= {budget_seconds}s), retornando dados parciais: {batch_start}/{len(votacoes_ids)} votações')
                    self.partial = True
                    break

                batch_ids = votacoes_ids[batch_start:batch_start + batch_size]
                tasks = [
                    self.client.get(session, self.ENDPOINT.format(id=votacao_id))
                    for votacao_id in batch_ids
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for votacao_id, result in zip(batch_ids, results):
                    if isinstance(result, Exception):
                        print(f'Error while extracting votos for votacao {votacao_id}: {result}')
                        continue
                    votos = result.get('dados', [])

                    for voto in votos:
                        voto['votacao_id'] = votacao_id

                    all_votos.extend(votos)

                print(f'[votos] Lote {batch_start // batch_size + 1} concluído: {len(all_votos)} records')

        return all_votos
