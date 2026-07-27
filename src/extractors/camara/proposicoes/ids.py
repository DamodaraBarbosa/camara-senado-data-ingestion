from extractors.camara.base import CamaraBaseExtractor
import asyncio
import json
import aiohttp
import time


class AsyncIdsExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}'

    async def extract(
        self,
        proposicoes: json,
        batch_size: int = 100
    ):
        self.partial = False
        start_time = time.monotonic()
        budget_seconds = 540  # 540s de 600s do handler, margem de 60s

        proposicoes_ids = list(dict.fromkeys(proposicao.get('id')
                               for proposicao in proposicoes if proposicao.get('id')))
        all_ids = []

        async with aiohttp.ClientSession() as session:
            for batch_start in range(0, len(proposicoes_ids), batch_size):
                elapsed = time.monotonic() - start_time
                if elapsed >= budget_seconds:
                    print(f'[ids] Orçamento de tempo esgotado ({elapsed:.0f}s >= {budget_seconds}s), retornando dados parciais: {batch_start}/{len(proposicoes_ids)} proposições')
                    self.partial = True
                    break

                batch_ids = proposicoes_ids[batch_start:batch_start + batch_size]
                tasks = [
                    self.client.get(session, self.ENDPOINT.format(id=proposicao_id))
                    for proposicao_id in batch_ids
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for proposicao_id, result in zip(batch_ids, results):
                    if isinstance(result, Exception):
                        print(f'Error while extracting ids for proposicao {proposicao_id}: {result}')
                        continue
                    data = result.get('dados', [])
                    all_ids.append(data)

                print(f'[ids] Lote {batch_start // batch_size + 1} concluído: {len(all_ids)} records')

        return all_ids
