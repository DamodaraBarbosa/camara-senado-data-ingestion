from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncVotacoesIdsExtractor(CamaraBaseExtractor):
    ENDPOINT = 'votacoes/{id}'

    async def extract(
        self,
        votacoes: json,
        batch_size: int = 100
    ):
        votacoes_ids = list(dict.fromkeys(votacao.get('id') for votacao in votacoes if votacao.get('id')))
        all_ids = []

        async with aiohttp.ClientSession() as session:
            for batch_start in range(0, len(votacoes_ids), batch_size):
                batch_ids = votacoes_ids[batch_start:batch_start + batch_size]
                tasks = [
                    self.client.get(session, self.ENDPOINT.format(id=votacao_id))
                    for votacao_id in batch_ids
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for votacao_id, result in zip(batch_ids, results):
                    if isinstance(result, Exception):
                        print(f'Error while extracting ids for votacao {votacao_id}: {result}')
                        continue
                    if result.get('dados'):
                        all_ids.append(result.get('dados'))

                print(f'[votacoes_ids] Lote {batch_start // batch_size + 1} concluído: {len(all_ids)} records')

        return all_ids
