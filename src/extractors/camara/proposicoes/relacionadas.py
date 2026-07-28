from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncRelacionadasExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/relacionadas'

    async def extract(
        self,
        proposicoes: json,
        batch_size: int = 100
    ):
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id')
                               for proposicao in proposicoes if proposicao.get('id')))
        all_relacionadas = []

        async with aiohttp.ClientSession() as session:
            for batch_start in range(0, len(proposicoes_ids), batch_size):
                batch_ids = proposicoes_ids[batch_start:batch_start + batch_size]
                tasks = [
                    self.client.get(session, self.ENDPOINT.format(id=proposicao_id))
                    for proposicao_id in batch_ids
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for proposicao_id, result in zip(batch_ids, results):
                    if isinstance(result, Exception):
                        print(f'Error while extracting relacionadas for proposicao {proposicao_id}: {result}')
                        continue
                    relacionadas_data = result.get('dados', [])
                    for relacionada in relacionadas_data:
                        relacionada['relacionadoProposicao'] = proposicao_id
                    all_relacionadas.extend(relacionadas_data)

                print(f'[relacionadas] Lote {batch_start // batch_size + 1} concluído: {len(all_relacionadas)} records')

        return all_relacionadas
