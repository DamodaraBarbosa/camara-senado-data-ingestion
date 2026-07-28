from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncTemasExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/temas'

    async def extract(
        self,
        proposicoes: json,
        batch_size: int = 100
    ):
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id')
                               for proposicao in proposicoes if proposicao.get('id')))
        all_temas = []
        errors = 0
        skipped = 0

        async with aiohttp.ClientSession() as session:
            for batch_start in range(0, len(proposicoes_ids), batch_size):
                batch_ids = proposicoes_ids[batch_start:batch_start + batch_size]
                tasks = [
                    self._fetch_temas(session, proposicao_id)
                    for proposicao_id in batch_ids
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for proposicao_id, result in zip(batch_ids, results):
                    if isinstance(result, Exception):
                        errors += 1
                        continue

                    if result is None:
                        skipped += 1
                        continue

                    temas_data = result.get('dados', [])
                    if temas_data:
                        for tema in temas_data:
                            tema['idProposicao'] = proposicao_id
                        all_temas.extend(temas_data)

                batch_num = batch_start // batch_size + 1
                print(
                    f'[temas] Lote {batch_num} concluído: {len(all_temas)} registros '
                    f'(erros: {errors}, vazios: {skipped})'
                )

        return all_temas

    async def _fetch_temas(self, session, proposicao_id):
        try:
            return await self.client.get(session, self.ENDPOINT.format(id=proposicao_id))
        except Exception:
            return None
