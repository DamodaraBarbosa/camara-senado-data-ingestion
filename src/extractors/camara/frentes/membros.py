from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import asyncio


class AsyncFrentesMembrosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'frentes/{id}/membros'

    async def extract(
        self,
        frentes,
        batch_size: int = 100
    ):
        frentes_ids = list(frente.get('id') for frente in frentes if frente.get('id'))
        all_membros = []

        async with aiohttp.ClientSession() as session:
            for batch_start in range(0, len(frentes_ids), batch_size):
                batch_ids = frentes_ids[batch_start:batch_start + batch_size]
                tasks = [
                    self.client.get(session, self.ENDPOINT.format(id=frente_id))
                    for frente_id in batch_ids
                ]

                results = await asyncio.gather(*tasks)

                for batch_offset, result in enumerate(results):
                    membros_data = result.get('dados', [])
                    for membro in membros_data:
                        membro['idFrente'] = batch_ids[batch_offset]
                    all_membros.extend(membros_data)

                print(f'[frentes/membros] Lote {batch_start // batch_size + 1} concluído: {len(all_membros)} membros')

        return all_membros
