from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import asyncio

class AsyncFrentesMembrosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'frentes/{id}/membros'

    async def extract(
            self, 
            frentes
        ):
        frentes_ids = list(frente.get('id') for frente in frentes if frente.get('id'))
        all_membros = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for frente_id in frentes_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=frente_id))
                tasks.append(task)
                print(f'Fetching membros for frente ID: {frente_id}')

            print('Waiting for tasks to complete...')
            results = await asyncio.gather(*tasks)
            print('All tasks completed.')

            for index, result in enumerate(results):
                print(f'Processing result for frente ID: {frentes_ids[index]}')
                membros_data = result.get('dados', [])
                frente_id = frentes_ids[index]
                print(f'Frente ID: {frente_id}, Membros count: {len(membros_data)}')
                for membro in membros_data:
                    membro['idFrente'] = frente_id
                all_membros.extend(membros_data)
        
        return all_membros