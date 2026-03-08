from extractors.camara.base import CamaraBaseExtractor
import json
import aiohttp

class AsyncFrentesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/frentes'

    async def extract(self, deputados: json):
        session = aiohttp.ClientSession()
        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_frentes = []

        try:
            for deputado_id in deputados_ids:
                response = await self.client.get(session, self.ENDPOINT.format(id=deputado_id))
                data = response.get('dados', [])
                
                for frente in data:
                    frente['deputado_id'] = deputado_id
                
                all_frentes.extend(data)

        except Exception as e:
            print(f'Error while extracting frentes for deputado {deputado_id}: {e}')

        await session.close()
        return all_frentes