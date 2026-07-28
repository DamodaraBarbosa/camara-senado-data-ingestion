from extractors.camara.base import CamaraBaseExtractor
import aiohttp


class AsyncCodigoTemaExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/proposicoes/codTema'

    async def extract(self):
        async with aiohttp.ClientSession() as session:
            try:
                response = await self.client.get(session, self.ENDPOINT)
                return response.get('dados', [])
            except Exception as e:
                print(f'[codigo_tema] Error: {e}')
                return []
