from extractors.camara.base import CamaraBaseExtractor
import aiohttp

class AsyncCodigoTemaExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/proposicoes/codTema'

    async def extract(self):
        session = aiohttp.ClientSession()
        try:
            response = await self.client.get(session, self.ENDPOINT)
            data = response.get('dados', [])
            await session.close()
            return data
        
        except Exception as e:
            print(f'Error while extracting codigo tema: {e}')
            await session.close()
            return []