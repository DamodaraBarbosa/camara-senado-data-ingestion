from extractors.camara.base import CamaraBaseExtractor
import aiohttp

class AsyncTiposProposicaoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/tiposProposicao'

    async def extract(self):
        session = aiohttp.ClientSession()
        try:
            response = await self.client.get(session, self.ENDPOINT)
            data = response.get('dados', [])
            await session.close()
            return data
        
        except Exception as e:
            print(f'Error while extracting tipos proposicao: {e}')
            await session.close()
            return []