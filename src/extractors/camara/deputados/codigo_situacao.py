from extractors.camara.base import CamaraBaseExtractor
import aiohttp


class AsyncCodigoSituacaoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/deputados/codSituacao'

    async def extract(self):
        session = aiohttp.ClientSession()
        response = await self.client.get(session, self.ENDPOINT)
        data = response.get('dados', [])
        await session.close()
        return data
