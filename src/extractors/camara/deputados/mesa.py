from extractors.camara.base import CamaraBaseExtractor
import aiohttp


class AsyncMesaExtractor(CamaraBaseExtractor):
    ENDPOINT = 'legislaturas/{id}/mesa'
    LEGISLATURAS = 'legislaturas'

    async def extract(self, init_legislatura: int = None):
        session = aiohttp.ClientSession()
        legislaturas = (await self.client.get(session, self.LEGISLATURAS))['dados']
        current_legislatura = max(legislatura['id'] for legislatura in legislaturas)

        start = init_legislatura if init_legislatura is not None else current_legislatura
        all_mesa = []

        try:
            for legislatura in range(start, current_legislatura + 1):
                response = await self.client.get(session, self.ENDPOINT.format(id=legislatura))
                data = response.get('dados', [])
                all_mesa.extend(data)

        except Exception as e:
            print(f'Error while extracting mesa for legislatura {legislatura}: {e}')

        await session.close()
        return all_mesa
