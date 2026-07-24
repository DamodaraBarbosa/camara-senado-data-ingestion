from extractors.camara.base import CamaraBaseExtractor
import aiohttp


class AsyncLideresExtractor(CamaraBaseExtractor):
    ENDPOINT = 'legislaturas/{id}/lideres'
    LEGISLATURAS = 'legislaturas'

    async def extract(
        self, init_legislatura: int = None,
        items: int = 50,
        request_tries: int = 4
    ):
        async with aiohttp.ClientSession() as session:
            legislaturas = (await self.client.get(session, self.LEGISLATURAS))['dados']
            current_legislatura = max(legislatura['id'] for legislatura in legislaturas)

            start_legislatura_date = init_legislatura if init_legislatura is not None else current_legislatura
            all_lideres = []

            for legislatura in range(start_legislatura_date, current_legislatura + 1):
                data = await self.client.get_all_pages(
                    session,
                    self.ENDPOINT.format(id=legislatura),
                    itens=items
                )
                all_lideres.extend(data)

            return all_lideres
