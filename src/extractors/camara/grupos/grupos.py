from extractors.camara.base import CamaraBaseExtractor
import aiohttp


class AsyncGruposExtractor(CamaraBaseExtractor):
    ENDPOINT = 'grupos'
    LEGISLATURAS = 'legislaturas'

    async def extract(
        self,
        init_legislatura: int = None,
        itens: int = 100,
        request_tries: int = 4
    ):
        all_grupos = []

        async with aiohttp.ClientSession() as session:
            legislaturas = await self.client.get(session, self.LEGISLATURAS)
            legislaturas_data = legislaturas.get('dados', [])
            current_legislatura = max([leg['id'] for leg in legislaturas_data]) if legislaturas_data else 0

            start = init_legislatura if init_legislatura is not None else current_legislatura

            for id_legislatura in range(start, current_legislatura + 1):
                grupos_data = await self.client.get_all_pages(session, self.ENDPOINT, itens=itens)

                for grupo in grupos_data:
                    grupo['idLegislatura'] = id_legislatura

                all_grupos.extend(grupos_data)

        return all_grupos
