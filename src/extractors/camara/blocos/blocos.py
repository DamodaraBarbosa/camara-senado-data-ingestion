from extractors.camara.base import CamaraBaseExtractor
import aiohttp


class AsyncBlocosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'blocos'
    LEGISLATURAS = 'legislaturas'

    async def extract(
        self,
        init_legislatura: int = None,
        itens: int = 100,
        request_tries: int = 4
    ):
        all_blocos = []

        async with aiohttp.ClientSession() as session:
            legislaturas = await self.client.get(session, self.LEGISLATURAS)
            legislaturas_data = legislaturas.get('dados', [])

            legislaturas_ids = [legislatura.get('id') for legislatura in legislaturas_data if legislatura.get('id')]
            current_legislatura = max(legislaturas_ids)
            start = init_legislatura if init_legislatura is not None else current_legislatura

            for id_legislatura in range(start, current_legislatura + 1):
                blocos_data = await self.client.get_all_pages(session, self.ENDPOINT, itens=itens)

                for bloco in blocos_data:
                    bloco['idLegislatura'] = id_legislatura

                all_blocos.extend(blocos_data)

        return all_blocos
