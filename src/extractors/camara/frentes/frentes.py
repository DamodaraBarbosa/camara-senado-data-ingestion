from extractors.camara.base import CamaraBaseExtractor
import aiohttp


class AsyncFrentesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'frentes'
    LEGISLATURAS = 'legislaturas'

    async def _fetch_pages(
        self,
        session,
        id_legislatura,
        request_tries,
        itens
    ):
        extracted_data = []
        page = 1
        empty_count = 0

        while empty_count < request_tries:
            try:
                current_params = {
                    'itens': itens,
                    'pagina': page
                }

                current_params = {k: v for k, v in current_params.items() if v is not None}

                response = await self.client.get(session, self.ENDPOINT, params=current_params)
                data = response.get('dados', [])

                if not data:
                    empty_count += 1
                    page += 1
                    continue

                for frente in data:
                    frente['idLegislatura'] = id_legislatura
                    extracted_data.append(frente)

                page += 1
                empty_count = 0

            except Exception as e:
                print(f"Error fetching data for legislatura {id_legislatura}, page {page}: {e}")
                empty_count += 1

        return extracted_data

    async def extract(
        self,
        init_legislatura: int = None,
        itens: int = 100,
        request_tries: int = 4
    ):
        all_frentes = []

        async with aiohttp.ClientSession() as session:
            legislaturas = await self.client.get(session, self.LEGISLATURAS)
            legislaturas_data = legislaturas.get('dados', [])
            current_legislatura = max([leg['id'] for leg in legislaturas_data]) if legislaturas_data else 0

            start = init_legislatura if init_legislatura is not None else current_legislatura

            for id_legislatura in range(start, current_legislatura + 1):
                frentes_data = await self._fetch_pages(session, id_legislatura, request_tries, itens)
                all_frentes.extend(frentes_data)

        return all_frentes
