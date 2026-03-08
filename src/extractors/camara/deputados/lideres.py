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
        session = aiohttp.ClientSession()
        legislaturas = (await self.client.get(session, self.LEGISLATURAS))['dados']
        current_legislatura = max(legislatura['id'] for legislatura in legislaturas)

        start_legislatura_date = init_legislatura if init_legislatura is not None else current_legislatura
        all_lideres = []

        for legislatura in range(start_legislatura_date, current_legislatura + 1):
            page = 1 
            empty_count = 0

            while empty_count < request_tries:
                try:
                    params = {
                        'itens': items,
                        'pagina': page
                    }

                    params = {k: v for k, v in params.items() if v is not None}

                    response = await self.client.get(session, self.ENDPOINT.format(id=legislatura), params=params)
                    data = response.get('dados', [])

                    if not data:
                        empty_count += 1
                        page += 1
                        continue

                    empty_count = 0

                    all_lideres.extend(data)
                    page += 1

                except Exception as e:
                    print(f'Error while extracting lideres for legislatura {legislatura}: {e}')

        await session.close()
        return all_lideres
