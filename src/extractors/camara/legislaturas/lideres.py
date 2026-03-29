from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncLegislaturaLideresExtractor(CamaraBaseExtractor):
    ENDPOINT = 'legislaturas/{id}/lideres'

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

                response = await self.client.get(
                    session,
                    self.ENDPOINT.format(id=id_legislatura),
                    params=current_params,
                )
                data = response.get('dados', [])

                if not data:
                    empty_count += 1
                    page += 1
                    continue

                for lider in data:
                    lider['idLegislatura'] = id_legislatura

                extracted_data.extend(data)
                empty_count = 0
                page += 1

            except Exception as e:
                print(f'Error fetching lideres from API with params {current_params}. Error: {e}')
                break

        return extracted_data

    async def extract(
        self,
        legislaturas: json,
        itens: int = 100,
        request_tries: int = 4
    ):
        legislaturas_ids = [
            legislatura.get('id')
            for legislatura in legislaturas
            if legislatura.get('id')
        ]

        async with aiohttp.ClientSession() as session:
            tasks = []

            for legislatura in legislaturas_ids:
                task = self._fetch_pages(
                    session=session,
                    id_legislatura=legislatura,
                    request_tries=request_tries,
                    itens=itens
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            all_lideres = [item for sublist in results if sublist for item in sublist]

            return all_lideres
