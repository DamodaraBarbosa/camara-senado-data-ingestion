from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp

class AsyncLideresExtractor(CamaraBaseExtractor):
    ENDPOINT = 'partidos/{id}/lideres'

    async def _fetch_pages(
            self,
            session,
            id_partido,
            request_tries,
            itens
        ):
        extracted_data = []
        page = 1
        empty_count = 0
        current_params = {}

        while empty_count < request_tries:
            try:
                current_params = {
                    'itens': itens,
                    'pagina': page
                }

                current_params = {k: v for k, v in current_params.items() if v is not None}

                response = await self.client.get(session, self.ENDPOINT.format(id=id_partido), params=current_params)
                data = response.get('dados', [])

                if not data:
                    empty_count += 1
                    page += 1
                    continue

                for lider in data:
                    lider['idPartido'] = id_partido

                extracted_data.extend(data)
                empty_count = 0
                page += 1

            except Exception as e:
                print(f'Error fetching lideres from API with params {current_params}. Error: {e}')
                break

        return extracted_data

    async def extract(
            self,
            partidos: json,
            itens: int = 100,
            request_tries: int = 4
        ):
        partidos_ids = list(dict.fromkeys(partido.get('id') for partido in partidos if partido.get('id')))
                            
        async with aiohttp.ClientSession() as session:
            tasks = []

            async with aiohttp.ClientSession() as session:
                for partido in partidos_ids:
                    task = self._fetch_pages(
                        session=session,
                        id_partido=partido,
                        request_tries=request_tries,
                        itens=itens
                    )
                    tasks.append(task)

                results = await asyncio.gather(*tasks)

                all_lideres = [item for sublist in results if sublist for item in sublist]

                return all_lideres