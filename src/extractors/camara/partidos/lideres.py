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
        itens
    ):
        extracted_data = await self.client.get_all_pages(
            session,
            self.ENDPOINT.format(id=id_partido),
            itens=itens
        )
        for lider in extracted_data:
            lider['idPartido'] = id_partido

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

            for partido in partidos_ids:
                task = self._fetch_pages(
                    session=session,
                    id_partido=partido,
                    itens=itens
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            all_lideres = [item for sublist in results if sublist for item in sublist]

            return all_lideres
