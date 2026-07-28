from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import json
import aiohttp


class AsyncLegislaturaLideresExtractor(CamaraBaseExtractor):
    ENDPOINT = 'legislaturas/{id}/lideres'

    async def _fetch_pages(
        self,
        session,
        id_legislatura,
        itens
    ):
        extracted_data = await self.client.get_all_pages(
            session,
            self.ENDPOINT.format(id=id_legislatura),
            itens=itens
        )
        for lider in extracted_data:
            lider['idLegislatura'] = id_legislatura
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
                    itens=itens
                )
                tasks.append(task)

            results, coverage, _errors = await gather_aligned(tasks, label='legislaturas/lideres')

            all_lideres = [item for sublist in results if sublist for item in sublist]

            self.partial = coverage < 0.99
        return all_lideres
