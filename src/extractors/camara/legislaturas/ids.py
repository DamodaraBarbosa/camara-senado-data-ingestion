from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp

class AsyncLegislaturaIdsExtractor(CamaraBaseExtractor):
    ENDPOINT = 'legislaturas/{id}'

    async def extract(
            self,
            legislaturas: json       
        ):
        legislaturas_ids = list(legislatura.get('id') for legislatura in legislaturas if legislatura.get('id'))
        all_ids = []

        async with aiohttp.ClientSession() as session:
            tasks = []
            
            for legislatura in legislaturas_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=legislatura))
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            for result in results:
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        return all_ids