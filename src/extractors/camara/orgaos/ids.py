from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp

class AsyncOrgaosIdsExtractor(CamaraBaseExtractor):
    ENDPOINT = 'orgaos/{id}'

    async def extract(
            self,
            orgaos: json
        ):
        orgaos_ids = list(dict.fromkeys(orgao.get('id') for orgao in orgaos if orgao.get('id')))
        all_ids = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for orgao_id in orgaos_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=orgao_id))
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            for result in results:
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        return all_ids