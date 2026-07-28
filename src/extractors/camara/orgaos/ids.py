from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
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

            results, coverage, _errors = await gather_aligned(tasks, label='orgaos/ids')

            for result in results:
                if result is None:
                    continue
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        self.partial = coverage < 0.99
        return all_ids
