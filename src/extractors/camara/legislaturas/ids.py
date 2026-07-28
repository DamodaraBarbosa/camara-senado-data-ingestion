from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
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

            results, coverage, _errors = await gather_aligned(tasks, label='legislaturas/ids')

            for result in results:
                if result is None:
                    continue
                ids_data = result.get('dados', {})
                all_ids.append(ids_data)

        self.partial = coverage < 0.99
        return all_ids
