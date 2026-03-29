from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncVotacoesIdsExtractor(CamaraBaseExtractor):
    ENDPOINT = 'votacoes/{id}'

    async def extract(
        self,
        votacoes: json
    ):
        votacoes_ids = list(dict.fromkeys(votacao.get('id') for votacao in votacoes if votacao.get('id')))
        all_ids = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for votacao_id in votacoes_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=votacao_id))
                tasks.append(task)

            results = await asyncio.gather(*tasks)
            for result in results:
                if result.get('dados'):
                    all_ids.append(result.get('dados'))

        return all_ids
