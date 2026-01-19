from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp

class AsyncVotacoesOrientacoes(CamaraBaseExtractor):
    ENDPOINT = 'votacoes/{id}/orientacoes'

    async def extract(
            self,
            votacoes: json
        ):
        votacoes_ids = list(dict.fromkeys(votacao.get('id') for votacao in votacoes if votacao.get('id')))
        all_orientacoes = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for votacao_id in votacoes_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=votacao_id))
                tasks.append(task)

            results = await asyncio.gather(*tasks)  
            all_orientacoes.extend(results)

        return all_orientacoes