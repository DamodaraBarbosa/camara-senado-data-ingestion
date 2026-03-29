from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncVotosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'votacoes/{id}/votos'

    async def extract(
        self,
        votacoes: json
    ):
        votacoes_ids = list(dict.fromkeys(votacao.get('id') for votacao in votacoes if votacao.get('id')))
        all_votos = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            for votacao_id in votacoes_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=votacao_id))
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            for index, result in enumerate(results):
                votacao_id = votacoes_ids[index]
                votos = result.get('dados', [])

                for voto in votos:
                    voto['votacao_id'] = votacao_id

                all_votos.extend(votos)

        return all_votos
