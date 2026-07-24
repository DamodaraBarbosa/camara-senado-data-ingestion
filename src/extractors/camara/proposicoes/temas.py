from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncTemasExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/temas'

    async def extract(
        self,
        proposicoes: json
    ):
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id')
                               for proposicao in proposicoes if proposicao.get('id')))
        all_temas = []

        async with aiohttp.ClientSession() as session:
            tasks = []
            for proposicao_id in proposicoes_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=proposicao_id))
                tasks.append((proposicao_id, task))

            try:
                results = await asyncio.gather(*[task for _, task in tasks])

                for (proposicao_id, _), data in zip(tasks, results):
                    temas_data = data.get('dados', [])
                    for tema in temas_data:
                        tema['idProposicao'] = proposicao_id
                    all_temas.extend(temas_data)

            except Exception as e:
                print(f'Error while extracting temas: {e}')

        return all_temas
