from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncAutoresExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/autores'

    async def extract(
        self,
        proposicoes: json
    ):
        session = aiohttp.ClientSession()
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id')
                               for proposicao in proposicoes if proposicao.get('id')))
        all_autores = []

        tasks = []
        for proposicao_id in proposicoes_ids:
            task = self.client.get(session, self.ENDPOINT.format(id=proposicao_id))
            tasks.append((proposicao_id, task))

        try:
            results = await asyncio.gather(*[task for _, task in tasks])

            for (proposicao_id, _), data in zip(tasks, results):
                autores_data = data.get('dados', [])
                for autor in autores_data:
                    autor['idProposicao'] = proposicao_id
                all_autores.extend(autores_data)

        except Exception as e:
            print(f'Error while extracting autores: {e}')

        await session.close()
        return all_autores
