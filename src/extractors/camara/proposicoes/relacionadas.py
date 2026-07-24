from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncRelacionadasExtractor(CamaraBaseExtractor):
    ENDPOINT = 'proposicoes/{id}/relacionadas'

    async def extract(
        self,
        proposicoes: json
    ):
        session = aiohttp.ClientSession()
        proposicoes_ids = list(dict.fromkeys(proposicao.get('id')
                               for proposicao in proposicoes if proposicao.get('id')))
        all_relacionadas = []

        tasks = []
        for proposicao_id in proposicoes_ids:
            task = self.client.get(session, self.ENDPOINT.format(id=proposicao_id))
            tasks.append((proposicao_id, task))

        try:
            results = await asyncio.gather(*[task for _, task in tasks])

            for (proposicao_id, _), data in zip(tasks, results):
                relacionadas_data = data.get('dados', [])
                for relacionada in relacionadas_data:
                    relacionada['relacionadoProposicao'] = proposicao_id
                all_relacionadas.extend(relacionadas_data)

        except Exception as e:
            print(f'Error while extracting relacionadas: {e}')

        await session.close()
        return all_relacionadas
