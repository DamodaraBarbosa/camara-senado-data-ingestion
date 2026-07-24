from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncHistoricoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/historico'

    async def extract(self, deputados: json):
        session = aiohttp.ClientSession()
        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_historico = []

        try:
            tasks = []
            for deputado_id in deputados_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=deputado_id))
                tasks.append((deputado_id, task))

            results = await asyncio.gather(*[task for _, task in tasks])

            for (deputado_id, _), data in zip(tasks, results):
                historico_data = data.get('dados', [])
                for historico in historico_data:
                    historico['deputado_id'] = deputado_id
                all_historico.extend(historico_data)

        except Exception as e:
            print(f'Error while extracting historico: {e}')

        await session.close()
        return all_historico
