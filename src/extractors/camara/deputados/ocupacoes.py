from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncOcupacoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/ocupacoes'

    async def extract(self, deputados: json):
        session = aiohttp.ClientSession()
        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_ocupacoes = []

        try:
            tasks = []
            for deputado_id in deputados_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=deputado_id))
                tasks.append((deputado_id, task))

            results = await asyncio.gather(*[task for _, task in tasks])

            for (deputado_id, _), data in zip(tasks, results):
                ocupacoes_data = data.get('dados', [])
                for ocupacao in ocupacoes_data:
                    ocupacao['deputado_id'] = deputado_id
                all_ocupacoes.extend(ocupacoes_data)

        except Exception as e:
            print(f'Error while extracting ocupacoes: {e}')

        await session.close()
        return all_ocupacoes
