from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncProfissoesExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/profissoes'

    async def extract(self, deputados: json):
        session = aiohttp.ClientSession()
        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_profissoes = []

        try:
            tasks = []
            for deputado_id in deputados_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=deputado_id))
                tasks.append((deputado_id, task))

            results = await asyncio.gather(*[task for _, task in tasks])

            for (deputado_id, _), data in zip(tasks, results):
                profissoes_data = data.get('dados', [])
                for profissao in profissoes_data:
                    profissao['deputado_id'] = deputado_id
                all_profissoes.extend(profissoes_data)

        except Exception as e:
            print(f'Error while extracting profissoes: {e}')

        await session.close()
        return all_profissoes
