from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import json
import asyncio
import aiohttp


class AsyncMandatosExternosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/mandatosExternos'

    async def extract(self, deputados: json):
        session = aiohttp.ClientSession()
        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_mandatos_externos = []

        try:
            tasks = []
            for deputado_id in deputados_ids:
                task = self.client.get(session, self.ENDPOINT.format(id=deputado_id))
                tasks.append((deputado_id, task))

            results, coverage, _errors = await gather_aligned(
                [task for _, task in tasks], label='deputados/mandatos_externos')

            for (deputado_id, _), data in zip(tasks, results):
                if data is None:
                    continue
                mandatos_data = data.get('dados', [])
                for mandato in mandatos_data:
                    mandato['deputado_id'] = deputado_id
                all_mandatos_externos.extend(mandatos_data)

        except Exception as e:
            print(f'Error while extracting mandatos externos: {e}')

        await session.close()
        self.partial = coverage < 0.99
        return all_mandatos_externos
