from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import aiohttp


class SituacoesOrgaoExtractor(CamaraBaseExtractor):
    ENDPOINT = 'referencias/situacoesOrgao'

    async def extract(self):
        all_situacoes = []

        async with aiohttp.ClientSession() as session:
            tasks = []

            task = self.client.get(session, self.ENDPOINT)
            tasks.append(task)

            results, coverage, _errors = await gather_aligned(tasks, label='orgaos/situacoes_orgao')

            for result in results:
                if result is None:
                    continue
                codigo = result.get('dados', [])
                all_situacoes.append(codigo)

            self.partial = coverage < 0.99
        return all_situacoes
