from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import assert_usable, gather_aligned
import json
import aiohttp


class AsyncDiscursosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/discursos'
    LEGISLATURAS = 'legislaturas'

    async def extract(
        self,
        deputados: json,
        init_legislatura: int = None,
        items: int = 50,
        request_tries: int = 4
    ):
        async with aiohttp.ClientSession() as session:
            legislatura = (await self.client.get(session, self.LEGISLATURAS))['dados']
            current_legislatura = max(legislatura['id'] for legislatura in legislatura)
            start_legislatura = init_legislatura if init_legislatura is not None else current_legislatura
            legislaturas_range = list(range(start_legislatura, current_legislatura + 1))

            deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))

            print(
                f'[discursos] Iniciando extração | {len(deputados_ids)} deputados | '
                f'legislaturas: {legislaturas_range}'
            )

            tasks = [
                self._fetch_deputado(session, deputado_id, legislatura_id, items)
                for deputado_id in deputados_ids
                for legislatura_id in legislaturas_range
            ]
            results, coverage, errors = await gather_aligned(tasks, label='deputados/discursos')

            all_discursos = [discurso for discursos in results if discursos for discurso in discursos]
            print(f'[discursos] Extração concluída | total de discursos: {len(all_discursos)}')

            self.partial = coverage < 0.99
            assert_usable(all_discursos, coverage, errors, label='deputados/discursos')
        return all_discursos

    async def _fetch_deputado(self, session, deputado_id, legislatura_id, items):
        params = {'idLegislatura': legislatura_id}

        discursos = await self.client.get_all_pages(
            session,
            self.ENDPOINT.format(id=deputado_id),
            params=params,
            itens=items
        )
        for discurso in discursos:
            discurso['deputado_id'] = deputado_id

        print(f'[discursos] Deputado {deputado_id} | legislatura {legislatura_id} | +{len(discursos)} discursos')
        return discursos

        print(f'[discursos] Concluído deputado {deputado_id} | total de discursos: {len(discursos)}')
        return discursos
