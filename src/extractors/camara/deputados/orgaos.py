from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import asyncio
import json
import aiohttp


class AsyncOrgaosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/orgaos'

    async def extract(
        self, deputados: json,
        init_legislatura: int = None,
        items: int = 50,
        request_tries: int = 4
    ):
        async with aiohttp.ClientSession() as session:
            params = {}
            if init_legislatura is not None:
                params['id'] = init_legislatura
            legislatura = await self.client.get(session, 'legislaturas', params=params)
            start_legislatura_date = legislatura['dados'][0].get('dataInicio', None) if legislatura.get('dados') else None

            deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))

            print(f'[orgaos] Iniciando extração para {len(deputados_ids)} deputados | data início legislatura: {start_legislatura_date}')

            tasks = [
                self._fetch_deputado(session, deputado_id, start_legislatura_date, items)
                for deputado_id in deputados_ids
            ]
            results, coverage, _errors = await gather_aligned(tasks, label='deputados/orgaos')

            all_bodies = [body for bodies in results for body in bodies]
            print(f'[orgaos] Extração concluída | total de orgãos: {len(all_bodies)}')

            self.partial = coverage < 0.99
        return all_bodies

    async def _fetch_deputado(self, session, deputado_id, start_legislatura_date, items):
        params = {'dataInicio': start_legislatura_date} if start_legislatura_date else {}

        bodies = await self.client.get_all_pages(
            session,
            self.ENDPOINT.format(id=deputado_id),
            params=params,
            itens=items
        )
        for orgao in bodies:
            orgao['deputado_id'] = deputado_id

        return bodies
