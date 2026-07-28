from extractors.camara.base import CamaraBaseExtractor
from utils.concurrency import gather_aligned
import json
import asyncio
import aiohttp


class AsyncEventosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/eventos'

    async def _fetch_all_eventos_for_deputado(self, session, deputado_id, start_legislatura_date, items):
        params = {'dataInicio': start_legislatura_date} if start_legislatura_date else {}
        eventos = await self.client.get_all_pages(
            session,
            self.ENDPOINT.format(id=deputado_id),
            params=params,
            itens=items
        )
        for evento in eventos:
            evento['deputado_id'] = deputado_id
        return eventos

    async def extract(
        self,
        deputados: json,
        init_legislatura: int = None,
        items: int = 50,
        request_tries: int = 4
    ):
        async with aiohttp.ClientSession() as session:
            legislatura_params = {'id': init_legislatura} if init_legislatura is not None else {}
            legislatura = await self.client.get(session, 'legislaturas', params=legislatura_params)
            start_legislatura_date = legislatura['dados'][0].get('dataInicio', None)

            deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
            all_eventos = []

            tasks = [self._fetch_all_eventos_for_deputado(session, deputado_id, start_legislatura_date, items)
                     for deputado_id in deputados_ids]
            results, coverage, _errors = await gather_aligned(tasks, label='deputados/eventos')

            for eventos in results:
                all_eventos.extend(eventos)

            self.partial = coverage < 0.99
        return all_eventos
