from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
import aiohttp


class AsyncEventosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados/{id}/eventos'

    async def _fetch_all_eventos_for_deputado(self, session, deputado_id, start_legislatura_date, items, request_tries):
        eventos = []
        page = 1
        empty_count = 0

        while empty_count < request_tries:
            try:
                params = {
                    'itens': items,
                    'dataInicio': start_legislatura_date,
                    'pagina': page
                }

                params = {k: v for k, v in params.items() if v is not None}

                response = await self.client.get(session, self.ENDPOINT.format(id=deputado_id), params=params)
                data = response.get('dados', [])

                if not data:
                    empty_count += 1
                    page += 1
                    continue

                empty_count = 0

                for evento in data:
                    evento['deputado_id'] = deputado_id

                eventos.extend(data)
                page += 1

            except Exception as e:
                print(f'Error while extracting eventos for deputado {deputado_id}: {e}')
                empty_count += 1
                page += 1

        return eventos

    async def extract(
        self,
        deputados: json,
        init_legislatura: int = None,
        items: int = 50,
        request_tries: int = 4
    ):
        session = aiohttp.ClientSession()
        legislatura_params = {'id': init_legislatura} if init_legislatura is not None else {}
        legislatura = await self.client.get(session, 'legislaturas', params=legislatura_params)
        start_legislatura_date = legislatura['dados'][0].get('dataInicio', None)

        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_eventos = []

        tasks = [self._fetch_all_eventos_for_deputado(session, deputado_id, start_legislatura_date, items, request_tries)
                 for deputado_id in deputados_ids]
        results = await asyncio.gather(*tasks)

        for eventos in results:
            all_eventos.extend(eventos)

        await session.close()
        return all_eventos
