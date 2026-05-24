from extractors.camara.base import CamaraBaseExtractor
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
        session = aiohttp.ClientSession()
        legislatura = await self.client.get(session, 'legislaturas', params={'id': init_legislatura})
        start_legislatura_date = legislatura['dados'][0].get('dataInicio', None)

        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))

        print(f'[orgaos] Iniciando extração para {len(deputados_ids)} deputados | data início legislatura: {start_legislatura_date}')
        
        tasks = [
            self._fetch_deputado(session, deputado_id, start_legislatura_date, items, request_tries)
            for deputado_id in deputados_ids
        ]
        results = await asyncio.gather(*tasks)

        all_bodies = [body for bodies in results for body in bodies]
        print(f'[orgaos] Extração concluída | total de orgãos: {len(all_bodies)}')

        await session.close()
        return all_bodies

    async def _fetch_deputado(self, session, deputado_id, start_legislatura_date, items, request_tries):
        bodies = []
        page = 1
        empty_count = 0

        while empty_count < request_tries:
            try:
                params = {
                    'dataInicio': start_legislatura_date,
                    'pagina': page,
                    'itens': items
                }
                params = {k: v for k, v in params.items() if v is not None}

                response = await self.client.get(session, self.ENDPOINT.format(id=deputado_id), params=params)
                data = response.get('dados', [])

                if not data:
                    empty_count += 1
                else:
                    bodies.extend(data)
                    page += 1

            except Exception as e:
                print(f'Error fetching deputado {deputado_id}: {e}')
                empty_count += 1

        return bodies
