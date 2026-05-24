from extractors.camara.base import CamaraBaseExtractor
import json
import asyncio
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
        session = aiohttp.ClientSession()
        legislatura = (await self.client.get(session, self.LEGISLATURAS))['dados']
        current_legislatura = max(legislatura['id'] for legislatura in legislatura)
        start_legislatura = init_legislatura if init_legislatura is not None else current_legislatura
        legislaturas_range = list(range(start_legislatura, current_legislatura + 1))

        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))

        print(f'[discursos] Iniciando extração | {len(deputados_ids)} deputados | legislaturas: {legislaturas_range}')

        tasks = [
            self._fetch_deputado(session, deputado_id, legislatura_id, items, request_tries)
            for deputado_id in deputados_ids
            for legislatura_id in legislaturas_range
        ]
        results = await asyncio.gather(*tasks)

        all_discursos = [discurso for discursos in results for discurso in discursos]
        print(f'[discursos] Extração concluída | total de discursos: {len(all_discursos)}')

        await session.close()
        return all_discursos

    async def _fetch_deputado(self, session, deputado_id, legislatura_id, items, request_tries):
        discursos = []
        page = 1
        empty_count = 0

        print(f'[discursos] Buscando deputado {deputado_id}...')

        while empty_count < request_tries:
            try:
                params = {
                    'idLegislatura': legislatura_id,
                    'pagina': page,
                    'itens': items
                }

                response = await self.client.get(session, self.ENDPOINT.format(id=deputado_id), params=params)
                data = response.get('dados', [])

                if not data:
                    empty_count += 1
                    page += 1
                    continue

                for discurso in data:
                    discurso['deputado_id'] = deputado_id

                discursos.extend(data)
                print(f'[discursos] Deputado {deputado_id} | legislatura {legislatura_id} | página {page} | +{len(data)} discursos')
                page += 1

            except Exception as e:
                print(f'Error while extracting discursos for deputado {deputado_id}: {e}')
                break

        print(f'[discursos] Concluído deputado {deputado_id} | total de discursos: {len(discursos)}')
        return discursos                          
