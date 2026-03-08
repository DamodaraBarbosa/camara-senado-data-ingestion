from extractors.camara.base import CamaraBaseExtractor
from datetime import datetime
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
        session = aiohttp.ClientSession()
        legislatura = (await self.client.get(session, self.LEGISLATURAS))['dados']
        current_legislatura = max(legislatura['id'] for legislatura in legislatura)
        start_legislatura = init_legislatura if init_legislatura is not None else current_legislatura

        deputados_ids = list(dict.fromkeys(deputado.get('id') for deputado in deputados if deputado.get('id')))
        all_discursos = []

        for legislatura in range(start_legislatura, current_legislatura + 1):
            print(f'Legislatura: {legislatura}')
            for  deputado_id in deputados_ids:
                page = 1
                empty_count = 0

                while empty_count < request_tries:
                    try:
                        params = {
                            'idLegislatura': legislatura,
                            'itens': items,
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

                        for discurso in data:
                            discurso['deputado_id'] = deputado_id
                        
                        all_discursos.extend(data)
                        page += 1

                    except Exception as e:
                        print(f'Error while extracting discursos for deputado {deputado_id}: {e}')

        await session.close()
        return all_discursos