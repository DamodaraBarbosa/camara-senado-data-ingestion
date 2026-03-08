from extractors.camara.base import CamaraBaseExtractor
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
        all_orgaos = []

        for deputado_id in deputados_ids:
            page = 1
            empty_count = 0
            
            print(f'Extracting orgaos for deputado ID: {deputado_id}')
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

                    for orgao in data:
                        orgao['deputado_id'] = deputado_id
                    
                    all_orgaos.extend(data)
                    page += 1

                except Exception as e:
                    print(f'Error while extracting orgaos for deputado {deputado_id}: {e}')

        await session.close()
        return all_orgaos