from extractors.camara.base import CamaraBaseExtractor
import asyncio
import aiohttp

class BlocosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'blocos'
    LEGISLATURAS = 'legislaturas'

    async def _fetch_pages(
            self,
            session,
            id_legislatura,
            request_tries,
            itens  
        ):
        extracted_data = []
        page = 1
        empty_count = 0
        
        while empty_count < request_tries:
            try:
                current_params = {
                    'itens': itens,
                    'pagina': page
                }

                current_params = {k: v for k, v in current_params.items() if v is not None}

                response = await self.client.get(session, self.ENDPOINT, params=current_params)
                data = response.get('dados', [])

                if not data:
                    empty_count += 1
                    page += 1
                    continue

                for bloco in data:
                    bloco['idLegislatura'] = id_legislatura
                    extracted_data.append(bloco)

                print(f'Fetched page {page} for legislatura {id_legislatura}. Total items: {extracted_data}')
                page += 1
                empty_count = 0

            except Exception as e:
                print(f"Error fetching data for legislatura {id_legislatura}, page {page}: {e}")
                empty_count += 1
        
        return extracted_data
    
    async def extract(
            self,
            init_legislatura: int = None,
            itens: int = 100,
            request_tries: int = 4
        ):
        all_blocos = []

        async with aiohttp.ClientSession() as session:
            legislaturas = await self.client.get(session, self.LEGISLATURAS)
            legislaturas_data = legislaturas.get('dados', [])

            legislaturas_ids = [legislatura.get('id') for legislatura in legislaturas_data if legislatura.get('id')]
            current_legislatura = max(legislaturas_ids)
            start = init_legislatura if init_legislatura is not None else current_legislatura   

            for id_legislatura in range(start, current_legislatura + 1):
                print(f'Fetching data for legislatura {id_legislatura}')
                blocos_data = await self._fetch_pages(session, id_legislatura, request_tries, itens)
                all_blocos.extend(blocos_data)
        
        return all_blocos
