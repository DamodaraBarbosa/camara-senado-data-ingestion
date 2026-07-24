from extractors.camara.base import CamaraBaseExtractor
import aiohttp
from typing import Optional


class AsyncDeputadosExtractor(CamaraBaseExtractor):
    ENDPOINT = 'deputados'
    LEGISLATURAS = 'legislaturas'

    async def extract(
        self,
        init_legislatura: Optional[int] = None,
        sigla_partido: str = None,
        sigla_sexo: str = None,
        sigla_uf: str = None,
        order_by: str = None,
        order: str = None,
        items: int = 1000,
        request_tries: int = 4
    ):
        async with aiohttp.ClientSession() as session:
            legislaturas = (await self.client.get(session, self.LEGISLATURAS))['dados']
            current_legislatura = max(legislatura['id'] for legislatura in legislaturas)

            start = init_legislatura if init_legislatura is not None else current_legislatura
            all_deputados = []

            for legislatura in range(start, current_legislatura + 1):
                params = {
                    'idLegislatura': legislatura,
                    'siglaUf': sigla_uf,
                    'siglaPartido': sigla_partido,
                    'siglaSexo': sigla_sexo,
                    'ordernarPor': order_by,
                    'ordem': order,
                }
                params = {k: v for k, v in params.items() if v is not None}

                data = await self.client.get_all_pages(session, self.ENDPOINT, params=params, itens=items)
                all_deputados.extend(data)

            return all_deputados
