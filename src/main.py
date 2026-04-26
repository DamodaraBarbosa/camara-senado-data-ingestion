from clients.camara_client import AsyncCamaraClient
from extractors.camara.deputados.deputados import AsyncDeputadosExtractor
from extractors.camara.deputados.discursos import AsyncDiscursosExtractor
from extractors.camara.deputados.orgaos import AsyncOrgaosExtractor
import asyncio
from datetime import datetime


async def main():
    client = AsyncCamaraClient()
    deputados = AsyncDeputadosExtractor(client)

    start = datetime.now()
    deputados_data = await deputados.extract(init_legislatura=56)

    orgao = AsyncOrgaosExtractor(client)
    orgaos_data = await orgao.extract(deputados_data, init_legislatura=56)

    end = datetime.now()

    print(f'Número de orgãos: {len(orgaos_data)}')
    print(f'Tempo gasto: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())
