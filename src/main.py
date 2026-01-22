from clients.camara_client import AsyncCamaraClient
from extractors.camara.legislaturas.legislaturas import AsyncLegislaturaExtractor
from extractors.camara.legislaturas.lideres import AsyncLegislaturaLideresExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    legislaturas = AsyncLegislaturaExtractor(client)
    start = datetime.now()

    legislaturas_data = await legislaturas.extract(init_legislatura=56)

    end = datetime.now()
    # print(legislaturas_data)
    print(f'Time elapsed: {end - start}')

    lideres = AsyncLegislaturaLideresExtractor(client)
    lideres_data = await lideres.extract(legislaturas=legislaturas_data)
    print(f'Total lideres: {len(lideres_data)}')

if __name__ == '__main__':
    asyncio.run(main())