from clients.camara_client import AsyncCamaraClient
from extractors.camara.legislaturas.legislaturas import AsyncLegislaturaExtractor
from extractors.camara.legislaturas.mesa import AsyncMesaExtractor
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

    mesa = AsyncMesaExtractor(client)
    mesa_data = await mesa.extract(legislaturas=legislaturas_data)
    print(f'Total mesas: {len(mesa_data)}')
    # print(f'Dados mesa: {mesa_data}')

if __name__ == '__main__':
    asyncio.run(main())