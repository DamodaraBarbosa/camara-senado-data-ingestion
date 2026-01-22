from clients.camara_client import AsyncCamaraClient
from extractors.camara.partidos.partidos import AsyncPartidosExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    partidos = AsyncPartidosExtractor(client)
    start = datetime.now()

    partidos_data = await partidos.extract(init_legislatura=56)

    end = datetime.now()
    print(f'Partidos extracted: {partidos_data}')
    print(f'Time elapsed: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())