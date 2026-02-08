from clients.camara_client import AsyncCamaraClient
from extractors.camara.frentes.frentes import AsyncFrentesExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    frentes = AsyncFrentesExtractor(client)

    start = datetime.now()
    frentes_data = await frentes.extract()

    end = datetime.now()

    print(f'Tempo gasto: {end - start}')
    print(f'Len frentes: {len(frentes_data)}')
    print(f'Frentes: {frentes_data}')

if __name__ == '__main__':
    asyncio.run(main())