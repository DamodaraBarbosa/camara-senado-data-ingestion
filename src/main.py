from clients.camara_client import AsyncCamaraClient
from extractors.camara.frentes.frentes import AsyncFrentesExtractor
from extractors.camara.frentes.ids import AsyncFrentesIdsExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    frentes = AsyncFrentesExtractor(client)

    start = datetime.now()
    frentes_data = await frentes.extract()
    frentes_ids_extractor = AsyncFrentesIdsExtractor(client)
    frentes_ids_data = await frentes_ids_extractor.extract(frentes_data)

    end = datetime.now()

    print(f'Tempo gasto: {end - start}')
    print(f'Len frentes: {len(frentes_ids_data)}')
    print(f'Frentes: {frentes_ids_data}')

if __name__ == '__main__':
    asyncio.run(main())