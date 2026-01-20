from clients.camara_client import AsyncCamaraClient
from extractors.camara.orgaos.orgaos import AsyncOrgaosExtractor
from extractors.camara.orgaos.ids import AsyncOrgaosIdsExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    orgaos = AsyncOrgaosExtractor(client)
    start = datetime.now()

    orgaos_data = await orgaos.extract(init_legislatura=56)

    end = datetime.now()
    print(f'Total orgaos extracted: {len(orgaos_data)}')
    print(f'Time elapsed: {end - start}')

    orgaos_ids = AsyncOrgaosIdsExtractor(client)
    start = datetime.now()

    orgaos_ids_data = await orgaos_ids.extract(orgaos_data)
    end = datetime.now()

    print(f'Total orgaos ids extracted: {len(orgaos_ids_data)}')
    print(f'Time elapsed: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())