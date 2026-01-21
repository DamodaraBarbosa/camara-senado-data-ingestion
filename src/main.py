from clients.camara_client import AsyncCamaraClient
from extractors.camara.orgaos.orgaos import AsyncOrgaosExtractor
from extractors.camara.orgaos.eventos import AsyncEventosExtractor
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

    eventos = AsyncEventosExtractor(client)
    start = datetime.now()

    eventos_data = await eventos.extract(orgaos=orgaos_data[:20], init_legislatura=56)
    end = datetime.now()

    print(f'Total eventos extracted: {eventos_data}')
    print(f'Time elapsed: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())