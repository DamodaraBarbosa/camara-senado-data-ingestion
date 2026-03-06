from clients.camara_client import AsyncCamaraClient
from extractors.camara.grupos.grupos import AsyncGruposExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    grupos = AsyncGruposExtractor(client)

    start = datetime.now()
    grupos_data = await grupos.extract(init_legislatura=56)

    end = datetime.now()

    print(f'Tempo gasto: {end - start}')
    print(f'Len grupos: {len(grupos_data)}')
    print(f'Grupos: {grupos_data}')

if __name__ == '__main__':
    asyncio.run(main())