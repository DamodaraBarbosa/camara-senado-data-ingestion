from clients.camara_client import AsyncCamaraClient
from extractors.camara.grupos.grupos import AsyncGruposExtractor
from extractors.camara.grupos.ids import AsyncGruposIdsExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    grupos = AsyncGruposExtractor(client)

    start = datetime.now()
    grupos_data = await grupos.extract(init_legislatura=56)

    grupos_ids_extractor = AsyncGruposIdsExtractor(client)
    grupos_ids_data = await grupos_ids_extractor.extract(grupos_data)

    end = datetime.now()

    print(f'Grupos IDs: {grupos_ids_data}')
    print(f'Tempo gasto: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())