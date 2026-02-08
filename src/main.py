from clients.camara_client import AsyncCamaraClient
from extractors.camara.blocos.blocos import BlocosExtractor
from extractors.camara.blocos.ids import AsyncBlocosIdsExtractor
from extractors.camara.blocos.partidos import AsyncBlocosPartidosExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    blocos = BlocosExtractor(client)
    start = datetime.now()

    blocos_data = await blocos.extract(init_legislatura=56)
    blocos_ids_extractor = AsyncBlocosIdsExtractor(client)
    blocos_ids_data = await blocos_ids_extractor.extract(blocos_data)

    blocos_partidos_extractor = AsyncBlocosPartidosExtractor(client)
    blocos_partidos_data = await blocos_partidos_extractor.extract(blocos_ids_data)

    end = datetime.now()

    print(f'Tempo gasto: {end - start}')
    print(f'Len ids: {len(blocos_ids_data)}')
    print(f'Partidos: {blocos_partidos_data}')

if __name__ == '__main__':
    asyncio.run(main())