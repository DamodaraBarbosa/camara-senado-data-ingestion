from clients.camara_client import AsyncCamaraClient
from extractors.camara.partidos.partidos import AsyncPartidosExtractor
from extractors.camara.partidos.lideres import AsyncLideresExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    partidos = AsyncPartidosExtractor(client)
    start = datetime.now()

    partidos_data = await partidos.extract(init_legislatura=56)

    end = datetime.now()
    # print(f'Partidos extracted: {partidos_data}')
    print(f'Time elapsed: {end - start}')

    lideres = AsyncLideresExtractor(client)
    start = datetime.now()

    lideres_data = await lideres.extract(partidos=partidos_data)
    # ids = AsyncPartidosIdsExtractor(client)
    # ids_data = await ids.extract(partidos=partidos_data)
    
    end = datetime.now()
    print(f'Total of lideres: {lideres_data}')
    print(f'Time elapsed: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())