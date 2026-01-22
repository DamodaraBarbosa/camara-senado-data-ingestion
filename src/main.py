from clients.camara_client import AsyncCamaraClient
from extractors.camara.partidos.partidos import AsyncPartidosExtractor
from extractors.camara.partidos.membros import AsyncPartidosMembrosExtractor
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

    membros = AsyncPartidosMembrosExtractor(client)
    start = datetime.now()

    membros_data = await membros.extract(partidos=partidos_data, init_legislatura=56)
    # ids = AsyncPartidosIdsExtractor(client)
    # ids_data = await ids.extract(partidos=partidos_data)
    
    end = datetime.now()
    print(f'Total of membros: {membros_data}')
    print(f'Time elapsed: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())