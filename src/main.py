from clients.camara_client import AsyncCamaraClient
from extractors.camara.eventos.eventos import AsyncEventosExtractor
from extractors.camara.eventos.ids import AsyncEventosIdsExtractor
from extractors.camara.eventos.deputados import AsyncEventosDeputadosExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    eventos = AsyncEventosExtractor(client)

    start = datetime.now()
    eventos_data = await eventos.extract(init_legislatura=56)

    ids = AsyncEventosIdsExtractor(client)
    eventos_ids = await ids.extract(eventos_data)

    eventos_deputados = AsyncEventosDeputadosExtractor(client)
    eventos_deputados_data = await eventos_deputados.extract(eventos_ids)

    end = datetime.now()

    print(f'Eventos: {eventos_deputados_data}')
    print(f'Número de eventos: {len(eventos_deputados_data)}')
    print(f'Tempo gasto: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())