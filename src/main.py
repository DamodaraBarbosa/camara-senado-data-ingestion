from clients.camara_client import AsyncCamaraClient
from extractors.camara.eventos.eventos import AsyncEventosExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    eventos = AsyncEventosExtractor(client)

    start = datetime.now()
    eventos_data = await eventos.extract(init_legislatura=56)

    end = datetime.now()

    print(f'Eventos: {eventos_data}')
    print(f'Número de eventos: {len(eventos_data)}')
    print(f'Tempo gasto: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())