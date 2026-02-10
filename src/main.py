from clients.camara_client import AsyncCamaraClient
from extractors.camara.frentes.frentes import AsyncFrentesExtractor
from extractors.camara.frentes.membros import AsyncFrentesMembrosExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    frentes = AsyncFrentesExtractor(client)

    start = datetime.now()
    frentes_data = await frentes.extract(init_legislatura=56)
    frentes_membros = AsyncFrentesMembrosExtractor(client)
    frentes_membros_data = await frentes_membros.extract(frentes_data)

    end = datetime.now()

    print(f'Tempo gasto: {end - start}')
    print(f'Len frentes: {len(frentes_membros_data)}')
    print(f'Frentes: {frentes_membros_data}')

if __name__ == '__main__':
    asyncio.run(main())