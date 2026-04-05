from clients.camara_client import AsyncCamaraClient
from extractors.camara.deputados.deputados import AsyncDeputadosExtractor
from extractors.camara.deputados.despesas import AsyncDespesasExtractor
import asyncio
from datetime import datetime


async def main():
    client = AsyncCamaraClient()
    deputados = AsyncDeputadosExtractor(client)

    start = datetime.now()
    deputados_data = await deputados.extract(init_legislatura=56)

    despesas = AsyncDespesasExtractor(client)
    despesas_data = await despesas.extract(deputados_data, init_legislatura=56)

    end = datetime.now()

    print(f'Despesas: {despesas_data}')
    print(f'Número de despesas: {len(despesas_data)}')
    print(f'Tempo gasto: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())
