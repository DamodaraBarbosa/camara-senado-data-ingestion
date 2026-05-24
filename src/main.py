from clients.camara_client import AsyncCamaraClient
from extractors.camara.proposicoes.proposicoes import AsyncProposicoesExtractor
import asyncio
from datetime import datetime


async def main():
    client = AsyncCamaraClient()
    proposicoes = AsyncProposicoesExtractor(client)

    start = datetime.now()
    proposicoes_data = await proposicoes.extract(init_legislatura=56)

    end = datetime.now()

    print(f'Número de proposições: {len(proposicoes_data)}')
    print(f'Tempo gasto: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())
