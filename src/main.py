from clients.camara_client import AsyncCamaraClient
from extractors.camara.votacoes.votacoes import AsyncVotacoesExtractor
from extractors.camara.votacoes.orientacoes import AsyncVotacoesOrientacoes
from extractors.camara.votacoes.votos import AsyncVotosExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    votacoes = AsyncVotacoesExtractor(client)
    start = datetime.now()

    votacoes_data = await votacoes.extract(init_legislatura=57)

    end = datetime.now()
    print(f'Total votacoes extracted: {len(votacoes_data)}')
    print(f'Time elapsed: {end - start}')

    # votacoes_ids = AsyncVotacoesIdsExtractor(client)
    # start = datetime.now()

    # votacoes_ids_data = await votacoes_ids.extract(votacoes_data)
    # end = datetime.now()

    # print(f'Total votacoes ids extracted: {len(votacoes_ids_data)}')
    # print(f'Time elapsed: {end - start}')

    orientacoes = AsyncVotacoesOrientacoes(client)
    start = datetime.now()

    orientacoes_data = await orientacoes.extract(votacoes_data[:100])
    end = datetime.now()

    print(orientacoes_data)
    print(f'Total orientacoes extracted: {len(orientacoes_data)}')
    print(f'Time elapsed: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())
