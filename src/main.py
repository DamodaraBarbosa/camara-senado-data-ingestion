from clients.camara_client import AsyncCamaraClient
from extractors.camara.orgaos.orgaos import AsyncOrgaosExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    orgaos = AsyncOrgaosExtractor(client)
    start = datetime.now()

    orgaos_data = await orgaos.extract(init_legislatura=56)

    end = datetime.now()
    print(f'Total votacoes extracted: {len(orgaos_data)}')
    print(f'Time elapsed: {end - start}')

    # votacoes_ids = AsyncVotacoesIdsExtractor(client)
    # start = datetime.now()

    # votacoes_ids_data = await votacoes_ids.extract(votacoes_data)
    # end = datetime.now()

    # print(f'Total votacoes ids extracted: {len(votacoes_ids_data)}')
    # print(f'Time elapsed: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())