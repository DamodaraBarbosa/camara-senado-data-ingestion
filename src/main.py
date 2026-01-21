from clients.camara_client import AsyncCamaraClient
from extractors.camara.orgaos.orgaos import AsyncOrgaosExtractor
from extractors.camara.orgaos.codigo_situacao import CodigoSituacaoOrgaoExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    orgaos = AsyncOrgaosExtractor(client)
    start = datetime.now()

    orgaos_data = await orgaos.extract(init_legislatura=56)

    end = datetime.now()
    print(f'Total orgaos extracted: {len(orgaos_data)}')
    print(f'Time elapsed: {end - start}')

    codigo_situacao = CodigoSituacaoOrgaoExtractor(client)

    start = datetime.now()

    codigo_situacao_data = await codigo_situacao.extract()

    end = datetime.now()

    print(f'Total codigos situacao: {codigo_situacao_data}')
    print(f'Time elapsed: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())