from clients.camara_client import AsyncCamaraClient
from extractors.camara.base import CamaraBaseExtractor
from extractors.camara.deputados.deputados import DeputadosExtractor
from extractors.camara.votacoes.votacoes import AsyncVotacoesExtractor
import asyncio
from datetime import datetime

if __name__ == "__main__":
    client = AsyncCamaraClient()
    # deputados = DeputadosExtractor(client)
    # data = deputados.extract(init_legislatura=56)
    # print(f'Total deputados extracted: {len(data)}')
    # ids = IdsExtractor(client)
    # ids_data = ids.extract(deputados=data)

    votacoes = AsyncVotacoesExtractor(client)
    start = datetime.now()
    votacoes_data = asyncio.run(votacoes.extract(init_legislatura=57))
    end = datetime.now()
    print(f'Total votacoes extracted: {len(votacoes_data)}')
    print(f'Time elapsed: {end - start}')

    
