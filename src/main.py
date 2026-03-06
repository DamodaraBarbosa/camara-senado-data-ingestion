from clients.camara_client import AsyncCamaraClient
from extractors.camara.grupos.grupos import AsyncGruposExtractor
from extractors.camara.grupos.ids import AsyncGruposIdsExtractor
from extractors.camara.grupos.historico import AsyncGruposHistoricoExtractor
from extractors.camara.grupos.membros import AsyncGruposMembrosExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    grupos = AsyncGruposExtractor(client)

    start = datetime.now()
    grupos_data = await grupos.extract(init_legislatura=56)

    grupos_ids_extractor = AsyncGruposIdsExtractor(client)
    grupos_ids_data = await grupos_ids_extractor.extract(grupos_data)
    grupos_membros_extractor = AsyncGruposMembrosExtractor(client)
    grupos_membros_data = await grupos_membros_extractor.extract(grupos_data)

    end = datetime.now()

    print(f'Grupos IDs: {grupos_membros_data}')
    print(f'Número de histórico de grupos: {len(grupos_membros_data)}')
    print(f'Tempo gasto: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())