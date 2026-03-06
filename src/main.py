from clients.camara_client import AsyncCamaraClient
from extractors.camara.grupos.grupos import AsyncGruposExtractor
from extractors.camara.grupos.ids import AsyncGruposIdsExtractor
from extractors.camara.grupos.historico import AsyncGruposHistoricoExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    grupos = AsyncGruposExtractor(client)

    start = datetime.now()
    grupos_data = await grupos.extract(init_legislatura=56)

    grupos_ids_extractor = AsyncGruposIdsExtractor(client)
    grupos_ids_data = await grupos_ids_extractor.extract(grupos_data)
    grupos_historico_extractor = AsyncGruposHistoricoExtractor(client)
    grupos_historico_data = await grupos_historico_extractor.extract(grupos_data)
    end = datetime.now()

    print(f'Grupos IDs: {grupos_historico_data}')
    print(f'Número de histórico de grupos: {len(grupos_historico_data)}')
    print(f'Tempo gasto: {end - start}')

if __name__ == '__main__':
    asyncio.run(main())