from clients.camara_client import AsyncCamaraClient
from extractors.camara.blocos.blocos import BlocosExtractor
import asyncio
from datetime import datetime

async def main():
    client = AsyncCamaraClient()
    blocos = BlocosExtractor(client)
    start = datetime.now()

    blocos_data = await blocos.extract(init_legislatura=56)

    end = datetime.now()
    print(len(blocos_data))

if __name__ == '__main__':
    asyncio.run(main())