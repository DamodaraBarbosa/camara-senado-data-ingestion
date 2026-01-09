from clients.camara_client import CamaraClient
# from extractors.camara.base import CamaraBaseExtractor
from extractors.camara.deputados.deputados import DeputadosExtractor
from extractors.camara.deputados.ids import IdsExtractor

if __name__ == "__main__":
    client = CamaraClient()
    deputados = DeputadosExtractor(client)
    data = deputados.extract(init_legislatura=56)

    ids = IdsExtractor(client)
    ids_data = ids.extract(deputados=data)
    print(ids_data)
