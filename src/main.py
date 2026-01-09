from clients.camara_client import CamaraClient
from extractors.camara.base import CamaraBaseExtractor
from src.extractors.camara.deputados.deputados import DeputadosExtractor

if __name__ == "__main__":
    client = CamaraClient()
    extractor = DeputadosExtractor(client)
    data = extractor.extract(init_legislatura=50)

    print(data)
