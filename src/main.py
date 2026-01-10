from clients.camara_client import CamaraClient
# from extractors.camara.base import CamaraBaseExtractor
from extractors.camara.deputados.deputados import DeputadosExtractor
from extractors.camara.deputados.ids import IdsExtractor
from extractors.camara.deputados.despesas import DespesasExtractor

if __name__ == "__main__":
    client = CamaraClient()
    deputados = DeputadosExtractor(client)
    data = deputados.extract(init_legislatura=57)[:4]

    ids = IdsExtractor(client)
    ids_data = ids.extract(deputados=data)
    
    despesas = DespesasExtractor(client)
    despesas_data = despesas.extract(deputados=data, init_year=2025)
    print(f'Total despesas extracted: {despesas_data}')
