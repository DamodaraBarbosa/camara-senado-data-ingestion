from clients.camara_client import CamaraClient
# from extractors.camara.base import CamaraBaseExtractor
from extractors.camara.deputados.deputados import DeputadosExtractor
from extractors.camara.deputados.discursos import DiscursosExtractor
from extractors.camara.deputados.ids import IdsExtractor
from extractors.camara.deputados.despesas import DespesasExtractor
from extractors.camara.deputados.eventos import EventosExtractor

if __name__ == "__main__":
    client = CamaraClient()
    deputados = DeputadosExtractor(client)
    data = deputados.extract(init_legislatura=56)[:30]
    print(f'Total deputados extracted: {len(data)}')
    # ids = IdsExtractor(client)
    # ids_data = ids.extract(deputados=data)
    
    # despesas = DespesasExtractor(client)
    # despesas_data = despesas.extract(deputados=data, init_year=2025)
    # print(f'Total despesas extracted: {despesas_data}')
    # discursos = DiscursosExtractor(client)
    # discursos_data = discursos.extract(deputados=data, init_legislatura=57)
    # print(f'Total discursos extracted: {len(discursos_data)}')

    eventos = EventosExtractor(client)
    eventos_data = eventos.extract(deputados=data, init_legislatura=56)
    print(eventos_data)