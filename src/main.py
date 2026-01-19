from clients.camara_client import CamaraClient
from extractors.camara.base import CamaraBaseExtractor
from extractors.camara.deputados.deputados import DeputadosExtractor
from extractors.camara.votacoes.votacoes import VotacoesExtractor

if __name__ == "__main__":
    client = CamaraClient()
    # deputados = DeputadosExtractor(client)
    # data = deputados.extract(init_legislatura=56)
    # print(f'Total deputados extracted: {len(data)}')
    # ids = IdsExtractor(client)
    # ids_data = ids.extract(deputados=data)

    votacoes = VotacoesExtractor(client)
    votacoes_data = votacoes.extract(init_legislatura=56)
    print(f'Total votacoes extracted: {len(votacoes_data)}')

    
