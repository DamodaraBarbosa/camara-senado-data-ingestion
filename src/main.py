from clients.camara_client import CamaraClient
from extractors.camara.base import CamaraBaseExtractor
from extractors.camara.deputados.deputados import DeputadosExtractor
from extractors.camara.deputados.discursos import DiscursosExtractor
from extractors.camara.deputados.ids import IdsExtractor
from extractors.camara.deputados.despesas import DespesasExtractor
from extractors.camara.deputados.eventos import EventosExtractor
from extractors.camara.deputados.frentes import FrentesExtractor
from extractors.camara.deputados.historico import HistoricoExtractor
from extractors.camara.deputados.mandatos_externos import MandatosExternosExtractor
from extractors.camara.deputados.ocupacoes import OcupacoesExtractor
from extractors.camara.deputados.orgaos import OrgaosExtractor
from extractors.camara.deputados.profissoes import ProfissoesExtractor
from extractors.camara.deputados.lideres import LideresExtractor
from extractors.camara.deputados.mesa import MesaExtractor
from extractors.camara.deputados.codigo_situacao import CodigoSituacaoExtractor
# 
if __name__ == "__main__":
    client = CamaraClient()
    deputados = DeputadosExtractor(client)
    data = deputados.extract(init_legislatura=56)
    print(f'Total deputados extracted: {len(data)}')
    # ids = IdsExtractor(client)
    # ids_data = ids.extract(deputados=data)
    
    # despesas = DespesasExtractor(client)
    # despesas_data = despesas.extract(deputados=data, init_year=2025)
    # print(f'Total despesas extracted: {despesas_data}')
    # discursos = DiscursosExtractor(client)
    # discursos_data = discursos.extract(deputados=data, init_legislatura=57)
    # print(f'Total discursos extracted: {len(discursos_data)}')

    # eventos = EventosExtractor(client)
    # eventos_data = eventos.extract(deputados=data, init_legislatura=56)
    # print(eventos_data)

    # frentes = FrentesExtractor(client)
    # frentes_data = frentes.extract(deputados=data)
    # print(frentes_data)

    # historico = HistoricoExtractor(client)
    # historico_data = historico.extract(deputados=data)
    # print(historico_data)

    # mandatos_externos = MandatosExternosExtractor(client)
    # mandatos_externos_data = mandatos_externos.extract(deputados=data)
    # print(mandatos_externos_data)

    # ocupacoes = OcupacoesExtractor(client)
    # ocupacoes_data = ocupacoes.extract(deputados=data)
    # print(ocupacoes_data)

    # orgaos = OrgaosExtractor(client)
    # orgaos_data = orgaos.extract(deputados=data, init_legislatura=57)
    # print(f'Total orgaos extracted: {orgaos_data}')

    # profissoes = ProfissoesExtractor(client)
    # profissoes_data = profissoes.extract(deputados=data)
    # print(f'Total profissoes extracted: {profissoes_data}')

    # lideres = LideresExtractor(client)
    # lideres_data = lideres.extract(init_legislatura=56)
    # print(lideres_data)

    # mesas = MesaExtractor(client)
    # mesas_data = mesas.extract(init_legislatura=56)
    # print(mesas_data)

    # codigo_situacao = CodigoSituacaoExtractor(client)
    # codigo_situacao_data = codigo_situacao.extract()
    # print(codigo_situacao_data)

    # situacoes_deputados = SituacoesDeputadosExtractor(client)
    # situacoes_deputados_data = situacoes_deputados.extract()
    # print(situacoes_deputados_data)