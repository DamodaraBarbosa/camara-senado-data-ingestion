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
from extractors.camara.proposicoes.proposicoes import ProposicoesExtractor
from extractors.camara.proposicoes.ids import IdsExtractor
from extractors.camara.proposicoes.autores import AutoresExtractor
from extractors.camara.proposicoes.relacionadas import RelacionadasExtractor
from extractors.camara.proposicoes.temas import TemasExtractor
from extractors.camara.proposicoes.tramitacoes import TramitacoesExtractor
from extractors.camara.proposicoes.votacoes import VotacoesExtractor
from extractors.camara.proposicoes.codigo_tema import CodigoTemaExtractor
from extractors.camara.proposicoes.codigo_tipo_tramitacao import CodigoTipoTramitacaoExtractor
from extractors.camara.proposicoes.sigla_tipo import SiglaTipoExtractor
from extractors.camara.proposicoes.situacoes_proposicao import SituacoesProposicaoExtractor
from extractors.camara.proposicoes.tipos_autor import TiposAutorExtractor
from extractors.camara.proposicoes.tipos_proposicao import TiposProposicaoExtractor
from extractors.camara.proposicoes.tipos_tramitacao import TiposTramitacaoExtractor

if __name__ == "__main__":
    client = CamaraClient()
    # deputados = DeputadosExtractor(client)
    # data = deputados.extract(init_legislatura=56)
    # print(f'Total deputados extracted: {len(data)}')
    # ids = IdsExtractor(client)
    # ids_data = ids.extract(deputados=data)
    
    # despesas = DespesasExtractor(client)
    # despesas_data = despesas.extract(deputados=data, init_legislatura=56)
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

    # proposicoes = ProposicoesExtractor(client)
    # proposicoes_data = proposicoes.extract(init_legislatura=56, autor='Kim Kataguiri')
    # print(f'Total proposicoes extracted: {len(proposicoes_data)}')

    # proposicoes_ids = IdsExtractor(client)
    # proposicoes_ids_data = proposicoes_ids.extract(proposicoes=proposicoes_data)
    # print(f'Total proposicoes ids extracted: {proposicoes_ids_data}')

    # autores = AutoresExtractor(client)
    # autores_data = autores.extract(proposicoes=proposicoes_data)
    # print(f'Total autores extracted: {autores_data}')

    # relacionadas = RelacionadasExtractor(client)
    # relacionadas_data = relacionadas.extract(proposicoes=proposicoes_data)
    # print(f'Total relacionadas extracted: {relacionadas_data}')

    # temas = TemasExtractor(client)
    # temas_data = temas.extract(proposicoes=proposicoes_data)
    # print(f'Total temas extracted: {len(temas_data)}')

    # tramitacoes = TramitacoesExtractor(client)
    # tramitacoes_data = tramitacoes.extract(proposicoes=proposicoes_data)
    # print(tramitacoes_data)

    # votacoes = VotacoesExtractor(client)
    # votacoes_data = votacoes.extract(proposicoes=proposicoes_data)
    # print(f'Total votacoes extracted: {len(votacoes_data)}')

    # codigo_tema = CodigoTemaExtractor(client)
    # codigo_tema_data = codigo_tema.extract()
    # print(codigo_tema_data)

    # codigo_tipo_tramitacao = CodigoTipoTramitacaoExtractor(client)
    # codigo_tipo_tramitacao_data = codigo_tipo_tramitacao.extract()
    # print(codigo_tipo_tramitacao_data)

    # sigla_tipo = SiglaTipoExtractor(client)
    # sigla_tipo_data = sigla_tipo.extract()
    # print(sigla_tipo_data)

    # situacoes_proposicao = SituacoesProposicaoExtractor(client)
    # situacoes_proposicao_data = situacoes_proposicao.extract()
    # print(situacoes_proposicao_data)

    # tipos_autor = TiposAutorExtractor(client)
    # tipos_autor_data = tipos_autor.extract()
    # print(tipos_autor_data)

    # tipos_proposicao = TiposProposicaoExtractor(client)
    # tipos_proposicao_data = tipos_proposicao.extract()
    # print(tipos_proposicao_data)

    tipos_tramitacao = TiposTramitacaoExtractor(client)
    tipos_tramitacao_data = tipos_tramitacao.extract()
    print(tipos_tramitacao_data)