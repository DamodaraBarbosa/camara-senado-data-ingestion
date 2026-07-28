from extractors.camara.base import CamaraBaseExtractor
import aiohttp

from utils.bulk import intern_str, nullify, to_fk, to_int
from utils.periods import legislatura_of_year, resolve_years

DATASET = "votacoes"


def votacao_from_row(row: dict, ano: int = None) -> dict:
    """Converte uma linha de ``votacoes-{ano}.csv`` no formato da API.

    O arquivo bulk é mais rico que ``GET /votacoes/{id}``: traz
    ``votosSim``/``votosNao``/``votosOutros``, que o endpoint de detalhe não
    devolve de forma alguma. Esses campos entram de forma aditiva.

    Atenção ao tipo de ``id``: é string (``"2458405-38"``), não inteiro.
    """
    return {
        "id": nullify(row.get("id")),  # string, não converter
        "uri": nullify(row.get("uri")),
        "data": nullify(row.get("data")),
        "dataHoraRegistro": nullify(row.get("dataHoraRegistro")),
        "idOrgao": to_int(row.get("idOrgao")),
        "uriOrgao": nullify(row.get("uriOrgao")),
        "siglaOrgao": intern_str(nullify(row.get("siglaOrgao"))),
        # `0` no CSV significa "sem evento"; a API devolve null.
        "idEvento": to_fk(row.get("idEvento")),
        "uriEvento": nullify(row.get("uriEvento")) if to_fk(row.get("idEvento")) else None,
        "aprovacao": to_int(row.get("aprovacao")),
        "descricao": nullify(row.get("descricao")),
        # Aditivos: ausentes no endpoint de detalhe.
        "votosSim": to_int(row.get("votosSim")),
        "votosNao": to_int(row.get("votosNao")),
        "votosOutros": to_int(row.get("votosOutros")),
        # A API expõe estes dois como campos planos, não aninhados — confirmado
        # contra o endpoint de detalhe pelo harness de paridade.
        "dataHoraUltimaAberturaVotacao": nullify(
            row.get("ultimaAberturaVotacao_dataHoraRegistro")
        ),
        "descUltimaAberturaVotacao": nullify(row.get("ultimaAberturaVotacao_descricao")),
        "ultimaApresentacaoProposicao": {
            "dataHoraRegistro": nullify(row.get("ultimaApresentacaoProposicao_dataHoraRegistro")),
            "descricao": nullify(row.get("ultimaApresentacaoProposicao_descricao")),
            # A API nomeia assim; o CSV usa `_uriProposicao`.
            "uriProposicaoCitada": nullify(row.get("ultimaApresentacaoProposicao_uriProposicao")),
        },
        "idLegislatura": legislatura_of_year(ano) if ano else None,
    }


class AsyncVotacoesExtractor(CamaraBaseExtractor):
    """Votações a partir dos arquivos bulk.

    Antes: varredura trimestral por ``votacoes?dataInicio=...``. Como
    ``dataInicio`` é um filtro aberto (``>=``), os 14 períodos re-liam
    largamente os mesmos dados — caro e propenso a 504. Pior, o guard de
    orçamento era código morto (``batch_size=50`` sobre 14 períodos gera uma
    única iteração), então a extração virava um ``gather`` ilimitado: 569s numa
    execução, 601s e falha total na seguinte.
    """

    async def extract(
        self,
        init_legislatura: int = None,
        id_proposicao: list = None,
        id_evento: list = None,
        id_orgao: list = None,
        itens: int = 100,            # mantidos por compatibilidade de assinatura
        request_tries: int = 4,
        batch_size: int = 50,
        anos: list = None,
        ano_inicio: int = None,
    ):
        self.partial = False

        async with aiohttp.ClientSession() as session:
            years = await resolve_years(
                self.client, session,
                init_legislatura=init_legislatura, anos=anos, ano_inicio=ano_inicio,
            )
        years = await self.bulk.available_partitions(DATASET, years)

        # Filtros que antes eram query params da API viram filtros em memória.
        orgaos = {str(o) for o in id_orgao} if id_orgao else None
        eventos = {str(e) for e in id_evento} if id_evento else None
        proposicoes = {str(p) for p in id_proposicao} if id_proposicao else None

        def keep(row):
            if orgaos and row.get("idOrgao") not in orgaos:
                return False
            if eventos and row.get("idEvento") not in eventos:
                return False
            if proposicoes and row.get("ultimaApresentacaoProposicao_idProposicao") not in proposicoes:
                return False
            return True

        all_votacoes = []
        for ano in years:
            rows = await self.bulk.read_rows(
                DATASET, ano,
                transform=lambda r, a=ano: votacao_from_row(r, a),
                row_filter=keep if (orgaos or eventos or proposicoes) else None,
            )
            all_votacoes.extend(rows)
            print(f"[votacoes] {ano}: {len(rows)} registros (total {len(all_votacoes)})")

        return all_votacoes
