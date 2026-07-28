from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import json

from utils.bulk import intern_str, normalize_cnpj, nullify, to_float, to_int
from utils.periods import resolve_years


class AsyncDespesasExtractor(CamaraBaseExtractor):
    """Despesas da Cota Parlamentar (CEAP) a partir dos arquivos bulk.

    Antes: ``deputados/{id}/despesas`` por deputado e por ano (647 × 4 =
    ~2.588 requisições, mais paginação). Entregava 298k–479k registros
    parciais, variando conforme o rate limiting do momento.

    Os arquivos ficam em ``camara.leg.br/cotas`` (host diferente do restante) e
    vêm zipados.

    Nota de escopo: o CEAP inclui gastos de lideranças e bancadas (ex.
    ``LID.GOV-CD``) que a rota por deputado nunca devolve. Por decisão de
    produto eles são ingeridos, com ``deputadoId`` nulo — por isso o parâmetro
    ``deputados`` não é usado como filtro (filtrar por id descartaria justamente
    essas linhas).
    """

    DATASET = "ceap"

    async def extract(
        self,
        deputados: json = None,      # mantido por compatibilidade de assinatura
        init_legislatura: int = None,
        month: int = None,
        items: int = 1000,
        request_tries: int = 4,
        batch_size: int = 40,
        anos: list = None,
        ano_inicio: int = None,
    ):
        self.partial = False

        async with aiohttp.ClientSession() as session:
            years = await resolve_years(
                self.client, session,
                init_legislatura=init_legislatura, anos=anos, ano_inicio=ano_inicio,
            )
        years = await self.bulk.available_partitions(self.DATASET, years)

        mes = str(int(month)) if month is not None else None

        all_despesas = []
        for ano in years:
            rows = await self.bulk.read_rows(
                self.DATASET, ano,
                transform=_to_despesa,
                row_filter=(lambda r: r.get("numMes") == mes) if mes else None,
            )
            all_despesas.extend(rows)
            print(f"[despesas] {ano}: {len(rows)} registros (total {len(all_despesas)})")

        sem_deputado = sum(1 for d in all_despesas if d["deputadoId"] is None)
        if sem_deputado:
            print(f"[despesas] {sem_deputado} registro(s) de liderança/bancada "
                  "(sem ideCadastro) incluídos.")
        return all_despesas


def _to_despesa(row: dict) -> dict:
    return {
        # Nulo nas linhas de liderança/bancada, que não têm ideCadastro.
        "deputadoId": to_int(row.get("ideCadastro")),
        "nomeParlamentar": nullify(row.get("txNomeParlamentar")),
        "ano": to_int(row.get("numAno")),
        "mes": to_int(row.get("numMes")),
        "tipoDespesa": intern_str(nullify(row.get("txtDescricao"))),
        "codDocumento": to_int(row.get("ideDocumento")),
        "tipoDocumento": intern_str(nullify(row.get("txtDescricaoEspecificacao"))),
        "codTipoDocumento": to_int(row.get("indTipoDocumento")),
        "dataDocumento": nullify(row.get("datEmissao")),
        "numDocumento": nullify(row.get("txtNumero")),
        "valorDocumento": to_float(row.get("vlrDocumento")),
        "valorGlosa": to_float(row.get("vlrGlosa")),
        "valorLiquido": to_float(row.get("vlrLiquido")),
        "nomeFornecedor": nullify(row.get("txtFornecedor")),
        # A API devolve só dígitos; o CEAP vem formatado.
        "cnpjCpfFornecedor": normalize_cnpj(row.get("txtCNPJCPF")),
        "cnpjCpfFornecedorFormatado": nullify(row.get("txtCNPJCPF")),
        "codLote": to_int(row.get("numLote")),
        "parcela": to_int(row.get("numParcela")),
        "urlDocumento": nullify(row.get("urlDocumento")),
        # Aditivos exclusivos do CEAP.
        "codSubCota": to_int(row.get("numSubCota")),
        "codEspecificacaoSubCota": to_int(row.get("numEspecificacaoSubCota")),
        "siglaUf": intern_str(nullify(row.get("sgUF"))),
        "siglaPartido": intern_str(nullify(row.get("sgPartido"))),
        "idLegislatura": to_int(row.get("nuLegislatura")),
        "codLegislatura": to_int(row.get("codLegislatura")),
        "passageiro": nullify(row.get("txtPassageiro")),
        "trecho": nullify(row.get("txtTrecho")),
        "numRessarcimento": nullify(row.get("numRessarcimento")),
        "dataPagamentoRestituicao": nullify(row.get("datPagamentoRestituicao")),
        "valorRestituicao": to_float(row.get("vlrRestituicao")),
    }
