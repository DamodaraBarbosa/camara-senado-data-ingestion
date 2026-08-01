from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import json

from utils.bulk import intern_str, nullify, to_int
from utils.periods import resolve_years


class AsyncIdsExtractor(CamaraBaseExtractor):
    """Detalhe das proposições a partir dos arquivos bulk.

    Era o caso mais extremo do pipeline: ``proposicoes/{id}`` para cada uma das
    ~124.867 proposições. A 10 req/s (teto da API) isso levaria ~3,5h; na
    prática entregava 1.800–2.900 registros, ou seja, 1,4–2,3% do total.

    O CSV traz o ``ultimoStatus`` achatado; aqui ele volta a ser aninhado como
    ``statusProposicao``, no formato do endpoint de detalhe.
    """

    DATASET = "proposicoes"

    async def extract(
        self,
        proposicoes: json = None,
        batch_size: int = 100,       # mantido por compatibilidade de assinatura
        init_legislatura: int = None,
        anos: list = None,
        ano_inicio: int = None,
        use_upstream_filter: bool = True,
    ):
        self.partial = False

        async with aiohttp.ClientSession() as session:
            years = await resolve_years(
                self.client, session,
                init_legislatura=init_legislatura, anos=anos, ano_inicio=ano_inicio,
            )
        years = await self.bulk.available_partitions(self.DATASET, years)

        wanted = None
        if proposicoes and use_upstream_filter:
            wanted = {
                str(p.get("id")) for p in proposicoes if p.get("id") is not None
            }

        all_ids = []
        for ano in years:
            rows = await self.bulk.read_rows(
                self.DATASET, ano,
                transform=_to_proposicao,
                row_filter=(lambda r: r.get("id") in wanted) if wanted else None,
            )
            all_ids.extend(rows)
            print(f"[ids] {ano}: {len(rows)} registros (total {len(all_ids)})")

        if wanted:
            encontrados = {str(p["id"]) for p in all_ids}
            faltando = len(wanted - encontrados)
            if faltando:
                # Proposição antiga referenciada por uma tramitação recente cai
                # num arquivo de ano fora da janela — esperado em volume baixo.
                print(f"[ids] {faltando} proposição(ões) do upstream fora da janela de anos.")
                self.partial = faltando / len(wanted) > 0.05

        return all_ids


def _to_proposicao(row: dict) -> dict:
    uri = nullify(row.get("uri"))
    return {
        "id": to_int(row.get("id")),
        "uri": uri,
        "siglaTipo": intern_str(nullify(row.get("siglaTipo"))),
        "codTipo": to_int(row.get("codTipo")),
        "numero": to_int(row.get("numero")),
        "ano": to_int(row.get("ano")),
        "descricaoTipo": intern_str(nullify(row.get("descricaoTipo"))),
        "ementa": nullify(row.get("ementa")),
        "ementaDetalhada": nullify(row.get("ementaDetalhada")),
        "keywords": nullify(row.get("keywords")),
        "dataApresentacao": nullify(row.get("dataApresentacao")),
        "uriOrgaoNumerador": nullify(row.get("uriOrgaoNumerador")),
        "uriPropAnterior": nullify(row.get("uriPropAnterior")),
        "uriPropPrincipal": nullify(row.get("uriPropPrincipal")),
        "uriPropPosterior": nullify(row.get("uriPropPosterior")),
        "urlInteiroTeor": nullify(row.get("urlInteiroTeor")),
        "urnFinal": nullify(row.get("urnFinal")),
        # Determinístico a partir da uri; o CSV não traz a coluna.
        "uriAutores": f"{uri}/autores" if uri else None,
        # O CSV achata como `ultimoStatus_*`; a API aninha em `statusProposicao`.
        "statusProposicao": {
            "dataHora": nullify(row.get("ultimoStatus_dataHora")),
            "sequencia": to_int(row.get("ultimoStatus_sequencia")),
            "siglaOrgao": intern_str(nullify(row.get("ultimoStatus_siglaOrgao"))),
            "uriOrgao": nullify(row.get("ultimoStatus_uriOrgao")),
            "idOrgao": to_int(row.get("ultimoStatus_idOrgao")),
            "uriRelator": nullify(row.get("ultimoStatus_uriRelator")),
            "regime": intern_str(nullify(row.get("ultimoStatus_regime"))),
            "descricaoTramitacao": intern_str(nullify(row.get("ultimoStatus_descricaoTramitacao"))),
            "codTipoTramitacao": to_int(row.get("ultimoStatus_idTipoTramitacao")),
            "descricaoSituacao": intern_str(nullify(row.get("ultimoStatus_descricaoSituacao"))),
            "codSituacao": to_int(row.get("ultimoStatus_idSituacao")),
            "despacho": nullify(row.get("ultimoStatus_despacho")),
            "apreciacao": intern_str(nullify(row.get("ultimoStatus_apreciacao"))),
            "url": nullify(row.get("ultimoStatus_url")),
            # Só existe no endpoint de detalhe.
            "ambito": None,
        },
        # Exclusivos do endpoint de detalhe, sem equivalente bulk (aceito).
        "justificativa": None,
        "texto": None,
    }
