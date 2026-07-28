from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import json

from utils.bulk import intern_str, nullify, to_int, unflatten
from utils.periods import resolve_years


class AsyncVotosExtractor(CamaraBaseExtractor):
    """Votos individuais a partir dos arquivos bulk.

    Antes: ``votacoes/{id}/votos`` por votação (~9.846 requisições), entregando
    ~22.253 registros parciais.

    É o maior volume da migração (~1,1M linhas/ano). Por isso as strings de
    baixa cardinalidade são internadas: ``voto`` tem ~5 valores distintos e
    ``siglaPartido`` algumas dezenas, então internar corta centenas de MB.
    """

    DATASET = "votacoesVotos"

    async def extract(
        self,
        votacoes: json = None,
        batch_size: int = 100,       # mantido por compatibilidade de assinatura
        init_legislatura: int = None,
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

        wanted = None
        if votacoes:
            wanted = {str(v.get("id")) for v in votacoes if v.get("id") is not None}

        all_votos = []
        for ano in years:
            rows = await self.bulk.read_rows(
                self.DATASET, ano,
                transform=_to_voto,
                row_filter=(lambda r: r.get("idVotacao") in wanted) if wanted else None,
            )
            all_votos.extend(rows)
            print(f"[votos] {ano}: {len(rows)} registros (total {len(all_votos)})")

        return all_votos


def _to_voto(row: dict) -> dict:
    # A API aninha o deputado sob a chave `deputado_` (com underscore final).
    deputado = unflatten(row, "deputado")
    return {
        "votacao_id": nullify(row.get("idVotacao")),
        "uriVotacao": nullify(row.get("uriVotacao")),
        "tipoVoto": intern_str(nullify(row.get("voto"))),
        "dataRegistroVoto": nullify(row.get("dataHoraVoto")),
        "deputado_": {
            "id": to_int(deputado.get("id")),
            "uri": deputado.get("uri"),
            "nome": deputado.get("nome"),
            "siglaPartido": intern_str(deputado.get("siglaPartido")),
            "uriPartido": intern_str(deputado.get("uriPartido")),
            "siglaUf": intern_str(deputado.get("siglaUf")),
            "idLegislatura": to_int(deputado.get("idLegislatura")),
            "urlFoto": deputado.get("urlFoto"),
        },
    }
