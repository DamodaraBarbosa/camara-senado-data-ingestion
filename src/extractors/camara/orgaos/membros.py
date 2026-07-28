from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import json

from utils.bulk import id_from_uri, intern_str, nullify
from utils.periods import legislaturas_for_years, resolve_years

# `orgaosDeputados` só traz URIs, sem colunas de id numérico. Se a extração de
# id falhar acima deste limite, algo mudou no formato e é melhor falhar alto do
# que emitir um arquivo cheio de ids nulos.
_MAX_URI_PARSE_FAILURE_RATE = 0.01


class AsyncMembrosExtractor(CamaraBaseExtractor):
    """Membros de órgãos a partir do arquivo bulk, particionado por legislatura.

    Antes: ``orgaos/{id}/membros`` por órgão e por trimestre — 1.170 órgãos ×
    ~14 períodos construídos num único ``gather`` sem lotes, sem orçamento e
    sem ``return_exceptions`` (~16.000 corrotinas de uma vez).

    Nota sobre contagem: a versão antiga consultava por trimestre, duplicando
    fortemente os mesmos vínculos. O arquivo bulk é um retrato limpo, então o
    total de linhas cai bastante — é correção, não perda de dados.
    """

    DATASET = "orgaosDeputados"

    async def extract(
        self,
        init_legislatura: int = None,
        orgaos: json = None,
        itens: int = 100,            # mantidos por compatibilidade de assinatura
        request_tries: int = 4,
        anos: list = None,
        ano_inicio: int = None,
    ):
        self.partial = False

        async with aiohttp.ClientSession() as session:
            years = await resolve_years(
                self.client, session,
                init_legislatura=init_legislatura, anos=anos, ano_inicio=ano_inicio,
            )
        legislaturas = await self.bulk.available_partitions(
            self.DATASET, legislaturas_for_years(years)
        )

        wanted = None
        if orgaos:
            wanted = {o.get("id") for o in orgaos if o.get("id") is not None}

        all_membros, unparsed = [], 0
        for legislatura in legislaturas:
            rows = await self.bulk.read_rows(
                self.DATASET, legislatura,
                transform=lambda r, l=legislatura: _to_membro(r, l),
            )
            for row in rows:
                if row["idOrgao"] is None or row["id"] is None:
                    unparsed += 1
                    continue
                if wanted is not None and row["idOrgao"] not in wanted:
                    continue
                all_membros.append(row)
            print(f"[orgaos/membros] L{legislatura}: {len(rows)} linhas "
                  f"(acumulado {len(all_membros)})")

        total = len(all_membros) + unparsed
        if total and (unparsed / total) > _MAX_URI_PARSE_FAILURE_RATE:
            raise ValueError(
                f"[orgaos/membros] {unparsed}/{total} linhas sem id extraível da URI — "
                "formato do arquivo provavelmente mudou."
            )
        if unparsed:
            print(f"[orgaos/membros] {unparsed} linha(s) descartada(s) por URI inválida.")

        return all_membros


def _to_membro(row: dict, legislatura: int) -> dict:
    return {
        # Sem colunas de id no arquivo — reconstruídos a partir das URIs.
        "id": id_from_uri(row.get("uriDeputado")),
        "uri": nullify(row.get("uriDeputado")),
        "nome": nullify(row.get("nomeDeputado")),
        "siglaPartido": intern_str(nullify(row.get("siglaPartido"))),
        "siglaUf": intern_str(nullify(row.get("siglaUF"))),  # atenção ao caixa
        "titulo": intern_str(nullify(row.get("cargo"))),
        "dataInicio": nullify(row.get("dataInicio")),
        "dataFim": nullify(row.get("dataFim")),
        "idOrgao": id_from_uri(row.get("uriOrgao")),
        "idLegislatura": legislatura,
        # Aditivos vindos do arquivo bulk.
        "uriOrgao": nullify(row.get("uriOrgao")),
        "siglaOrgao": intern_str(nullify(row.get("siglaOrgao"))),
        "nomeOrgao": nullify(row.get("nomeOrgao")),
        "nomePublicacaoOrgao": nullify(row.get("nomePublicacaoOrgao")),
        # Presentes no endpoint de detalhe, ausentes no arquivo bulk.
        "codTitulo": None,
        "uriPartido": None,
        "urlFoto": None,
    }
