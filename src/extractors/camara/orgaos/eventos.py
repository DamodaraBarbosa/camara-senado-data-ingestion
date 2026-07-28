from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import json

from utils.bulk import intern_str, nullify, to_int, unflatten
from utils.periods import legislatura_of_year, resolve_years


class AsyncEventosExtractor(CamaraBaseExtractor):
    """Eventos por órgão, via junção de dois arquivos bulk.

    Antes: ``orgaos/{id}/eventos`` por órgão e por trimestre, montando ~16.000
    corrotinas num único ``gather`` sem lotes nem orçamento.

    Agora: ``eventosOrgaos-{ano}.csv`` traz as arestas órgão↔evento e
    ``eventos-{ano}.csv`` os atributos do evento. Um registro por aresta,
    mantendo o mesmo formato que a versão por órgão produzia.
    """

    EDGES = "eventosOrgaos"
    EVENTS = "eventos"

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
        years = await self.bulk.available_partitions(self.EVENTS, years)

        wanted = None
        if orgaos:
            wanted = {str(o.get("id")) for o in orgaos if o.get("id") is not None}

        all_eventos = []
        for ano in years:
            events = {
                row["id"]: row
                for row in await self.bulk.read_rows(self.EVENTS, ano, transform=_to_evento)
                if row["id"] is not None
            }
            edges = await self.bulk.read_rows(
                self.EDGES, ano,
                row_filter=(lambda r: r.get("idOrgao") in wanted) if wanted else None,
            )

            missing = 0
            for edge in edges:
                id_evento = to_int(edge.get("idEvento"))
                base = events.get(id_evento)
                if base is None:
                    missing += 1
                    continue
                record = dict(base)
                record["idOrgao"] = to_int(edge.get("idOrgao"))
                record["uriOrgao"] = nullify(edge.get("uriOrgao"))
                record["siglaOrgao"] = intern_str(nullify(edge.get("siglaOrgao")))
                record["idLegislatura"] = legislatura_of_year(ano)
                all_eventos.append(record)

            if missing:
                # Aresta apontando para evento de outro ano é normal na virada;
                # volume alto indicaria arquivos dessincronizados.
                print(f"[orgaos/eventos] {ano}: {missing} aresta(s) sem evento correspondente")
            print(f"[orgaos/eventos] {ano}: {len(edges)} arestas (total {len(all_eventos)})")

        return all_eventos


def _to_evento(row: dict) -> dict:
    return {
        "id": to_int(row.get("id")),
        "uri": nullify(row.get("uri")),
        "dataHoraInicio": nullify(row.get("dataHoraInicio")),
        "dataHoraFim": nullify(row.get("dataHoraFim")),
        "situacao": intern_str(nullify(row.get("situacao"))),
        "descricaoTipo": intern_str(nullify(row.get("descricaoTipo"))),
        "descricao": nullify(row.get("descricao")),
        "localExterno": nullify(row.get("localExterno")),
        "urlDocumentoPauta": nullify(row.get("urlDocumentoPauta")),
        # O arquivo usa ponto como separador: `localCamara.nome`.
        "localCamara": unflatten(row, "localCamara"),
    }
