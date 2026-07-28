from extractors.camara.base import CamaraBaseExtractor

from utils.bulk import intern_str, nullify, to_int, unflatten


class AsyncFrentesMembrosExtractor(CamaraBaseExtractor):
    """Membros de frentes parlamentares a partir do arquivo bulk.

    Antes: ``frentes/{id}/membros`` para cada uma das 1.443 frentes. Passava,
    mas em 600,3s — a 0,3s do timeout duro, e sem nenhum guard de orçamento nem
    ``return_exceptions``. Era falha iminente, não estabilidade.

    ``frentesDeputados.csv`` não é particionado (98 MB), então o cache com TTL
    importa especialmente aqui.
    """

    DATASET = "frentesDeputados"

    async def extract(self, frentes=None, batch_size: int = 100):
        self.partial = False

        wanted = None
        if frentes:
            wanted = {str(f.get("id")) for f in frentes if f.get("id") is not None}

        membros = await self.bulk.read_rows(
            self.DATASET,
            transform=_to_membro,
            row_filter=(lambda r: r.get("id") in wanted) if wanted else None,
        )
        print(f"[frentes/membros] {len(membros)} registros")
        return membros


def _to_membro(row: dict) -> dict:
    """Achata o membro, resolvendo a colisão de ``titulo``.

    O CSV tem ``titulo`` (nome da *frente*) e ``deputado_.titulo`` (cargo do
    *membro*). Remover o prefixo ingenuamente faria um sobrescrever o outro.
    O objeto de membro da API carrega o cargo, então ``deputado_.titulo`` fica
    com ``titulo`` e o nome da frente vai para ``tituloFrente``.
    """
    deputado = unflatten(row, "deputado")
    return {
        "id": to_int(deputado.get("id")),
        "uri": deputado.get("uri"),
        "nome": deputado.get("nome"),
        "siglaPartido": intern_str(deputado.get("siglaPartido")),
        "uriPartido": intern_str(deputado.get("uriPartido")),
        "siglaUf": intern_str(deputado.get("siglaUf")),
        "idLegislatura": to_int(deputado.get("idLegislatura")),
        "urlFoto": deputado.get("urlFoto"),
        "codTitulo": deputado.get("codTitulo"),
        "titulo": deputado.get("titulo"),          # cargo do membro
        "dataInicio": nullify(row.get("dataInicio")),
        "dataFim": nullify(row.get("dataFim")),
        # Chave injetada pelo extractor antigo; o downstream depende dela.
        "idFrente": to_int(row.get("id")),
        "tituloFrente": nullify(row.get("titulo")),
    }
