from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import json
from collections import defaultdict

from utils.bulk import nullify, to_int
from utils.periods import resolve_years
from extractors.camara.votacoes.votacoes import DATASET, votacao_from_row


class AsyncVotacoesIdsExtractor(CamaraBaseExtractor):
    """Detalhe das votações a partir dos arquivos bulk.

    Antes: ``votacoes/{id}`` para cada uma das ~9.846 votações. Entregava
    ~2.396 antes de estourar o orçamento.

    Os dois campos-array do endpoint de detalhe têm arquivos bulk próprios e
    são reconstruídos aqui:
      - ``objetosPossiveis``    <- votacoesObjetos-{ano}.csv
      - ``proposicoesAfetadas`` <- votacoesProposicoes-{ano}.csv

    Só ``efeitosRegistrados`` não tem equivalente bulk (veio vazio nas amostras
    inspecionadas).
    """

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
        years = await self.bulk.available_partitions(DATASET, years)

        wanted = None
        if votacoes:
            wanted = {str(v.get("id")) for v in votacoes if v.get("id") is not None}

        all_votacoes = []
        for ano in years:
            rows = await self.bulk.read_rows(
                DATASET, ano,
                transform=lambda r, a=ano: votacao_from_row(r, a),
                row_filter=(lambda r: r.get("id") in wanted) if wanted else None,
            )
            objetos = await self._grouped(ano, "votacoesObjetos")
            afetadas = await self._grouped(ano, "votacoesProposicoes")

            for votacao in rows:
                key = votacao["id"]
                votacao["objetosPossiveis"] = objetos.get(key, [])
                votacao["proposicoesAfetadas"] = afetadas.get(key, [])
                # Sem equivalente nos arquivos bulk.
                votacao["efeitosRegistrados"] = []

            all_votacoes.extend(rows)
            print(f"[votacoes_ids] {ano}: {len(rows)} registros (total {len(all_votacoes)})")

        return all_votacoes

    async def _grouped(self, ano: int, dataset: str) -> dict:
        """Agrupa proposições por votação, a partir de um arquivo auxiliar."""
        try:
            rows = await self.bulk.read_rows(dataset, ano, transform=_to_proposicao_ref)
        except Exception as exc:  # noqa: BLE001 — auxiliar ausente não invalida a votação
            print(f"[votacoes_ids] {dataset}-{ano} indisponível ({exc}); seguindo sem esse campo.")
            self.partial = True
            return {}

        grouped = defaultdict(list)
        for row in rows:
            grouped[row.pop("_idVotacao")].append(row)
        # Ordem estável por id: as listas da API não vêm ordenadas, e sem isto
        # duas execuções produziriam diffs espúrios.
        for items in grouped.values():
            items.sort(key=lambda p: (p["id"] is None, p["id"]))
        return grouped


def _to_proposicao_ref(row: dict) -> dict:
    """Referência de proposição, no formato dos arrays do endpoint de detalhe.

    A API traz `dataApresentacao` nestes objetos; os arquivos bulk não. Em
    troca trazem `titulo`, que a API não expõe aqui.
    """
    return {
        "_idVotacao": nullify(row.get("idVotacao")),
        "id": to_int(row.get("proposicao_id")),
        "uri": nullify(row.get("proposicao_uri")),
        "siglaTipo": nullify(row.get("proposicao_siglaTipo")),
        "codTipo": to_int(row.get("proposicao_codTipo")),
        "numero": to_int(row.get("proposicao_numero")),
        "ano": to_int(row.get("proposicao_ano")),
        "ementa": nullify(row.get("proposicao_ementa")),
        "dataApresentacao": None,  # ausente nos arquivos bulk
        "titulo": nullify(row.get("proposicao_titulo")),  # aditivo do bulk
    }
