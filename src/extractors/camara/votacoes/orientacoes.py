from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import json

from utils.bulk import intern_str, nullify
from utils.periods import resolve_years


class AsyncVotacoesOrientacoes(CamaraBaseExtractor):
    """Orientações de bancada por votação, a partir dos arquivos bulk.

    Antes: uma requisição a ``votacoes/{id}/orientacoes`` por votação — ~9.846
    requisições contra uma API limitada a 10 req/s, o que nunca cabia no
    orçamento de 600s (entregava ~1.238 de 9.846).

    Agora: um arquivo CSV por ano. O mapeamento é 1:1 com o endpoint de
    detalhe, e o CSV já traz ``idVotacao``, sem chave a sintetizar.
    """

    DATASET = "votacoesOrientacoes"

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

        # Quando a dependência é fornecida, restringe às votações dela para
        # manter paridade exata com o comportamento atual.
        wanted = None
        if votacoes:
            wanted = {
                str(v.get("id")) for v in votacoes if v.get("id") is not None
            }

        all_orientacoes = []
        for ano in years:
            rows = await self.bulk.read_rows(
                self.DATASET, ano,
                transform=_to_orientacao,
                row_filter=(lambda r: r.get("idVotacao") in wanted) if wanted else None,
            )
            all_orientacoes.extend(rows)
            print(f"[orientacoes] {ano}: {len(rows)} registros (total {len(all_orientacoes)})")

        return all_orientacoes


def _to_orientacao(row: dict) -> dict:
    return {
        # A chave era injetada como `votacao_id` pelo extractor antigo; o
        # downstream já depende desse nome.
        "votacao_id": nullify(row.get("idVotacao")),
        "uriVotacao": nullify(row.get("uriVotacao")),
        "siglaOrgao": intern_str(nullify(row.get("siglaOrgao"))),
        "descricao": nullify(row.get("descricao")),
        "siglaBancada": intern_str(nullify(row.get("siglaBancada"))),
        "uriBancada": nullify(row.get("uriBancada")),
        "orientacao": intern_str(nullify(row.get("orientacao"))),
        # Presentes no endpoint de detalhe, ausentes no arquivo bulk.
        "codTipoLideranca": None,
        "uriPartido": None,
    }
