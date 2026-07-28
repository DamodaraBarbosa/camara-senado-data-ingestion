from extractors.camara.base import CamaraBaseExtractor
import aiohttp
import json

from utils.periods import resolve_years
from extractors.camara.votacoes.votacoes import DATASET, votacao_from_row


class AsyncOrgaosVotacoesExtractor(CamaraBaseExtractor):
    """Votações por órgão — agora um agrupamento local, sem rede.

    Antes: ``orgaos/{id}/votacoes`` por órgão e por trimestre — 1.170 órgãos ×
    ~14 períodos = ~16.000 requisições, das quais só ~1.300 cabiam no
    orçamento.

    ``votacoes-{ano}.csv`` já carrega ``idOrgao``, então basta filtrar o mesmo
    arquivo que ``votacoes/votacoes`` e ``votacoes/ids`` já baixaram — o cache
    compartilhado torna o custo de rede efetivamente zero.
    """

    async def extract(
        self,
        init_legislatura: int = None,
        orgaos: json = None,
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

        wanted = None
        if orgaos:
            wanted = {str(o.get("id")) for o in orgaos if o.get("id") is not None}

        all_votacoes = []
        for ano in years:
            rows = await self.bulk.read_rows(
                DATASET, ano,
                transform=lambda r, a=ano: votacao_from_row(r, a),
                row_filter=(lambda r: r.get("idOrgao") in wanted) if wanted else None,
            )
            all_votacoes.extend(rows)
            print(f"[orgaos_votacoes] {ano}: {len(rows)} registros (total {len(all_votacoes)})")

        return all_votacoes
