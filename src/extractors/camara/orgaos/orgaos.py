from extractors.camara.base import CamaraBaseExtractor
import aiohttp

from utils.periods import legislatura_of_year, resolve_years


class AsyncOrgaosExtractor(CamaraBaseExtractor):
    """Órgãos da Câmara.

    A versão anterior varria ``orgaos?dataInicio=...`` trimestre a trimestre.
    Isso tinha dois defeitos sérios:

    1. ``dataInicio`` filtra o órgão pela data de *criação*, então só voltavam
       os criados dentro da janela — 180 de 1.639 órgãos (89% perdidos),
       excluindo justamente os permanentes como o Plenário (id 180), onde
       ocorre a maior parte das votações.
    2. Cada trimestre repetia a mesma consulta, inflando 180 órgãos distintos
       em 1.170 linhas duplicadas.

    Como o cadastro de órgãos não é particionável por data de forma útil, a
    extração passa a ser uma listagem paginada simples e completa.
    """

    ENDPOINT = 'orgaos'

    async def extract(
        self,
        init_legislatura: int = None,
        id_orgao: list = None,
        sigla: list = None,
        itens: int = 100,
        request_tries: int = 4,      # mantido por compatibilidade de assinatura
    ):
        self.partial = False

        params = {}
        if id_orgao:
            params['id'] = id_orgao
        if sigla:
            params['sigla'] = sigla

        async with aiohttp.ClientSession() as session:
            orgaos = await self.client.get_all_pages(
                session, self.ENDPOINT, params=params or None, itens=itens
            )
            years = await resolve_years(self.client, session, init_legislatura=init_legislatura)

        # Mantém a chave que os extractors dependentes já consomem.
        id_legislatura = legislatura_of_year(max(years)) if years else None
        for orgao in orgaos:
            orgao['idLegislatura'] = id_legislatura

        print(f"[orgaos] {len(orgaos)} órgãos")
        return orgaos
