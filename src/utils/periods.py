"""Resolução de legislatura e janela de anos.

Esta lógica vivia copiada em 8 extractors, cada um chamando ``/legislaturas`` e
lendo ``dados[0]``. Isso depende da ordenação default da API (``ordem=DESC``)
permanecer estável — se a API mudasse o default, a janela silenciosamente
apontaria para 1826.

Aqui a chamada é mantida por compatibilidade de comportamento, mas com fallback
aritmético: legislaturas duram 4 anos e a 49ª começou em 1991.
"""
from datetime import datetime

# A 49ª legislatura começou em 1991; cada legislatura dura 4 anos.
_ANCHOR_LEGISLATURA = 49
_ANCHOR_YEAR = 1991
_YEARS_PER_LEGISLATURA = 4


def legislatura_start_year(numero: int) -> int:
    """Ano inicial de uma legislatura. 57 -> 2023, 56 -> 2019."""
    return _ANCHOR_YEAR + (numero - _ANCHOR_LEGISLATURA) * _YEARS_PER_LEGISLATURA


def legislatura_of_year(year: int) -> int:
    """Legislatura vigente em um ano. 2023 -> 57."""
    return _ANCHOR_LEGISLATURA + (year - _ANCHOR_YEAR) // _YEARS_PER_LEGISLATURA


def years_for_legislatura(numero: int, today: datetime = None) -> list:
    """Anos de uma legislatura, truncados no ano corrente."""
    today = today or datetime.now()
    start = legislatura_start_year(numero)
    end = min(start + _YEARS_PER_LEGISLATURA - 1, today.year)
    return list(range(start, end + 1))


def legislaturas_for_years(years) -> list:
    """Legislaturas distintas que cobrem uma lista de anos."""
    return sorted({legislatura_of_year(y) for y in years})


async def resolve_years(
    client,
    session,
    *,
    init_legislatura: int = None,
    anos: list = None,
    ano_inicio: int = None,
    today: datetime = None,
) -> list:
    """Resolve a janela de anos que um extractor deve cobrir.

    Precedência (do mais explícito ao default):

    1. ``anos``       — lista literal, para backfill pontual.
    2. ``ano_inicio`` — de um ano até o corrente.
    3. ``init_legislatura`` — da legislatura informada até o ano corrente.
    4. default        — apenas a legislatura corrente, preservando exatamente o
       comportamento atual do pipeline (hoje ``init_legislatura`` é sempre None
       em ``bundles_config.json``, e ``/legislaturas`` devolve a mais recente).

    ``anos`` e ``ano_inicio`` são lidos de ``params`` no config, permitindo
    backfill sem alterar código.
    """
    today = today or datetime.now()
    current_year = today.year

    if anos:
        return sorted({int(a) for a in anos})

    if ano_inicio is not None:
        return list(range(int(ano_inicio), current_year + 1))

    if init_legislatura is not None:
        return list(range(legislatura_start_year(int(init_legislatura)), current_year + 1))

    start_year = await _start_year_from_api(client, session)
    if start_year is None:
        start_year = legislatura_start_year(legislatura_of_year(current_year))
    return list(range(start_year, current_year + 1))


async def _start_year_from_api(client, session) -> int:
    """Ano inicial da legislatura corrente via API, ou None se indisponível."""
    try:
        payload = await client.get(session, "legislaturas")
        dados = payload.get("dados") or []
        if not dados:
            return None
        # max(id) em vez de dados[0]: não depende da ordenação default da API.
        atual = max(dados, key=lambda item: item.get("id", 0))
        data_inicio = atual.get("dataInicio")
        return int(data_inicio.split("-")[0]) if data_inicio else None
    except Exception as exc:  # noqa: BLE001 — fallback aritmético cobre qualquer falha
        print(f"[periods] /legislaturas indisponível ({exc}); usando fallback aritmético.")
        return None
