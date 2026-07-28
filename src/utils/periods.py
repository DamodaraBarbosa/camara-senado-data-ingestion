"""Resolution of legislatura and year window.

This logic used to be copied across 8 extractors, each calling ``/legislaturas``
and reading ``dados[0]``. That depends on the API's default ordering
(``ordem=DESC``) staying stable — if the API changed its default, the window
would silently point to 1826.

Here the call is kept for behavior compatibility, but with arithmetic fallback:
legislaturas last 4 years and the 49th started in 1991.
"""
from datetime import datetime

# The 49th legislatura started in 1991; each legislatura lasts 4 years.
_ANCHOR_LEGISLATURA = 49
_ANCHOR_YEAR = 1991
_YEARS_PER_LEGISLATURA = 4


def legislatura_start_year(numero: int) -> int:
    """Start year of a legislatura. 57 -> 2023, 56 -> 2019."""
    return _ANCHOR_YEAR + (numero - _ANCHOR_LEGISLATURA) * _YEARS_PER_LEGISLATURA


def legislatura_of_year(year: int) -> int:
    """Active legislatura in a given year. 2023 -> 57."""
    return _ANCHOR_LEGISLATURA + (year - _ANCHOR_YEAR) // _YEARS_PER_LEGISLATURA


def years_for_legislatura(numero: int, today: datetime = None) -> list:
    """Years of a legislatura, truncated to the current year."""
    today = today or datetime.now()
    start = legislatura_start_year(numero)
    end = min(start + _YEARS_PER_LEGISLATURA - 1, today.year)
    return list(range(start, end + 1))


def legislaturas_for_years(years) -> list:
    """Distinct legislaturas covering a list of years."""
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
    """Resolve the year window an extractor should cover.

    Precedence (from most explicit to default):

    1. ``anos``       — literal list, for point-in-time backfill.
    2. ``ano_inicio`` — from a given year to the current year.
    3. ``init_legislatura`` — from the given legislatura to the current year.
    4. default        — only the current legislatura, preserving exactly the
       current pipeline behavior (today ``init_legislatura`` is always None
       in ``bundles_config.json``, and ``/legislaturas`` returns the latest).

    ``anos`` and ``ano_inicio`` are read from ``params`` in config, allowing
    backfill without code changes.
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
    """Start year of current legislatura via API, or None if unavailable."""
    try:
        payload = await client.get(session, "legislaturas")
        dados = payload.get("dados") or []
        if not dados:
            return None
        # max(id) instead of dados[0]: does not depend on API default ordering.
        atual = max(dados, key=lambda item: item.get("id", 0))
        data_inicio = atual.get("dataInicio")
        return int(data_inicio.split("-")[0]) if data_inicio else None
    except Exception as exc:  # noqa: BLE001 — arithmetic fallback covers any failure
        print(f"[periods] /legislaturas unavailable ({exc}); using arithmetic fallback.")
        return None
