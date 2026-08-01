"""Helpers for transforming rows from bulk files.

CSV returns **everything as strings** and uses `""` where the API returns `null`.
Without explicit conversion, the output JSON would have `"id": "12345"` where
the consumer expects `"id": 12345` — a silent contract break that no count-based test catches.
"""
import re
import sys

_URI_ID = re.compile(r"/(\d+)/?$")
_DIGITS = re.compile(r"\D")


def nullify(value):
    """`""` -> None. CSV has no null; the API does."""
    if value is None:
        return None
    value = value.strip() if isinstance(value, str) else value
    return value or None


def to_int(value):
    """Convert to int, tolerating empty and garbage."""
    value = nullify(value)
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def to_float(value):
    """Convert to float, accepting comma or period as decimal separator."""
    value = nullify(value)
    if value is None:
        return None
    text = str(value).strip()
    # "1.234,56" (pt-BR) -> "1234.56"; "1234.56" stays as is.
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def to_fk(value):
    """Convert a foreign key, treating `0` as absence.

    Bulk files use `0` as a null sentinel in FKs (e.g., `idEvento` when a
    votação does not belong to any event), while the API returns `null`.
    Deliberately separated from ``to_int``: in ``aprovacao``, ``votosSim``
    and similar, zero is a legitimate value and nullifying it would distort
    the data.
    """
    parsed = to_int(value)
    return None if parsed == 0 else parsed


def id_from_uri(uri):
    """Extract numeric id from end of an API URI.

    `orgaosDeputados` returns only URIs without numeric id columns, so this
    is the only way to reconstruct `idOrgao`/`idDeputado`.
    """
    uri = nullify(uri)
    if uri is None:
        return None
    match = _URI_ID.search(str(uri))
    return int(match.group(1)) if match else None


def normalize_cnpj(value):
    """`085.324.290/0013-1` -> `08532429000131`.

    CEAP formats the document; the API returns digits only.
    """
    value = nullify(value)
    if value is None:
        return None
    digits = _DIGITS.sub("", str(value))
    return digits or None


def unflatten(row: dict, prefix: str, *, separators=(".", "_")):
    """Regroup flattened columns into a sub-dictionary.

    Bulk files are inconsistent with each other: `votacoesVotos` uses
    `deputado_id`, `frentesDeputados` uses `deputado_.id`, and `eventos`
    uses `localCamara.nome`. Accepting all forms avoids a per-file mapper.

    Args:
        prefix: logical group name, e.g., `"deputado"` or `"localCamara"`.

    Returns:
        Sub-dictionary with keys already without prefix (can be empty).
    """
    out = {}
    candidates = {f"{prefix}{sep}" for sep in separators}
    candidates |= {f"{prefix}_{sep}" for sep in separators if sep != "_"}
    # From most specific to most generic: `deputado_.` must be tested
    # before `deputado_`, otherwise `.id` would remain as key instead of `id`.
    ordered = sorted(candidates, key=len, reverse=True)

    for key, value in row.items():
        for cand in ordered:
            if key.startswith(cand):
                out[key[len(cand):]] = nullify(value)
                break
    return out


def intern_str(value):
    """Intern low-cardinality strings.

    In `votacoesVotos`, ~1.1M rows where `voto` has ~5 distinct values and
    `siglaPartido` has dozens. Interning cuts hundreds of MB of RSS.
    """
    if isinstance(value, str) and value:
        return sys.intern(value)
    return value
