"""Helpers de transformação de linhas dos arquivos bulk.

O CSV devolve **tudo como string** e usa `""` onde a API devolve `null`. Sem
conversão explícita, o JSON de saída ficaria com `"id": "12345"` onde o
consumidor espera `"id": 12345` — uma quebra silenciosa de contrato que nenhum
teste de contagem detecta.
"""
import re
import sys

_URI_ID = re.compile(r"/(\d+)/?$")
_DIGITS = re.compile(r"\D")


def nullify(value):
    """`""` -> None. O CSV não tem null; a API tem."""
    if value is None:
        return None
    value = value.strip() if isinstance(value, str) else value
    return value or None


def to_int(value):
    """Converte para int, tolerando vazio e lixo."""
    value = nullify(value)
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def to_float(value):
    """Converte para float, aceitando vírgula ou ponto como decimal."""
    value = nullify(value)
    if value is None:
        return None
    text = str(value).strip()
    # "1.234,56" (pt-BR) -> "1234.56"; "1234.56" fica como está.
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def to_fk(value):
    """Converte uma chave estrangeira, tratando `0` como ausência.

    Os arquivos bulk usam `0` como sentinela de nulo em FKs (ex.: `idEvento`
    quando a votação não pertence a evento algum), enquanto a API devolve
    `null`. Deliberadamente separado de ``to_int``: em ``aprovacao``,
    ``votosSim`` e afins o zero é um valor legítimo e anulá-lo distorceria o
    dado.
    """
    parsed = to_int(value)
    return None if parsed == 0 else parsed


def id_from_uri(uri):
    """Extrai o id numérico do fim de uma URI da API.

    `orgaosDeputados` só traz URIs, sem colunas de id numérico, então este é o
    único caminho para reconstruir `idOrgao`/`idDeputado`.
    """
    uri = nullify(uri)
    if uri is None:
        return None
    match = _URI_ID.search(str(uri))
    return int(match.group(1)) if match else None


def normalize_cnpj(value):
    """`085.324.290/0013-1` -> `08532429000131`.

    O CEAP formata o documento; a API devolve só dígitos.
    """
    value = nullify(value)
    if value is None:
        return None
    digits = _DIGITS.sub("", str(value))
    return digits or None


def unflatten(row: dict, prefix: str, *, separators=(".", "_")):
    """Reagrupa colunas achatadas num sub-dicionário.

    Os arquivos bulk não são consistentes entre si: `votacoesVotos` usa
    `deputado_id`, `frentesDeputados` usa `deputado_.id` e `eventos` usa
    `localCamara.nome`. Aceitar todas as formas evita um mapeador por arquivo.

    Args:
        prefix: nome lógico do grupo, ex. `"deputado"` ou `"localCamara"`.

    Returns:
        Sub-dicionário com as chaves já sem prefixo (pode vir vazio).
    """
    out = {}
    candidates = {f"{prefix}{sep}" for sep in separators}
    candidates |= {f"{prefix}_{sep}" for sep in separators if sep != "_"}
    # Do mais específico ao mais genérico: `deputado_.` precisa ser testado
    # antes de `deputado_`, senão sobraria `.id` como chave em vez de `id`.
    ordered = sorted(candidates, key=len, reverse=True)

    for key, value in row.items():
        for cand in ordered:
            if key.startswith(cand):
                out[key[len(cand):]] = nullify(value)
                break
    return out


def intern_str(value):
    """Interna strings de baixa cardinalidade.

    Em `votacoesVotos` são ~1,1M linhas onde `voto` tem ~5 valores distintos e
    `siglaPartido` algumas dezenas. Internar corta centenas de MB de RSS.
    """
    if isinstance(value, str) and value:
        return sys.intern(value)
    return value
