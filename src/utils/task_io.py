"""I/O de dependências e de saída — fonte única de verdade para os 10 runners.

Substitui ``_read_dependency_output``/``_write_output`` duplicados (e
divergentes) em ``bundles/*/app/runner.py``. Duas divergências causavam bugs
reais:

1. **Mismatch de run_id.** 6 runners (eventos, frentes, grupos, legislaturas,
   orgaos, partidos) gravavam ``{nome}.json`` mas liam
   ``{nome}_{run_id}.json`` — o cache errava 100% das vezes, e cada task
   dependente re-extraía o upstream inteiro. Eram 21 misses por execução,
   desperdiçando quota de uma API limitada a 10 req/s.

2. **Reuso de ``destination["path"]``.** 8 leitores usavam o ``path`` da task
   *corrente* como caminho da *dependência*. Não dispara hoje só porque o
   orquestrador omite ``path``, mas é uma armadilha latente.

Aqui o caminho de cache é sempre canônico e derivado de
``(bundle, nome, run_id)`` — nunca de ``destination["path"]``.
"""
import json
import os
from pathlib import Path

# Cache miss levanta erro em vez de recomputar silenciosamente. A recomputação
# silenciosa é justamente o que queimava quota sem ninguém perceber.
STRICT_DEPENDENCY_CACHE = os.getenv("STRICT_DEPENDENCY_CACHE", "1") not in ("0", "false", "False")

DEFAULT_CACHE_DIR = os.getenv("CAMARA_CACHE_DIR", "/tmp")


class DependencyCacheMiss(RuntimeError):
    """Dependência ausente do cache com STRICT_DEPENDENCY_CACHE ligado."""


def cache_path(bundle: str, name: str, run_id: str, cache_dir: str = None) -> Path:
    """Caminho canônico de cache. Sempre namespaced por run_id."""
    base = Path(cache_dir or DEFAULT_CACHE_DIR)
    return base / bundle / f"{name}_{run_id}.json"


def s3_key(bundle: str, name: str, run_id: str) -> str:
    """Chave canônica no S3, espelhando o prefixo que o DAG monta."""
    return f"raw/{bundle}/{name}/{name}_{run_id}.json"


def read_dependency(destination: dict, bundle: str, name: str, run_id: str):
    """Lê a saída de uma dependência do cache.

    Returns:
        Os dados, ou ``None`` se ausente e ``STRICT_DEPENDENCY_CACHE`` desligado.

    Raises:
        DependencyCacheMiss: ausente com modo estrito ligado.
    """
    destination = destination or {}
    dest_type = destination.get("type", "local")

    try:
        if dest_type == "s3":
            import boto3

            bucket = destination.get("bucket")
            key = s3_key(bundle, name, run_id)
            body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
            data = json.loads(body.read().decode("utf-8"))
            print(f"[cache] '{name}' lido de s3://{bucket}/{key}: {len(data)} registros")
            return data

        path = cache_path(bundle, name, run_id, destination.get("cache_dir"))
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        print(f"[cache] '{name}' lido de {path}: {len(data)} registros")
        return data

    except Exception as exc:  # noqa: BLE001
        message = f"[cache] '{name}' indisponível no cache: {exc}"
        if STRICT_DEPENDENCY_CACHE:
            raise DependencyCacheMiss(message) from exc
        print(f"{message} — recomputando.")
        return None


def write_output(data, destination: dict, bundle: str, name: str, run_id: str) -> int:
    """Grava a saída no cache canônico e, se pedido, no path explícito.

    Grava sempre no caminho canônico (para os dependentes encontrarem) e
    adicionalmente em ``destination["path"]`` quando o operador especificar um
    — sem que isso contamine a chave de cache.

    Args:
        data: lista de registros, ou qualquer iterável (streaming).

    Returns:
        Número de registros gravados.
    """
    destination = destination or {}
    dest_type = destination.get("type", "local")

    if dest_type == "s3":
        return _write_s3(data, destination, bundle, name, run_id)
    if dest_type == "local":
        return _write_local(data, destination, bundle, name, run_id)
    raise ValueError(f"Tipo de destino desconhecido: {dest_type}")


def _write_local(data, destination: dict, bundle: str, name: str, run_id: str) -> int:
    canonical = cache_path(bundle, name, run_id, destination.get("cache_dir"))
    count = _dump_json(data, canonical)
    print(f"[runner] {count} registros gravados em {canonical}")

    explicit = destination.get("path")
    if explicit and Path(explicit) != canonical:
        # Relê o canônico em vez de reconsumir `data`, que pode ser um iterador.
        with open(canonical, "r", encoding="utf-8") as fh:
            _dump_json(json.load(fh), Path(explicit))
        print(f"[runner] cópia adicional em {explicit}")

    return count


def _write_s3(data, destination: dict, bundle: str, name: str, run_id: str) -> int:
    import boto3

    bucket = destination.get("bucket")
    key = s3_key(bundle, name, run_id)

    records = data if isinstance(data, list) else list(data)
    body = json.dumps(records, ensure_ascii=False).encode("utf-8")

    boto3.client("s3").put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/json"
    )
    print(f"[runner] {len(records)} registros gravados em s3://{bucket}/{key}")

    explicit_prefix = destination.get("prefix")
    if explicit_prefix:
        alt_key = f"{explicit_prefix.rstrip('/')}/{name}_{run_id}.json"
        if alt_key != key:
            boto3.client("s3").put_object(
                Bucket=bucket, Key=alt_key, Body=body, ContentType="application/json"
            )
            print(f"[runner] cópia adicional em s3://{bucket}/{alt_key}")

    return len(records)


def _dump_json(data, path: Path) -> int:
    """Serializa registro a registro, sem materializar o JSON inteiro em memória.

    ``json.dumps(lista, indent=2)`` constrói uma segunda cópia integral em
    string. Para ``votacoesVotos`` (~1,1M registros) isso é centenas de MB
    desnecessários no pico.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")

    count = 0
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write("[")
        for record in data:
            fh.write("\n  " if count == 0 else ",\n  ")
            fh.write(json.dumps(record, ensure_ascii=False))
            count += 1
        fh.write("\n]" if count else "]")

    os.replace(tmp, path)  # publicação atômica: leitor nunca vê arquivo parcial
    return count
