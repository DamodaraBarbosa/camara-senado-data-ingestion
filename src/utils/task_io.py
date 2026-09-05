"""Dependency and output I/O — single source of truth for the 10 runners.

Replaces duplicated (and divergent) ``_read_dependency_output``/``_write_output``
across ``bundles/*/app/runner.py``. Two divergences caused real bugs:

1. **run_id mismatch.** 6 runners (eventos, frentes, grupos, legislaturas,
   orgaos, partidos) wrote ``{nome}.json`` but read
   ``{nome}_{run_id}.json`` — cache failed 100% of the time, and each
   downstream task re-extracted the entire upstream. That was 21 misses per run,
   wasting quota on a 10 req/s-limited API.

2. **destination["path"] reuse.** 8 readers used the *current* task's ``path``
   as the *dependency* path. Doesn't trigger today only because the orchestrator
   omits ``path``, but it's a latent trap.

Here the cache path is always canonical and derived from
``(bundle, nome, run_id)`` — never from ``destination["path"]``.

Output format is **NDJSON** (one complete JSON object per line, no enclosing
array). The previous pretty-printed array was unreadable by the Glue/Athena JSON
SerDe, which requires exactly one object per line — no table over ``raw/`` could
work. S3 writes stream via multipart upload without materializing the whole
payload in memory.

S3 keys are **Hive-partitioned** by ``ingestion_date``. Before that, every weekly
run landed in the same prefix with the date only in the *file name*, so a table
over ``raw/votacoes/votacoes/`` would UNION every execution ever made, growing by
one per week, with no way to read a single load incrementally.
"""
import inspect
import json
import os
import re
from datetime import date
from pathlib import Path

_S3_UPLOAD_PART_BYTES = int(os.getenv("S3_UPLOAD_PART_BYTES", 8 << 20))  # 8 MB

# Cache miss raises error instead of silently recomputing. Silent recomputation
# is exactly what was burning quota without anyone noticing.
STRICT_DEPENDENCY_CACHE = os.getenv("STRICT_DEPENDENCY_CACHE", "1") not in ("0", "false", "False")

DEFAULT_CACHE_DIR = os.getenv("CAMARA_CACHE_DIR", "/tmp")

# Pares (bundle, extractor) que podem legitimamente devolver zero registros.
# Todo o resto falha alto quando a contagem e zero — ver `_guard_empty`.
#
# **Esta lista esta vazia de proposito: nenhum extractor conhecido tem resultado
# vazio valido.**
#
# Ela chegou a conter ("eventos", "pauta") e ("eventos", "votacoes"), porque os
# dois vieram vazios em 3 das 4 runs historicas e isso parecia consistente com
# dependerem da janela temporal — uma pauta so existiria para evento futuro ja
# agendado. A run manual de producao de 2026-09-05 desmentiu isso: apos o retry,
# `eventos/pauta` gravou 792 KB e `eventos/votacoes`, 149 KB. Os vazios
# historicos eram a mesma instabilidade de API que derrubou outras tasks naquela
# run (Connection timeout em /legislaturas e /referencias/tiposAutor, HTTP 504
# em eventos/{id}/pauta), nao ausencia de dados.
#
# Conclusao empirica: os 10 pares que ja zeraram em producao produzem dado
# quando a API responde. Manter os dois na lista deixaria o guard desligado
# justamente nos datasets que mais falham.
#
# O mecanismo continua existindo para o dia em que um extractor genuinamente
# vazio aparecer — mas a entrada precisa vir com evidencia, nao com hipotese.
_ALLOW_EMPTY = frozenset()

_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


class DependencyCacheMiss(RuntimeError):
    """Missing dependency from cache with STRICT_DEPENDENCY_CACHE enabled."""


class EmptyExtractionError(RuntimeError):
    """Extractor devolveu 0 registros onde isso nunca e um resultado valido.

    Existe por causa da run `scheduled__2026-08-23`: quatro datasets terminaram
    com 2 bytes (`[]`) e as 56 tasks reportaram `success`. `raw/votacoes/votacoes`
    foi de 42.050 registros para vazio sem ninguem ver. Falhar aqui transforma
    corrupcao silenciosa em alerta.
    """


def resolve_ingestion_date(event: dict) -> str:
    """Data da particao Hive, resolvida uma vez por task a partir do evento.

    Precedencia:

    1. ``event["ingestion_date"]`` — o que a DAG injeta (`{{ data_interval_end | ds }}`),
       identico nas 56 tasks da mesma dagrun.
    2. a primeira data ISO dentro do ``run_id`` — cobre `scheduled__2026-08-23T06:00...`
       e `manual__2026-08-21T02:12...` de forma deterministica, para imagens antigas
       ou execucoes fora da DAG.
    3. hoje — ultimo recurso para run local/ad hoc.

    Nao derivar so de (3): leitor e escritor de uma dependencia rodam em tasks ECS
    diferentes, e uma dagrun que atravessasse a meia-noite UTC resolveria datas
    distintas e quebraria o cache.
    """
    explicit = (event or {}).get("ingestion_date")
    if explicit:
        return str(explicit)

    match = _ISO_DATE.search(str((event or {}).get("run_id", "")))
    if match:
        return match.group(0)

    return date.today().isoformat()


def cache_path(bundle: str, name: str, run_id: str, cache_dir: str = None) -> Path:
    """Canonical local cache path. Always namespaced by run_id.

    Nao particionado: e scratch efemero dentro do container, ja isolado por
    run_id. A particao so existe no S3, onde o Athena precisa dela.
    """
    base = Path(cache_dir or DEFAULT_CACHE_DIR)
    return base / bundle / f"{name}_{run_id}.json"


def s3_key(bundle: str, name: str, run_id: str, ingestion_date: str) -> str:
    """Canonical S3 key, Hive-partitioned by ingestion_date.

    Unico ponto de verdade da chave: `read_dependency` e `_write_s3` chamam esta
    funcao, entao leitura e escrita nao tem como divergir.
    """
    return f"raw/{bundle}/{name}/ingestion_date={ingestion_date}/{name}_{run_id}.json"


def _parse_records(text: str, source: str) -> list:
    """Le NDJSON, aceitando o array legado gravado antes desta mudanca."""
    stripped = text.lstrip()
    if stripped.startswith("["):
        # Arquivo no formato antigo (array JSON). Nenhuma dagrun mistura os dois
        # — todas as 56 tasks usam a mesma imagem — mas caches locais de dev
        # sobrevivem a troca de branch.
        return json.loads(stripped) if stripped.strip() != "[]" else []

    records = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise ValueError(f"NDJSON invalido em {source}, linha {line_number}: {exc}") from exc
    return records


def read_dependency(destination: dict, bundle: str, name: str, run_id: str,
                    ingestion_date: str = None):
    """Le a saida de uma dependencia do cache.

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
            key = s3_key(bundle, name, run_id, ingestion_date)
            body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
            data = _parse_records(body.read().decode("utf-8"), f"s3://{bucket}/{key}")
            print(f"[cache] '{name}' lido de s3://{bucket}/{key}: {len(data)} registros")
            return data

        path = cache_path(bundle, name, run_id, destination.get("cache_dir"))
        with open(path, "r", encoding="utf-8") as fh:
            data = _parse_records(fh.read(), str(path))
        print(f"[cache] '{name}' lido de {path}: {len(data)} registros")
        return data

    except Exception as exc:  # noqa: BLE001
        message = f"[cache] '{name}' indisponivel no cache: {exc}"
        if STRICT_DEPENDENCY_CACHE:
            raise DependencyCacheMiss(message) from exc
        print(f"{message} — recomputando.")
        return None


async def write_output(data, destination: dict, bundle: str, name: str, run_id: str,
                       ingestion_date: str = None) -> int:
    """Grava a saida no cache canonico e, se pedido, no path explicito.

    Args:
        data: lista de registros, gerador sincrono, ou gerador assincrono (streaming).

    Returns:
        Numero de registros gravados.

    Raises:
        EmptyExtractionError: zero registros onde isso nunca e valido.
    """
    destination = destination or {}
    dest_type = destination.get("type", "local")

    if dest_type == "s3":
        return await _write_s3(data, destination, bundle, name, run_id, ingestion_date)
    if dest_type == "local":
        return await _write_local(data, destination, bundle, name, run_id)
    raise ValueError(f"Tipo de destino desconhecido: {dest_type}")


def _guard_empty(count: int, bundle: str, name: str, run_id: str, target: str,
                 existing_size=None) -> None:
    """Recusa publicar uma extracao vazia. Ver `EmptyExtractionError`.

    Duas regras:

    1. zero registros falha por padrao — nenhum dos extractors que ja zeraram em
       producao e confiavelmente vazio, entao *falhar* e a polaridade correta;
    2. mesmo para os de `_ALLOW_EMPTY`, nunca sobrescrever um objeto nao-vazio
       que ja esta na chave. Foi exatamente isso que a retry de 08-23 fez.
    """
    if count > 0:
        return

    if (bundle, name) not in _ALLOW_EMPTY:
        raise EmptyExtractionError(
            f"{bundle}/{name} devolveu 0 registros em {run_id}. Recusando gravar "
            f"em {target}: este extractor nao tem resultado vazio valido. "
            f"Se esta semana for legitimamente vazia, adicione o par a _ALLOW_EMPTY."
        )

    if existing_size:
        raise EmptyExtractionError(
            f"{bundle}/{name} devolveu 0 registros em {run_id}, mas {target} ja "
            f"tem {existing_size} bytes. Recusando sobrescrever dado bom com vazio."
        )


def _head_size(s3, bucket: str, key: str):
    """ContentLength do objeto, ou None se ele nao existe."""
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except Exception:  # noqa: BLE001 — 404/403 tratados igual: nao ha o que proteger
        return None


async def _write_local(data, destination: dict, bundle: str, name: str, run_id: str) -> int:
    canonical = cache_path(bundle, name, run_id, destination.get("cache_dir"))
    existing = canonical.stat().st_size if canonical.exists() else None

    tmp = canonical.with_suffix(canonical.suffix + ".part")
    count = await _dump_ndjson(data, tmp)
    _guard_empty(count, bundle, name, run_id, str(canonical), existing)

    os.replace(tmp, canonical)  # publicacao atomica: leitor nunca ve arquivo parcial
    print(f"[runner] {count} registros gravados em {canonical}")

    explicit = destination.get("path")
    if explicit and Path(explicit) != canonical:
        # Rele o canonico em vez de reconsumir `data`, que pode ser um iterador.
        explicit_path = Path(explicit)
        explicit_path.parent.mkdir(parents=True, exist_ok=True)
        with open(canonical, "r", encoding="utf-8") as src, \
                open(explicit_path, "w", encoding="utf-8") as dst:
            for line in src:
                dst.write(line)
        print(f"[runner] copia adicional em {explicit}")

    return count


async def _write_s3(data, destination: dict, bundle: str, name: str, run_id: str,
                    ingestion_date: str) -> int:
    import boto3

    bucket = destination.get("bucket")
    key = s3_key(bundle, name, run_id, ingestion_date)
    target = f"s3://{bucket}/{key}"
    s3 = boto3.client("s3")

    # O multipart so nasce no primeiro flush. Isso resolve dois problemas de uma
    # vez: um payload pequeno vira um unico PUT (sem os 3 round-trips do MPU), e
    # o caso de zero registro nunca precisa subir uma parte de 0 byte, que o
    # complete_multipart_upload rejeita.
    upload_id = None
    parts = []
    count = 0
    buffer = bytearray()

    def _flush(body: bytes) -> None:
        response = s3.upload_part(
            Bucket=bucket, Key=key, UploadId=upload_id,
            PartNumber=len(parts) + 1, Body=body,
        )
        parts.append({"ETag": response["ETag"], "PartNumber": len(parts) + 1})

    try:
        async for record in _to_async_iter(data):
            buffer += (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")
            count += 1

            if len(buffer) > _S3_UPLOAD_PART_BYTES:
                if upload_id is None:
                    upload_id = s3.create_multipart_upload(
                        Bucket=bucket, Key=key, ContentType="application/x-ndjson",
                    )["UploadId"]
                _flush(bytes(buffer))
                buffer = bytearray()

        # A guarda roda antes de qualquer publicacao. O multipart ja e atomico —
        # as partes so ficam visiveis no complete — entao o que faltava nao era
        # uma chave temporaria, era recusar a promocao.
        _guard_empty(count, bundle, name, run_id, target,
                     _head_size(s3, bucket, key) if count == 0 else None)

        if upload_id is None:
            s3.put_object(
                Bucket=bucket, Key=key, Body=bytes(buffer),
                ContentType="application/x-ndjson",
            )
            n_parts = 1
        else:
            if buffer:
                _flush(bytes(buffer))
            s3.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            n_parts = len(parts)

        print(f"[runner] {count} registros gravados em {target} ({n_parts} partes)")
        return count

    except Exception:
        if upload_id is not None:
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise


async def _dump_ndjson(data, path: Path) -> int:
    """Serializa registro a registro, sem materializar o JSON inteiro em memoria.

    ``json.dumps(lista, indent=2)`` constroi uma segunda copia integral em
    string. Para ``votacoesVotos`` (~1,1M registros) isso e centenas de MB
    desnecessarios no pico.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with open(path, "w", encoding="utf-8") as fh:
        async for record in _to_async_iter(data):
            fh.write(json.dumps(record, ensure_ascii=False))
            fh.write("\n")
            count += 1
    return count


async def _to_async_iter(data):
    """Normaliza lista/gerador sincrono/gerador assincrono num async for."""
    if inspect.isasyncgen(data):
        async for item in data:
            yield item
    elif inspect.isgenerator(data):
        for item in data:
            yield item
    elif isinstance(data, list):
        for item in data:
            yield item
    else:
        for item in data:
            yield item
