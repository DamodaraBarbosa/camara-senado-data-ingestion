"""Testes do I/O de dependências.

Bug que motivou o módulo: 6 runners gravavam `{nome}.json` mas liam
`{nome}_{run_id}.json`. O cache errava 100% das vezes e cada task dependente
re-extraía o upstream inteiro — 21 misses por execução, queimando quota de uma
API limitada a 10 req/s.

O path S3 é o único que roda em produção e era 100% descoberto. Ele é testado
aqui contra um dublê de `boto3.client("s3")` em vez de `moto`: o dublê roda em
qualquer versão de Python (o venv local é 3.8, `moto` 5.x exige ≥3.9), não
adiciona dependência de teste, e permite afirmar *quais* chamadas S3 são feitas —
que é exatamente o que as correções desta rodada mudaram.
"""
import json

import boto3
import pytest

from utils import task_io
from utils.task_io import (
    DependencyCacheMiss,
    EmptyExtractionError,
    cache_path,
    read_dependency,
    resolve_ingestion_date,
    s3_key,
    write_output,
)

DATE = "2026-09-06"


@pytest.fixture
def dest(tmp_path):
    return {"type": "local", "cache_dir": str(tmp_path)}


# --------------------------------------------------------------------------
# Dublê de S3
# --------------------------------------------------------------------------

class FakeS3:
    """S3 mínimo, com as invariantes que importam para `_write_s3`.

    Em particular recusa parte de 0 byte, como o S3 real faz: era esse o risco
    concreto de trocar o array JSON (que sempre tinha ao menos os 2 bytes de
    `[]`) por NDJSON, onde zero registro é zero byte.
    """

    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.uploads = {}
        self.completed = []
        self.aborted = []
        self.put_calls = []
        self._next_upload_id = 0

    def create_multipart_upload(self, Bucket, Key, ContentType=None):
        self._next_upload_id += 1
        upload_id = f"upload-{self._next_upload_id}"
        self.uploads[upload_id] = {"key": Key, "parts": {}}
        return {"UploadId": upload_id}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        if not Body:
            raise AssertionError("S3 recusa parte de 0 byte")
        self.uploads[UploadId]["parts"][PartNumber] = Body
        return {"ETag": f'"etag-{PartNumber}"'}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        upload = self.uploads.pop(UploadId)
        numbers = [p["PartNumber"] for p in MultipartUpload["Parts"]]
        assert numbers == sorted(numbers), "partes fora de ordem"
        self.objects[Key] = b"".join(upload["parts"][n] for n in numbers)
        self.completed.append(Key)

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.uploads.pop(UploadId, None)
        self.aborted.append(Key)

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[Key] = Body
        self.put_calls.append(Key)

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise RuntimeError("404 Not Found")
        return {"ContentLength": len(self.objects[Key])}

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise RuntimeError("404 Not Found")
        return {"Body": _Body(self.objects[Key])}


class _Body:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return self._payload


@pytest.fixture
def s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: fake)
    return fake


@pytest.fixture
def s3_dest():
    return {"type": "s3", "bucket": "bucket-prod"}


# --------------------------------------------------------------------------
# Path local
# --------------------------------------------------------------------------

async def test_write_read_roundtrip(dest):
    data = [{"id": 1, "nome": "acentuação"}, {"id": 2, "nome": "ção"}]
    assert await write_output(data, dest, "orgaos", "orgaos", "r1") == 2
    assert read_dependency(dest, "orgaos", "orgaos", "r1") == data


async def test_read_and_write_agree_on_path(dest, tmp_path):
    """O bug original: leitura e escrita divergiam no run_id."""
    await write_output([{"id": 1}], dest, "orgaos", "orgaos", "run-abc")
    expected = tmp_path / "orgaos" / "orgaos_run-abc.json"
    assert expected.exists()
    assert cache_path("orgaos", "orgaos", "run-abc", str(tmp_path)) == expected


async def test_run_id_namespacing_isolates_runs(dest):
    await write_output([{"v": "antigo"}], dest, "orgaos", "orgaos", "r1")
    await write_output([{"v": "novo"}], dest, "orgaos", "orgaos", "r2")
    assert read_dependency(dest, "orgaos", "orgaos", "r1") == [{"v": "antigo"}]
    assert read_dependency(dest, "orgaos", "orgaos", "r2") == [{"v": "novo"}]


async def test_explicit_path_does_not_corrupt_cache_key(dest, tmp_path):
    """`destination["path"]` gera cópia extra, mas nunca move a chave de cache."""
    explicit = tmp_path / "saida_custom.json"
    dest_with_path = {**dest, "path": str(explicit)}

    await write_output([{"id": 1}], dest_with_path, "orgaos", "orgaos", "r1")

    assert explicit.exists()
    assert read_dependency(dest_with_path, "orgaos", "orgaos", "r1") == [{"id": 1}]
    assert (tmp_path / "orgaos" / "orgaos_r1.json").exists()


async def test_accepts_iterator_for_streaming(dest):
    """Grava sem materializar tudo — importa para votacoesVotos (~1,1M linhas)."""
    assert await write_output(({"i": i} for i in range(5)), dest, "b", "n", "r1") == 5
    assert len(read_dependency(dest, "b", "n", "r1")) == 5


def test_strict_mode_raises_on_miss(dest, monkeypatch):
    """Miss deve falhar alto — recomputar em silêncio é o que queimava quota."""
    monkeypatch.setattr(task_io, "STRICT_DEPENDENCY_CACHE", True)
    with pytest.raises(DependencyCacheMiss):
        read_dependency(dest, "orgaos", "inexistente", "r1")


def test_non_strict_mode_returns_none(dest, monkeypatch):
    monkeypatch.setattr(task_io, "STRICT_DEPENDENCY_CACHE", False)
    assert read_dependency(dest, "orgaos", "inexistente", "r1") is None


# --------------------------------------------------------------------------
# NDJSON
# --------------------------------------------------------------------------

async def test_local_output_is_ndjson(dest, tmp_path):
    """Um objeto completo por linha, sem array externo.

    O SerDe JSON do Glue/Athena exige exatamente isso; o array pretty-printed
    anterior tornava impossível qualquer tabela sobre `raw/`.
    """
    await write_output([{"id": 1}, {"id": 2}], dest, "b", "n", "r1")

    lines = (tmp_path / "b" / "n_r1.json").read_text(encoding="utf-8").splitlines()
    assert lines == ['{"id": 1}', '{"id": 2}']
    assert all(json.loads(line) for line in lines)


def test_reads_legacy_json_array(dest, tmp_path):
    """Cache local gravado antes desta mudança continua legível."""
    legacy = tmp_path / "b" / "n_r1.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text('[\n  {"id": 1},\n  {"id": 2}\n]', encoding="utf-8")

    assert read_dependency(dest, "b", "n", "r1") == [{"id": 1}, {"id": 2}]


def test_corrupt_ndjson_line_is_named(dest, tmp_path, monkeypatch):
    monkeypatch.setattr(task_io, "STRICT_DEPENDENCY_CACHE", False)
    path = tmp_path / "b" / "n_r1.json"
    path.parent.mkdir(parents=True)
    path.write_text('{"id": 1}\nnao-e-json\n', encoding="utf-8")

    assert read_dependency(dest, "b", "n", "r1") is None


# --------------------------------------------------------------------------
# Chave particionada
# --------------------------------------------------------------------------

def test_s3_key_is_partitioned_by_ingestion_date():
    assert s3_key("orgaos", "votacoes", "r1", DATE) == (
        f"raw/orgaos/votacoes/ingestion_date={DATE}/votacoes_r1.json"
    )


def test_ingestion_date_prefers_the_explicit_event_field():
    event = {"ingestion_date": "2026-09-06", "run_id": "scheduled__2026-08-30T06:00:00+00:00"}
    assert resolve_ingestion_date(event) == "2026-09-06"


@pytest.mark.parametrize("run_id,expected", [
    ("scheduled__2026-08-23T06:00:00+00:00", "2026-08-23"),
    ("manual__2026-08-21T02:12:56.113160+00:00", "2026-08-21"),
])
def test_ingestion_date_falls_back_to_the_date_inside_run_id(run_id, expected):
    """Determinístico para imagens antigas: leitor e escritor resolvem igual.

    Sem isso, o fallback seria "hoje" e uma dagrun que atravessasse a meia-noite
    UTC resolveria datas distintas em tasks diferentes, quebrando o cache.
    """
    assert resolve_ingestion_date({"run_id": run_id}) == expected


def test_ingestion_date_last_resort_is_today():
    from datetime import date
    assert resolve_ingestion_date({"run_id": "smoke-test-20260820"}) == date.today().isoformat()


async def test_s3_roundtrip_uses_the_partitioned_key(s3, s3_dest):
    data = [{"id": 1, "nome": "ação"}, {"id": 2}]
    assert await write_output(data, s3_dest, "votacoes", "votacoes", "r1", DATE) == 2

    key = s3_key("votacoes", "votacoes", "r1", DATE)
    assert list(s3.objects) == [key]
    assert s3.objects[key].decode("utf-8") == '{"id": 1, "nome": "ação"}\n{"id": 2}\n'
    assert read_dependency(s3_dest, "votacoes", "votacoes", "r1", DATE) == data


async def test_s3_write_never_copies_outside_the_partition(s3, s3_dest):
    """`destination["prefix"]` gerava uma segunda cópia não particionada.

    A cópia era inerte enquanto `alt_key == key`; com a partição na chave
    canônica ela passaria a duplicar cada arquivo — 632 MB só em `despesas` —
    e a espalhar arquivos soltos na raiz do LOCATION da tabela Glue.
    """
    dest_with_prefix = {**s3_dest, "prefix": "raw/votacoes/votacoes"}
    await write_output([{"id": 1}], dest_with_prefix, "votacoes", "votacoes", "r1", DATE)

    assert list(s3.objects) == [s3_key("votacoes", "votacoes", "r1", DATE)]


async def test_small_payload_uses_put_object_not_multipart(s3, s3_dest):
    """Sem flush não nasce multipart: um PUT, sem os 3 round-trips do MPU."""
    await write_output([{"id": 1}], s3_dest, "b", "n", "r1", DATE)

    assert s3.put_calls == [s3_key("b", "n", "r1", DATE)]
    assert s3.completed == []
    assert s3.uploads == {}


async def test_large_payload_streams_through_multipart(s3, s3_dest, monkeypatch):
    monkeypatch.setattr(task_io, "_S3_UPLOAD_PART_BYTES", 200)

    count = await write_output(
        ({"i": i, "pad": "x" * 50} for i in range(20)), s3_dest, "b", "n", "r1", DATE
    )

    key = s3_key("b", "n", "r1", DATE)
    assert count == 20
    assert s3.completed == [key]
    assert s3.put_calls == []
    assert len(s3.objects[key].decode("utf-8").splitlines()) == 20


# --------------------------------------------------------------------------
# Guarda de extração vazia — o achado central da run scheduled__2026-08-23
# --------------------------------------------------------------------------

async def test_zero_records_fails_loudly(s3, s3_dest):
    """0 registros era indistinguível de sucesso. Agora falha alto."""
    with pytest.raises(EmptyExtractionError, match="0 registros"):
        await write_output([], s3_dest, "votacoes", "votacoes", "r1", DATE)

    assert s3.objects == {}


@pytest.fixture
def allow_empty(monkeypatch):
    """Injeta um par ficticio em _ALLOW_EMPTY.

    O mecanismo de exceção precisa continuar coberto, mas amarrar o teste a um
    extractor real amarraria também a configuração de produção — e a lista real
    está vazia justamente porque nenhum extractor conhecido é vazio de verdade.
    """
    monkeypatch.setattr(task_io, "_ALLOW_EMPTY", frozenset({("b", "vazio_ok")}))


def test_production_allowlist_is_empty():
    """Trava a descoberta da run de 2026-09-05.

    `eventos/pauta` e `eventos/votacoes` estiveram nesta lista por hipótese e
    gravaram 792 KB e 149 KB assim que a API respondeu. Uma entrada aqui desliga
    o guard para aquele dataset, então só entra com evidência.
    """
    assert task_io._ALLOW_EMPTY == frozenset()


async def test_zero_records_does_not_overwrite_good_data(s3, s3_dest, allow_empty):
    """O incidente exato: a retry gravou `[]` sobre 42.050 registros."""
    key = s3_key("b", "vazio_ok", "r1", DATE)
    s3.objects[key] = b'{"id": 1}\n' * 100

    with pytest.raises(EmptyExtractionError, match="sobrescrever"):
        await write_output([], s3_dest, "b", "vazio_ok", "r1", DATE)

    assert s3.objects[key] == b'{"id": 1}\n' * 100


async def test_allowlisted_extractor_may_write_an_empty_first_load(s3, s3_dest, allow_empty):
    """A exceção só vale para particão nova, e nunca via parte de 0 byte."""
    assert await write_output([], s3_dest, "b", "vazio_ok", "r1", DATE) == 0

    key = s3_key("b", "vazio_ok", "r1", DATE)
    assert s3.objects[key] == b""
    assert s3.put_calls == [key]  # nunca uma parte de 0 byte


async def test_eventos_pauta_no_longer_bypasses_the_guard(s3, s3_dest):
    """Regressão direta: este par passava batido e gravava 2 bytes."""
    with pytest.raises(EmptyExtractionError, match="0 registros"):
        await write_output([], s3_dest, "eventos", "pauta", "r1", DATE)

    assert s3.objects == {}


async def test_zero_records_aborts_the_multipart_upload(s3, s3_dest, monkeypatch):
    """Sem o abort, as partes órfãs ficariam faturando em silêncio."""
    monkeypatch.setattr(task_io, "_S3_UPLOAD_PART_BYTES", 10)

    async def _explode():
        yield {"pad": "x" * 50}
        raise RuntimeError("API caiu no meio")

    with pytest.raises(RuntimeError, match="API caiu"):
        await write_output(_explode(), s3_dest, "b", "n", "r1", DATE)

    assert s3.aborted == [s3_key("b", "n", "r1", DATE)]
    assert s3.objects == {}


async def test_local_write_also_refuses_to_zero_good_data(dest, tmp_path):
    await write_output([{"id": 1}], dest, "votacoes", "votacoes", "r1")

    with pytest.raises(EmptyExtractionError):
        await write_output([], dest, "votacoes", "votacoes", "r1")

    assert read_dependency(dest, "votacoes", "votacoes", "r1") == [{"id": 1}]
