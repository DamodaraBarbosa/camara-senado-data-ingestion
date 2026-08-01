"""Client for the Câmara's official bulk files.

Separate module from ``AsyncCamaraClient`` on purpose: different hosts, no
429 quota (it's a static CDN, not the API gateway), completely different timeout
profile (a 98 MB file legitimately takes time), and low concurrency.

Motivation: the v2 API is limited to 10 req/s per IP. N+1 fan-out extractors
summed to ~157,000 requests — ~4.4h best case, impossible within the 600s per-task
budget. The Câmara team itself recommends bulk files for database loading.
Here that becomes a few dozen downloads.
"""
import asyncio
import csv
import fcntl
import io
import json
import os
import time
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# No `total`: a large file may legitimately take minutes. What detects hanging
# is `sock_read` — silence on the socket, not total duration.
_BULK_TIMEOUT = aiohttp.ClientTimeout(total=None, connect=30, sock_read=60)

_ARQUIVOS = "https://dadosabertos.camara.leg.br/arquivos"
_CACHE_DIR = os.getenv("CAMARA_BULK_CACHE", "/tmp/camara_bulk")
_TTL_SECONDS = int(os.getenv("CAMARA_BULK_TTL_S", 21600))  # 6h; files are daily

# A 404 returns a tiny body; we use this to distinguish from a real file.
_MIN_VALID_BYTES = 1024


class BulkNotFound(RuntimeError):
    """Non-existent partition (e.g., future year)."""


class BulkDownloadIncomplete(RuntimeError):
    """Truncated download — never publish as complete."""


class BulkSchemaChanged(RuntimeError):
    """Expected column disappeared from upstream file."""


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    url_template: str
    partition: str  # "ano" | "legislatura" | "none"
    archive: str = None  # None | "zip"
    encoding: str = "utf-8-sig"  # All files come with BOM
    delimiter: str = ";"
    required: tuple = field(default=())

    def url(self, partition=None) -> str:
        if self.partition == "none":
            return self.url_template
        if partition is None:
            raise ValueError(f"dataset '{self.name}' requires partition '{self.partition}'")
        key = "ano" if self.partition == "ano" else "legislatura"
        return self.url_template.format(**{key: partition})

    def filename(self, partition=None) -> str:
        return self.url(partition).rsplit("/", 1)[-1]


DATASETS = {
    "proposicoes": DatasetSpec(
        "proposicoes", f"{_ARQUIVOS}/proposicoes/csv/proposicoes-{{ano}}.csv", "ano",
        required=("id", "uri", "siglaTipo", "ementa", "ultimoStatus_idSituacao"),
    ),
    "votacoes": DatasetSpec(
        "votacoes", f"{_ARQUIVOS}/votacoes/csv/votacoes-{{ano}}.csv", "ano",
        required=("id", "uri", "data", "idOrgao", "siglaOrgao"),
    ),
    "votacoesVotos": DatasetSpec(
        "votacoesVotos", f"{_ARQUIVOS}/votacoesVotos/csv/votacoesVotos-{{ano}}.csv", "ano",
        required=("idVotacao", "voto", "dataHoraVoto"),
    ),
    "votacoesOrientacoes": DatasetSpec(
        "votacoesOrientacoes",
        f"{_ARQUIVOS}/votacoesOrientacoes/csv/votacoesOrientacoes-{{ano}}.csv", "ano",
        required=("idVotacao", "siglaBancada", "orientacao"),
    ),
    "votacoesObjetos": DatasetSpec(
        "votacoesObjetos", f"{_ARQUIVOS}/votacoesObjetos/csv/votacoesObjetos-{{ano}}.csv", "ano",
        required=("idVotacao", "proposicao_id"),
    ),
    "votacoesProposicoes": DatasetSpec(
        "votacoesProposicoes",
        f"{_ARQUIVOS}/votacoesProposicoes/csv/votacoesProposicoes-{{ano}}.csv", "ano",
        required=("idVotacao", "proposicao_id"),
    ),
    "eventos": DatasetSpec(
        "eventos", f"{_ARQUIVOS}/eventos/csv/eventos-{{ano}}.csv", "ano",
        required=("id", "uri", "dataHoraInicio", "situacao"),
    ),
    "eventosOrgaos": DatasetSpec(
        "eventosOrgaos", f"{_ARQUIVOS}/eventosOrgaos/csv/eventosOrgaos-{{ano}}.csv", "ano",
        required=("idEvento", "idOrgao", "siglaOrgao"),
    ),
    "orgaosDeputados": DatasetSpec(
        "orgaosDeputados",
        f"{_ARQUIVOS}/orgaosDeputados/csv/orgaosDeputados-L{{legislatura}}.csv", "legislatura",
        required=("uriOrgao", "uriDeputado", "cargo"),
    ),
    "frentesDeputados": DatasetSpec(
        "frentesDeputados", f"{_ARQUIVOS}/frentesDeputados/csv/frentesDeputados.csv", "none",
        required=("id", "titulo", "deputado_.id"),
    ),
    "ceap": DatasetSpec(
        "ceap", "https://www.camara.leg.br/cotas/Ano-{ano}.csv.zip", "ano", archive="zip",
        required=("txNomeParlamentar", "ideCadastro", "vlrLiquido", "numAno"),
    ),
}


class CamaraBulkClient:
    def __init__(self, cache_dir=None, session=None, max_parallel_downloads=3,
                 ttl_seconds=None):
        self.cache_dir = Path(cache_dir or _CACHE_DIR)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_seconds = _TTL_SECONDS if ttl_seconds is None else ttl_seconds
        self._session = session
        self._owns_session = session is None
        self._sem = asyncio.Semaphore(max_parallel_downloads)

    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=_BULK_TIMEOUT)
        return self._session

    async def close(self):
        if self._session is not None and self._owns_session:
            await self._session.close()
            self._session = None

    def local_path(self, dataset: str, partition=None) -> Path:
        return self.cache_dir / DATASETS[dataset].filename(partition)

    # ------------------------------------------------------------------ download

    async def ensure_file(self, dataset: str, partition=None) -> Path:
        """Garante o arquivo em cache local, baixando se necessário."""
        spec = DATASETS[dataset]
        dest = self.local_path(dataset, partition)

        if self._is_fresh(dest):
            return dest

        async with self._sem:
            # flock serializa entre processos: votacoes/votacoes, votacoes/ids e
            # orgaos/votacoes rodam em subprocessos distintos e pedem o MESMO
            # arquivo. Sem isso, os três baixariam em paralelo.
            return await asyncio.to_thread(self._ensure_file_locked, spec, partition, dest)

    def _is_fresh(self, dest: Path) -> bool:
        if not dest.exists() or dest.stat().st_size < _MIN_VALID_BYTES:
            return False
        return (time.time() - dest.stat().st_mtime) < self.ttl_seconds

    def _ensure_file_locked(self, spec: DatasetSpec, partition, dest: Path) -> Path:
        lock_path = dest.with_suffix(dest.suffix + ".lock")
        with open(lock_path, "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            # Outro processo pode ter concluído enquanto esperávamos o lock.
            if self._is_fresh(dest):
                print(f"[bulk] {dest.name} já em cache (outro processo baixou)")
                return dest
            # asyncio.run aninhado é seguro: estamos numa worker thread.
            asyncio.run(self._download(spec.url(partition), dest))
            return dest

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, BulkDownloadIncomplete,
                                       asyncio.TimeoutError)),
        reraise=True,
    )
    async def _download(self, url: str, dest: Path):
        part = dest.with_suffix(dest.suffix + ".part")
        offset = part.stat().st_size if part.exists() else 0
        headers = {"Range": f"bytes={offset}-"} if offset else {}

        session = aiohttp.ClientSession(timeout=_BULK_TIMEOUT)
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 404:
                    raise BulkNotFound(url)

                if response.status == 206:
                    mode = "ab"
                elif response.status == 200:
                    offset, mode = 0, "wb"  # servidor ignorou o Range
                else:
                    response.raise_for_status()
                    offset, mode = 0, "wb"

                declared = response.headers.get("Content-Length")
                expected = offset + int(declared) if declared else None

                print(f"[bulk] baixando {dest.name}"
                      + (f" (retomando em {offset / 1e6:.1f} MB)" if offset else ""))
                with open(part, mode) as fh:
                    async for chunk in response.content.iter_chunked(1 << 20):
                        fh.write(chunk)
        finally:
            await session.close()

        size = part.stat().st_size
        if expected is not None and size != expected:
            raise BulkDownloadIncomplete(
                f"{dest.name}: {size} bytes recebidos, {expected} esperados"
            )

        os.replace(part, dest)  # publicação atômica
        print(f"[bulk] {dest.name} pronto ({size / 1e6:.1f} MB)")

    # ------------------------------------------------------------------ partições

    async def available_partitions(self, dataset: str, candidates, concurrency: int = 8):
        """Filtra as partições que de fato existem.

        Usa `Range: bytes=0-0` em vez de HEAD — mais confiável atrás de CDN e
        igualmente barato. Um ano inexistente devolve 404 (ou um corpo
        minúsculo).
        """
        spec = DATASETS[dataset]
        if spec.partition == "none":
            return [None]

        manifest = self._read_manifest(dataset)
        if manifest is not None:
            return [p for p in candidates if p in manifest]

        sem = asyncio.Semaphore(concurrency)

        # Sessão local e autocontida: os runners não têm como chamar close(),
        # e uma sessão pendurada gera warning de recurso não liberado ao sair.
        async with aiohttp.ClientSession(timeout=_BULK_TIMEOUT) as session:

            async def probe(part):
                async with sem:
                    try:
                        async with session.get(spec.url(part), headers={"Range": "bytes=0-0"}) as r:
                            if r.status == 404:
                                return None
                            total = _content_range_total(r.headers.get("Content-Range"))
                            if total is not None and total < _MIN_VALID_BYTES:
                                return None
                            return part
                    except aiohttp.ClientError:
                        return None

            probed = await asyncio.gather(*(probe(c) for c in candidates))

        found = [p for p in probed if p is not None]
        self._write_manifest(dataset, found)
        return found

    def _manifest_path(self, dataset: str) -> Path:
        return self.cache_dir / f".manifest_{dataset}.json"

    def _read_manifest(self, dataset: str):
        path = self._manifest_path(dataset)
        if not path.exists() or (time.time() - path.stat().st_mtime) > self.ttl_seconds:
            return None
        try:
            return set(json.loads(path.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, OSError):
            return None

    def _write_manifest(self, dataset: str, partitions):
        try:
            self._manifest_path(dataset).write_text(
                json.dumps(sorted(partitions)), encoding="utf-8"
            )
        except OSError:
            pass  # manifest é otimização; falhar aqui não é fatal

    # ------------------------------------------------------------------ leitura

    async def read_rows(self, dataset: str, partition=None, *, transform=None, row_filter=None):
        """Baixa (se preciso) e faz o parse do CSV.

        O parse roda em thread separada porque ``csv`` é síncrono e **não é
        preemptável** pelo ``asyncio.wait_for(600)`` do runner — um parse longo
        no event loop estouraria o timeout duro exatamente como aconteceu com
        ``votacoes/votacoes``.
        """
        path = await self.ensure_file(dataset, partition)
        spec = DATASETS[dataset]
        return await asyncio.to_thread(_read_rows_sync, path, spec, transform, row_filter)


def _content_range_total(header: str):
    """`bytes 0-0/12345` -> 12345."""
    if not header or "/" not in header:
        return None
    total = header.rsplit("/", 1)[-1].strip()
    return int(total) if total.isdigit() else None


@contextmanager
def _open_text(path: Path, spec: DatasetSpec):
    if spec.archive == "zip":
        with zipfile.ZipFile(path) as zf:
            member = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
            with zf.open(member) as raw:
                yield io.TextIOWrapper(raw, encoding=spec.encoding, newline="")
    else:
        with open(path, "r", encoding=spec.encoding, newline="") as fh:
            yield fh


def _assert_header(fieldnames, spec: DatasetSpec):
    """Falha alto se uma coluna esperada sumiu.

    Sem isto, um rename upstream produziria silenciosamente um arquivo inteiro
    de valores `None` — o pior modo de falha possível numa ingestão.
    """
    if not fieldnames:
        raise BulkSchemaChanged(f"{spec.name}: arquivo sem cabeçalho")
    missing = [col for col in spec.required if col not in fieldnames]
    if missing:
        raise BulkSchemaChanged(
            f"{spec.name}: colunas ausentes {missing}; presentes: {list(fieldnames)[:12]}"
        )


def _read_rows_sync(path: Path, spec: DatasetSpec, transform, row_filter):
    rows = []
    with _open_text(path, spec) as fh:
        reader = csv.DictReader(fh, delimiter=spec.delimiter)
        _assert_header(reader.fieldnames, spec)
        for row in reader:
            if row_filter is not None and not row_filter(row):
                continue
            # transform aqui dentro descarta a linha crua na hora, mantendo o
            # pico de memória em uma lista em vez de duas.
            rows.append(transform(row) if transform else row)
    return rows
