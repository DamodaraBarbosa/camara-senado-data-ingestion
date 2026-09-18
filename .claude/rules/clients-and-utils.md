---
name: clients-and-utils
description: HTTP client, bulk downloads, I/O patterns, env vars
paths:
  - src/clients/**
  - src/utils/**
---

## AsyncCamaraClient (camara_client.py)

### Rate Limiting

- **Token Bucket (RateLimiter):** 8 rps (`CAMARA_RATE_LIMIT_RPS` env var).
- **Semaphore:** 16 concurrent requests (`CAMARA_MAX_CONCURRENCY`).
- **Circuit Breaker:** Pauses when 429 (rate limit) or 5xx storms detected (`CAMARA_5XX_TRIP_THRESHOLD`).

### Retry Policy

- **429 (Too Many Requests):** 5 attempts, jitter 1–8s.
- **Other errors (500, 502, 503, 504):** 3 attempts, jitter 1–8s.
- **404 (Not Found):** Returns `{}` immediately (not an error).

### Usage

```python
async with aiohttp.ClientSession() as session:
    client = AsyncCamaraClient(session)
    result = await client.get(endpoint, **params)
    # result is a dict (empty if 404) or raises on unrecoverable error
```

Do not create custom retry or rate-limit logic in extractors; the shared client handles it.

## CamaraBulkClient (camara_bulk_client.py)

### Bulk File Registry

Manages cached CSV downloads from Câmara bulk servers:
- `https://dadosabertos.camara.leg.br/arquivos/...` (Câmara data)
- `camara.leg.br/cotas` (CEAP)

Caching uses:
- TTL: `CAMARA_BULK_TTL_S` (default ~1 hour).
- Directory: `CAMARA_BULK_CACHE` (default `/tmp/camara_bulk`).
- Locking: `flock` for concurrent writes.
- Cooperative parsing deadline: aborts parse if task budget expires.

### Error Types

```python
class BulkNotFound(Exception): pass
class BulkDownloadIncomplete(Exception): pass
class BulkParseTimeout(Exception): pass
class BulkSchemaChanged(Exception): pass
```

Extractors may catch and handle these, or let them propagate (runner handles retries).

### Usage

```python
async def extract(self):
    for year in years:
        rows = await self.bulk.read_rows(self.DATASET, year)
        for row in rows:
            yield self._to_output_record(row)
```

`read_rows()` returns an async iterator of dicts (one per CSV row).

## task_io.py — Canonical I/O

### read_dependency()

```python
def read_dependency(destination, bundle, name, run_id, ingestion_date):
    # Reads from local cache or S3 raw/
    # Returns list of dicts from NDJSON
```

Called by runners to load upstream extractor output. `destination` is `local` or `s3`.

### write_output()

```python
def write_output(records, destination, bundle, name, run_id, ingestion_date):
    # Accepts: list, async generator, or bare generator
    # Writes NDJSON to local cache or S3
    # Raises EmptyExtractionError if records is empty
```

**Key behaviors:**
- Writes to `.part` file, then `os.replace()` (atomic for filesystem).
- For S3: uses multipart upload (lazy, starts only if data exceeds threshold). Single small payload uses `put_object()`.
- **Empty-guard:** Raises `EmptyExtractionError` if no records; skips the write and does not overwrite existing data.
- **Idempotent:** Replays are safe (file is overwritten).

### S3 Key Format

```
s3://dataplatform-camara-{env}-db/raw/{bundle}/{name}/ingestion_date=YYYY-MM-DD/{name}_{run_id}.json
```

- `ingestion_date=YYYY-MM-DD` is a Hive partition key (used by Glue).
- `{run_id}` is the Airflow task execution timestamp (format: `YYYYMMDD_HHMMSS`).
- Data is NDJSON (one JSON object per line).

### Env Vars

- `CAMARA_CACHE_DIR`: Local scratch (`/tmp` default).
- `S3_UPLOAD_PART_BYTES`: Multipart upload chunk size.

## Concurrency Utilities (concurrency.py)

### gather_aligned()

```python
results = await gather_aligned(
    coro1, coro2, coro3, ...
)
# Returns [result1, result2, result3, ...] preserving order
# On error, returns None in that position (doesn't raise)
```

Order-preserving, fails gracefully (one failure doesn't stop others).

### gather_with_coverage()

Like `gather_aligned()` but tracks success count and allows partial results.

### assert_usable()

```python
assert_usable(results, min_coverage=0.95)
# Raises InsufficientData if (successful_count / total) < min_coverage
```

Config: `CAMARA_MIN_COVERAGE` env var (default 0.95).

## Budget.py

A `Deadline` class enforces per-task timeouts. The runner sets `CAMARA_TASK_BUDGET_S` env var; extractors don't manage it directly.

## Bulk.py — CSV Parsing

Helpers for converting CSV string values to typed Python objects:

- `to_int(s)`: str/None → int/None.
- `to_float(s)`: str/None → float/None.
- `intern_str(s)`: str → intern'd string (memory optimization).
- `unflatten(flat_dict)`: `{a.b.c: val}` → `{a: {b: {c: val}}}`.

Use these in bulk-based extractors (despesas, frentes/membros, etc.) to match API response shape.

## Env Vars

**Client & rate limiting:**
- `CAMARA_RATE_LIMIT_RPS` (default 8).
- `CAMARA_MAX_CONCURRENCY` (default 16).
- `CAMARA_5XX_TRIP_THRESHOLD` (circuit breaker threshold).
- `CAMARA_TASK_BUDGET_S` (set by runner; enforces deadline).
- `CAMARA_MIN_COVERAGE` (default 0.95).

**Bulk & cache:**
- `CAMARA_BULK_CACHE` (default `/tmp/camara_bulk`).
- `CAMARA_BULK_TTL_S` (TTL in seconds).

**I/O:**
- `CAMARA_CACHE_DIR` (local scratch).
- `S3_UPLOAD_PART_BYTES` (multipart chunk size).

**Alerts:**
- `CAMARA_ALERT_SNS_TOPIC_ARN` (failure notifications).
