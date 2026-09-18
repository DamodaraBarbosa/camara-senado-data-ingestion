---
name: extractors
description: Async extractor pattern, rate-limit resilience, data shape conventions
paths:
  - src/extractors/**/*.py
---

## Extractor Class Signature

All extractors inherit from `CamaraBaseExtractor` (in `src/extractors/camara/base.py`):

```python
class Async{Name}Extractor(CamaraBaseExtractor):
    ENDPOINT = "https://dadosabertos.camara.leg.br/api/v2/..." | "relative/path"
    
    async def extract(self, **params) -> list | AsyncGenerator:
        # params may include: dependencies (upstream values), partial (boolean flag)
        self.partial = False  # Set at start; True if degraded
        ...
        return [dicts] or async generator yielding dicts
```

## Rate Limiting & Resilience

**Do not add your own retry logic or semaphore.** The `AsyncCamaraClient` (shared via `self.client`) handles:

- **RateLimiter**: 8 rps (token bucket).
- **Concurrency**: 16 concurrent requests (semaphore).
- **CircuitBreaker**: Pauses on 429 or 5xx storms.
- **Retries**: 5 for 429; 3 for 500/502/503/504; jitter 1–8s full exponential backoff.
- **404 → `{}`.** No error raised; extractor sees an empty response.

If you need higher concurrency or custom retry, raise an issue; modify the shared client, not individual extractors.

## Async Patterns

### Fan-Out with Coverage Guarantee

Use `gather_aligned()` (order-preserving, `None` on failure) or `gather_with_coverage()` from `src/utils/concurrency.py`:

```python
async def extract(self):
    session = aiohttp.ClientSession()
    try:
        results = await gather_aligned(
            self.client.get(...),
            self.client.get(...),
            ...
        )
        assert_usable(results, min_coverage=0.95)  # Fail if < 95% success
        return [r for r in results if r is not None]
    finally:
        await session.close()
```

`assert_usable()` raises `InsufficientData` if coverage < `MIN_COVERAGE` (default 0.95, from env `CAMARA_MIN_COVERAGE`).

### Deadline / Task Budget

The runner sets `os.environ.setdefault("CAMARA_TASK_BUDGET_S", max(60, timeout-120))` before calling extractors. A `Deadline` in `src/utils/budget.py` enforces this. Extractors don't create deadlines; the runner/task manages it.

### Streaming (Async Generator)

For high-volume datasets (e.g., despesas, votacoesVotos), yield records one at a time:

```python
async def extract(self):
    for year in years:
        rows = await self.bulk.read_rows(self.DATASET, year)
        for row in rows:
            yield self._to_output_record(row)
```

`write_output()` consumes the generator → S3 multipart upload (no OOM).

### List Pattern

For small/medium datasets:

```python
async def extract(self):
    async with aiohttp.ClientSession() as session:
        ...
        return [record1, record2, ...]
```

## Data Shape

### Output Records

Return camelCase API field names (as-is from Câmara). Inject foreign keys if dependent:

```python
# If dependent on deputados, inject idDeputado
{
    "idDeputado": 123456,
    "nome": "...",
    "partido": "...",
    # ... other camelCase fields
}
```

### Bulk CSV Extractors

CSV files come from Câmara bulk servers. Transform strings to typed values using helpers in `src/utils/bulk.py`:

- `to_int(s)` → int or None
- `to_float(s)` → float or None
- `intern_str(s)` → intern'd string (for memory efficiency)
- `unflatten(dict)` → nested dict from flat keys (e.g., `a.b.c` → `{a: {b: {c: ...}}}`)

Example (despesas extractor):

```python
def _to_output_record(self, row):
    return {
        "idDeputado": to_int(row.get("MATRICULA")),
        "ano": to_int(row.get("ANO")),
        "mes": to_int(row.get("MES")),
        "valor": to_float(row.get("VALOR")),
        ...
    }
```

## Logging & Errors

- Use `print()` with `[prefix]` tags (`[runner]`, `[cache]`, `[bulk]`, `[client]`, `[ERROR]`).
- Comments and logs are a mix of English and Portuguese. Stay consistent with the file you're editing.
- Custom errors inherit from `RuntimeError` with docstrings citing the production incident (`scheduled__2026-08-23`, etc.).
- **Known exception:** `src/extractors/camara/proposicoes/codigo_tema.py` swallows exceptions and returns `[]` (was a workaround; document it, don't replicate).
- **Fail loudly:** Never publish degraded data. Set `self.partial = True` and raise if coverage fails or data is incomplete.

## Empty Extraction Guard

Never overwrite non-empty data with empty results. Check:

```python
if not results or (isinstance(results, list) and len(results) == 0):
    raise EmptyExtractionError(f"Expected data from {self.ENDPOINT}, got empty")
```

`write_output()` watches for `EmptyExtractionError` and skips the write.

## Testing

- Mock `self.client` with `async_mock.AsyncMock()`.
- For async generators, check the yielded sequence (don't await the generator itself).
- Patch `gather_aligned()` if you need to simulate partial failures.
- No network calls in unit tests; use fixtures.
- Regression tests have docstrings naming the incident they test for.
