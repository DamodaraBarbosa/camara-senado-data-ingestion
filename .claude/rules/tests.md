---
name: tests
description: Test layout, asyncio patterns, mocking, pytest config
paths:
  - tests/**/*.py
  - pytest.ini
---

## Test Layout

```
tests/unit/
├── clients/
│   ├── test_camara_client.py
│   └── test_camara_bulk_client.py
├── extractors/
│   ├── deputados/
│   │   └── test_despesas.py
│   ├── eventos/
│   │   └── test_eventos.py
│   ├── grupos/
│   │   └── test_membros.py
│   └── proposicoes/
│       └── test_proposicoes.py
├── utils/
│   ├── test_task_io.py
│   ├── test_concurrency.py
│   ├── test_bulk.py
│   └── test_periods.py
├── conftest.py
└── fixtures/
    └── bulk/
        └── votacoesOrientacoes-2025.csv
```

Coverage is thin (only 4 extractor bundles are tested). Regression tests for other bundles are welcome.

## pytest Configuration (pytest.ini)

```ini
[pytest]
pythonpath = src
testpaths = tests
asyncio_mode = auto
```

**`asyncio_mode = auto`:** Automatically marks async test functions (`async def`) as async tests. Do not use `@pytest.mark.asyncio` (it is redundant and will be removed).

## Async Testing

```python
@pytest.fixture
async def mock_client():
    return AsyncMock(spec=AsyncCamaraClient)

async def test_extractor_with_data(mock_client):
    extractor = AsyncDeputadosExtractor(client=mock_client)
    results = await extractor.extract()
    assert len(results) > 0
```

The `async def` fixture is implicitly marked as async. No decorator needed.

## Mocking Patterns

### AsyncMock

```python
from unittest.mock import AsyncMock

mock_client = AsyncMock()
mock_client.get.return_value = {"id": 1, "name": "..."}

# Use in extractor
result = await mock_client.get(...)
```

### Patching gather_aligned()

Extractors use `gather_aligned()` for fan-out. To simulate partial failures, patch it:

```python
import pytest
from src.utils.concurrency import gather_aligned

@pytest.fixture
def fake_gather():
    async def _fake(*coros):
        results = []
        for coro in coros:
            try:
                results.append(await coro)
            except Exception:
                results.append(None)
        return results
    return _fake

async def test_with_partial_failure(monkeypatch, fake_gather):
    monkeypatch.setattr("src.utils.concurrency.gather_aligned", fake_gather)
    # ... test behavior with failures
```

### FakeS3

A hand-written dict-based S3 mock:

```python
class FakeS3:
    def __init__(self):
        self.data = {}
    
    async def put_object(self, Bucket, Key, Body):
        self.data[(Bucket, Key)] = Body
    
    async def get_object(self, Bucket, Key):
        return {"Body": self.data.get((Bucket, Key))}
```

Reason for custom fake (not `moto`): Simpler, faster, and avoids external service simulation overhead.

### Patching task_io Constants

```python
def test_with_local_cache(monkeypatch):
    monkeypatch.setenv("CAMARA_CACHE_DIR", "/tmp/test_cache")
    # ... test with local destination
```

## Regression Tests

Tests for known production incidents have docstrings citing the incident:

```python
async def test_scheduled_2026_08_23_recovers_partial_failure():
    """
    Regression: scheduled__2026-08-23. A 503 in the middle of a large
    vote dataset caused the entire extraction to fail. Now we use
    gather_aligned with MIN_COVERAGE 0.95 and assert_usable to allow
    partial results.
    """
    ...
```

Use the incident code (e.g., `scheduled__2026-08-23`) in the docstring. This helps future developers understand why the test exists.

## No Network

- **No real HTTP calls:** Mock `aiohttp.ClientSession` or use `mock_client`.
- **No S3 calls:** Use `FakeS3` or monkeypatch.
- **No database:** Not used in the project.

All external dependencies are mocked.

## Coverage

Currently thin coverage:
- `deputados` (despesas, glossario extractors covered).
- `eventos` (events extractor).
- `grupos` (membros extractor).
- `proposicoes` (proposicoes, ids extractors).
- Clients and utils (task_io, bulk, concurrency, periods).

To add tests for other bundles, follow the same pattern: mock the client, call `extract()`, assert output shape.

## Running Tests

```bash
# All tests
pytest tests/ -v

# One file
pytest tests/unit/clients/test_camara_client.py -v

# One test
pytest tests/unit/extractors/deputados/test_despesas.py::test_despesas_with_multiple_years -v

# With coverage (requires pytest-cov; not in requirements-dev.txt)
pytest tests/ --cov=src --cov-report=html
```

Note: local `.venv` (Python 3.8) fails some bulk-client tests. Recreate on Python 3.11+ for accurate results.

## Fixtures

`conftest.py` provides:

- `mock_client`: AsyncMock of `AsyncCamaraClient`.

Other fixtures (e.g., `dest`, `FakeS3`) are defined inline in test files.

## Known Issues

- `test_camara_client.py` opens a real `aiohttp.ClientSession` but makes no request (safe, just wasteful).
- Local `.venv` on Python 3.8 has import issues with bulk-client tests; use 3.11+.
