---
name: bundles-runners
description: Runner patterns, extractor registration, timeout overrides, dependency maps
paths:
  - bundles/**/app/runner.py
  - bundles/**/events/*.json
---

## Runner Contract

Each of the 10 bundles has an `app/runner.py` (~160–196 lines). These are **near-duplicates** — a change to one pattern must be replicated in all 10.

### Module Signature

```python
# bundles/{bundle}/app/runner.py

EXTRACTORS = [
    Async{Name1}Extractor,
    Async{Name2}Extractor,
    ...
]

DEPENDENCIES = {
    "name1": None,              # No upstream dependency
    "name2": "name1",           # Depends on name1
    "despesas": "deputados",    # Example: despesas depends on deputados output
}

_TIMEOUT_OVERRIDES = {
    "despesas": 3600,           # 1 hour
    "frentes": 1800,            # 30 min
    # Default (for entries not here): 600s
}

async def _run(destination, bundle, name, run_id, ingestion_date):
    # Extract and write
    extractor = handler(name)
    records = await extractor.extract(...)
    write_output(records, destination, bundle, name, run_id, ingestion_date)

def handler(name):
    # Find the extractor class by name
    # Load dependencies if the extractor requires them
    # Return instantiated extractor
```

### Entry Point

```python
if __name__ == "__main__":
    asyncio.run(read_dependency(...))
```

The runner is invoked by Airflow as:

```bash
python /app/bundles/{bundle}/app/runner.py
```

Or locally:

```bash
PYTHONPATH=src python bundles/{bundle}/app/runner.py
```

The runner parses an event payload from stdin or env var (`EVENT_PAYLOAD`).

### Timeout Overrides

`_TIMEOUT_OVERRIDES` is a dict mapping extractor name → timeout in seconds. Default 600s. Prod config per bundle (e.g., deputados):

```python
_TIMEOUT_OVERRIDES = {
    "despesas": 3600,
    "frentes": 1800,
}
```

The runner uses this to set `os.environ` before calling the extractor:

```python
timeout = _TIMEOUT_OVERRIDES.get(name, 600)
os.environ.setdefault("CAMARA_TASK_BUDGET_S", max(60, timeout - 120))
```

### Dependency Injection

If an extractor depends on upstream output (e.g., `despesas` depends on `deputados`), the runner:

1. Reads the upstream extractor's output via `read_dependency()`.
2. Passes it to the extractor's `extract()` call.

Example:

```python
if "despesas" in dependencies and dependencies["despesas"]:
    upstream = read_dependency(destination, bundle, dependencies["despesas"], run_id, ingestion_date)
    records = await extractor.extract(deputados=upstream)
else:
    records = await extractor.extract()
```

## No __init__.py

Bundles are **namespace packages** (no `__init__.py`). Imports must be explicit, e.g.:

```python
from bundles.deputados.app.runner import EXTRACTORS
# NOT: from bundles.deputados import EXTRACTORS
```

## Sample Events

`bundles/{bundle}/events/{name}.json` holds sample payloads for local testing:

```json
{
    "destination": "local",
    "bundle": "deputados",
    "name": "deputados",
    "run_id": "20250915_120000",
    "ingestion_date": "2025-09-15"
}
```

Use with:

```bash
EVENT_PAYLOAD='...' python bundles/deputados/app/runner.py
```

Or:

```bash
cat bundles/deputados/events/deputados.json | python bundles/deputados/app/runner.py
```

## The 10 Bundles

1. **deputados** — Deputies, expenses (despesas), expenses glossary (glossario).
2. **votacoes** — Votes, vote details (votos).
3. **proposicoes** — Proposals, topic codes, historical indices (ids).
4. **frentes** — Parliamentary fronts, members.
5. **grupos** — Groups, members.
6. **eventos** — Events.
7. **blocos** — Blocs.
8. **legislaturas** — Legislative sessions.
9. **orgaos** — Organs/committees.
10. **partidos** — Parties.

Each has its own cluster/task-definition name in `bundles_config.{dev,prod}.json` (via Airflow DAG).

## Common Pattern: Duplicate Changes

When adding a new extractor or changing a pattern (e.g., updating retry logic, adding a new `_TIMEOUT_OVERRIDES` entry), you must edit **all 10** `runner.py` files. Git history will show this as 10 commits or 1 commit with 10 file changes. This is expected and not a problem.

Use:

```bash
# List all runners
find bundles -name "runner.py" -type f

# Edit each one
sed -i 's/old/new/g' bundles/*/app/runner.py
# Then review git diff to verify correctness
```

## Task Execution Context

Airflow invokes runners via ECS operator. The container environment includes:

- `BUNDLE={bundle}` (optional override).
- `PYTHONPATH=/app/src`.
- AWS credentials (via IAM role attached to ECS task).
- `CAMARA_ALERT_SNS_TOPIC_ARN` (for failure notifications).
- `CAMARA_CACHE_DIR`, `S3_UPLOAD_PART_BYTES`, rate-limit env vars, etc.

The runner should handle missing env vars gracefully (use defaults).
