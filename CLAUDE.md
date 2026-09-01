# CLAUDE.md — Code Patterns and Architecture

Reference document to optimize future work in this repository. Describes structure, conventions, and implemented patterns.

## Project Structure

```
.
├── src/                          # Core extractors and utilities (published to Docker image)
│   ├── clients/
│   │   ├── camara_client.py      # Async HTTP client with retry/rate-limit
│   │   └── camara_bulk_client.py # Cached bulk CSV downloads from S3
│   ├── extractors/camara/        # 10 bundles × N extractors each
│   │   └── {bundle}/{extractor}.py (e.g., deputados/despesas.py)
│   └── utils/
│       ├── task_io.py            # Canonical I/O: read deps, write output (local/S3)
│       ├── bulk.py               # CSV parsing helpers
│       └── periods.py            # Date/year range utilities
│
├── bundles/                      # Airflow integration layer (each bundle is a package)
│   └── {bundle}/app/
│       ├── runner.py             # Entry point for Airflow ECS tasks
│       └── events/               # Sample event payloads (optional)
│
├── airflow/
│   ├── dags/
│   │   ├── camera_ingestion_dag.py      # DAG factory (two instances: dev + prod)
│   │   └── config/
│   │       ├── bundles_config.dev.json  # Cluster/task-def for dev
│   │       └── bundles_config.prod.json # Cluster/task-def for prod (placeholder)
│   ├── docker-compose-airflow.yml       # Local Airflow (dev mode)
│   ├── docker-compose-airflow.prod.yml  # Airflow for the prod EC2 host (IAM role creds, no SSH)
│   └── Dockerfile                       # Custom Airflow image (amazon provider + [aiobotocore] baked in, pinned to Airflow 2.8.1 constraints)
│
├── .github/workflows/
│   └── ci.yml                   # GitHub Actions: lint, test, deploy-dev, deploy-prod
│
├── tests/unit/                  # pytest + pytest-asyncio (async-native)
│   ├── conftest.py
│   └── {category}/test_*.py
│
├── docs/
│   ├── PROD_DEPLOY_RUNBOOK.md       # Manual checklist for prod ingestion infra (ECS/S3/IAM)
│   └── PROD_AIRFLOW_EC2_RUNBOOK.md  # Manual checklist for the prod Airflow EC2 host
│
├── Dockerfile                   # Multi-stage build, env vars for BUNDLE override
├── Makefile                     # build, up, down, test, push-ecr, etc.
├── requirements.txt             # Runtime: aiohttp, tenacity, boto3
├── requirements-dev.txt         # Dev/CI: pytest, pytest-asyncio, flake8
└── pytest.ini                   # asyncio_mode=auto for async tests
```

## Code Patterns

### 1. Extractors (Asynchronous)

Todos os extractores herdam de `CamaraBaseExtractor`:

```python
# src/extractors/camara/{bundle}/{extractor}.py
class Async{Name}Extractor(CamaraBaseExtractor):
    ENDPOINT = "https://..."
    
    async def extract(self, **params) -> list | AsyncGenerator:
        """Return list of dicts OR async generator (for streaming)."""
        # Use self.client (AsyncCamaraClient) for HTTP
        # Use self.bulk (CamaraBulkClient) for cached CSV downloads
        # Implement rate-limit resilience (AsyncCamaraClient handles 429 + 503)
        pass
```

**Streaming pattern** (high-volume extractors, e.g., despesas, votacoesVotos):
```python
async def extract(self):
    for year in years:
        rows = await self.bulk.read_rows(self.DATASET, year)
        for row in rows:
            yield self._to_output_record(row)
```
— Yields one record at a time → `task_io.py`'s `write_output()` consumes via async generator → S3 multipart streaming, no OOM.

**List pattern** (small/medium datasets):
```python
async def extract(self):
    return [await self.client.get(...) for ...]
```

### 2. Async/Await Conventions

- `async def` for anything that calls `self.client` or `self.bulk`.
- `await asyncio.gather(...)` for parallel I/O (download multiple years concurrently).
- **Never** use `asyncio.create_task()` without supervision — always `gather()` or explicit cancel.
- **Sequential parsing after parallel I/O**: download all years in parallel, parse them sequentially (avoids memory spikes).
- `pytest-asyncio` auto-marks test functions with `async def` as async tests (via `asyncio_mode = auto` in pytest.ini).

### 3. Rate Limiting & Retries

`AsyncCamaraClient` (src/clients/camara_client.py) handles this:
- **Semaphore**: 5 concurrent requests by default (respects Câmara API limit of ~10 req/s).
- **Retry**: exponential backoff on 429 (rate limit) and 503 (service unavailable), ~8 attempts.
- **No manual retry logic needed** in extractors — client handles it.

### 4. Dependency Resolution & Caching

`task_io.py::read_dependency()` is the canonical way to read upstream outputs:

```python
# In a bundle's runner.py, resolving dependencies between extractors
if param_name not in resolved_params:
    cached = read_dependency(destination, bundle_name, dependency_name, run_id)
    if cached is not None:
        resolved_params[param_name] = cached
    else:
        # Recompute dependency if cache miss and STRICT_DEPENDENCY_CACHE=0
        dep_data = await EXTRACTORS[dependency_name](client).extract(...)
        resolved_params[param_name] = dep_data
```

**Cache key is always canonical**: `{bundle}/{name}_{run_id}.json` (never uses `destination["path"]` for cache).

### 5. I/O: Reading & Writing Output

**Writing** (`task_io.py::write_output()`):
- Accepts: list of dicts, sync generator, or async generator.
- Always writes canonical path: `{cache_dir}/{bundle}/{name}_{run_id}.json`.
- Optional explicit path: `destination["path"]` (for custom output location).
- **S3 write**: multipart upload, buffered (8 MB chunks), streams records row-by-row → no materialization of full JSON in memory.
- Returns: count of records written.

**Reading** (`task_io.py::read_dependency()`):
- Reads from canonical cache: `{bundle}/{name}_{run_id}.json`.
- Supports both local (filesystem) and S3 (via boto3).
- Raises `DependencyCacheMiss` if strict mode (`STRICT_DEPENDENCY_CACHE=1`, default) and file absent.

### 6. Naming Conventions

- **Extractor classes**: `Async{CapitalCase}Extractor` (e.g., `AsyncDeputadosExtractor`, `AsyncVotacoesIdsExtractor`).
- **Extractor files**: `{lowercase_extractor_name}.py` (e.g., `deputados.py`, `ids.py`).
- **Bundle directories**: `{lowercase_bundle_name}/` (e.g., `blocos/`, `deputados/`, `proposicoes/`).
- **Methods**: snake_case (e.g., `extract()`, `_to_output_record()`).
- **Logging**: use `print()` for now (logged to CloudWatch in Fargate; could migrate to `logging` module later).
  - Prefix pattern: `[{extractor}]`, `[cache]`, `[runner]`, `[ERROR]`.

### 7. Error Handling & Partial Extraction

Extractors can mark results as **partial** (incomplete due to timeout/budget):
```python
self.partial = True  # Set during extract() if stopping early
```

Runners check this and set status accordingly:
```python
status = "partial" if getattr(extractor_instance, "partial", False) else "success"
```

## Bundles & Runners (Airflow Integration)

Each bundle has a `bundles/{bundle}/app/runner.py`:

```python
# bundles/{bundle}/app/runner.py
def handler(event: dict, context=None):
    """Entry point for Airflow EcsRunTaskOperator."""
    try:
        return asyncio.run(asyncio.wait_for(_run(event), timeout=TIMEOUT))
    except asyncio.TimeoutError:
        raise TimeoutError(f"Extraction timeout exceeded {timeout} seconds") from None

async def _run(event: dict):
    # Extract from EVENT_PAYLOAD (set by EcsRunTaskOperator)
    extractor_name = event["extractor"]
    params = event.get("params", {})
    destination = event["destination"]  # {"type": "s3", "bucket": "...", "prefix": "..."}
    run_id = event["run_id"]  # Set by Airflow {{ run_id }}
    
    # Resolve dependencies (read cached upstream outputs or recompute)
    resolved_params = ...
    
    # Run extractor
    data = await EXTRACTORS[extractor_name](client).extract(**resolved_params)
    
    # Write output (canonical cache + optional explicit path)
    records = await write_output(data, destination, bundle_name, extractor_name, run_id)
    
    return {"extractor": extractor_name, "status": "success"/"partial", "records": records}
```

## Airflow DAG & Configuration

### Structure (New as of CI/CD refactor)

**Single DAG file with factory pattern** (`airflow/dags/camera_ingestion_dag.py`):
```python
def build_dag(dag_id: str, config_path: Path, s3_bucket: str, schedule_interval):
    """Build a DAG instance for a specific environment."""
    # Load bundles_config from config_path (env-specific cluster/task-def)
    # Create DAG with given schedule_interval and S3 bucket
    # Generate EcsRunTaskOperator for each (bundle, extractor) pair
    return dag

# Two DAG instances:
camara_ingestion_pipeline = build_dag(
    dag_id="camara_ingestion_pipeline",
    config_path="bundles_config.dev.json",
    s3_bucket="dataplatform-camara-dev-db",
    schedule_interval=None  # Manual only — dev is for ad hoc testing
)

camara_ingestion_pipeline_prod = build_dag(
    dag_id="camara_ingestion_pipeline_prod",
    config_path="bundles_config.prod.json",
    s3_bucket="dataplatform-camara-prod-db",
    schedule_interval="0 6 * * 0"  # Every Sunday 06:00 UTC
)
```

### Configuration Files

- `bundles_config.dev.json`: cluster = `dataplatform-ecs-cluster-dev`, task_definition = `dataplatform-ingestion-task-dev`.
- `bundles_config.prod.json`: cluster = `dataplatform-ecs-cluster-prod`, task_definition = `dataplatform-ingestion-task-prod` (placeholder, provisioned via runbook).

Each bundle config entry:
```json
{
  "blocos": {
    "cluster": "dataplatform-ecs-cluster-dev",
    "task_definition": "dataplatform-ingestion-task-dev",
    "sequence": [
      {"extractor": "blocos", "depends_on": []},
      {"extractor": "ids", "depends_on": ["blocos"]},
      ...
    ]
  }
}
```

## CI/CD Pipeline (GitHub Actions)

**Branches triggering CI**: `main`, `master`, `develop`.

**Jobs**:
1. **lint** (all branches): flake8 on `src/` (120 char max).
2. **test** (all branches): pytest on Python 3.10/3.11/3.12, async-native via pytest-asyncio.
3. **deploy-dev** (push to `develop` only): build Docker image, tag `:latest` + `:dev-<sha>`, push to ECR via OIDC (no long-lived keys).
4. **deploy-prod** (push to `main` only, requires `environment: production`): build, tag `:prod` + `:prod-<sha>`, push via OIDC.

**Important**:
- Both deploy jobs use `dorny/paths-filter@v3` to skip rebuild if only docs/config changed.
- OIDC role assumes via `secrets.AWS_DEV_DEPLOY_ROLE_ARN` / `AWS_PROD_DEPLOY_ROLE_ARN` (set up manually via runbook).
- No automatic DAG trigger after deploy — next scheduled/manual DAG run picks up new image.

## Development Workflow

### Local Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
make test      # Run all tests
make build     # Build Docker image locally
```

### Adding a New Extractor

1. Create `src/extractors/camara/{bundle}/{extractor}.py`:
   ```python
   from extractors.camara.base import CamaraBaseExtractor
   
   class Async{Name}Extractor(CamaraBaseExtractor):
       ENDPOINT = "https://..."
       
       async def extract(self, **params):
           # Implement using self.client (AsyncCamaraClient)
           return [...]  # or yield records for streaming
   ```

2. Register in `bundles/{bundle}/app/runner.py` EXTRACTORS dict.

3. Add to dependency graph in DEPENDENCIES if needed (e.g., `"ids": {"ids": "{name}"}`).

4. Run locally:
   ```bash
   PYTHONPATH=src python bundles/{bundle}/app/runner.py
   ```

5. Add tests in `tests/unit/extractors/{bundle}/test_{extractor}.py` (async tests auto-detected).

### Testing

```bash
# All tests
make test

# Specific module
PYTHONPATH=src pytest tests/unit/extractors/deputados/test_despesas.py -v

# With coverage (add pytest-cov to requirements-dev.txt if needed)
pytest --cov=src tests/
```

**Test async functions**: decorate with `async def`, pytest-asyncio auto-runs them:
```python
async def test_extract_returns_dict():
    extractor = AsyncMyExtractor(mock_client)
    result = await extractor.extract()
    assert isinstance(result, (list, AsyncGenerator))
```

### Docker & Image Build

```bash
# Local build
make build

# Push to ECR (requires AWS credentials + ECR login)
make push-ecr
# Or in CI via OIDC: `docker build -t ... && docker push ...`
```

### Git Commit Conventions

```
feat: {short description}
fix: {short description}
chore: {short description}
refactor: {short description}

{Longer explanation if needed, wrapped at 72 chars}

Co-Authored-By: Claude Haiku 4.5 <noreply@anthropic.com>
```

Examples (commits must be in English):
- `feat: add despesas extractor with multipart S3 streaming`
- `fix: increase timeout from 10m to 30m in despesas`
- `chore: remove unused requests dependency`

## Environment Variables & Configuration

### Runtime (set by Airflow EcsRunTaskOperator)

- `BUNDLE`: extractor bundle name (e.g., "deputados"). Overrides Dockerfile default.
- `EVENT_PAYLOAD`: JSON string with extractor, params, destination, run_id.
- `STRICT_DEPENDENCY_CACHE`: "0"/"false" to allow recompute on cache miss (default "1" = fail on miss).
- `CAMARA_CACHE_DIR`: local cache directory (default "/tmp").
- `S3_UPLOAD_PART_BYTES`: multipart chunk size in bytes (default 8 MB).

### Development (local)

```bash
export PYTHONPATH=src
export STRICT_DEPENDENCY_CACHE=1
export CAMARA_CACHE_DIR=/tmp/camara-cache
```

## Dependencies & Versions

**Runtime** (`requirements.txt`):
- `aiohttp>=3.8.0` — async HTTP client.
- `tenacity>=8.2.0` — retry/backoff logic.
- `boto3>=1.34.0` — AWS S3 client.

**Dev/Test** (`requirements-dev.txt`):
- `pytest==8.3.3` — test runner.
- `pytest-asyncio==0.24.0` — pytest plugin for async tests (auto-mode).
- `flake8==7.1.1` — linter (120 char max in this project).

## Common Tasks

### Merging a Feature Branch

1. Push to `origin/feature/...`.
2. Create PR (GitHub UI or `gh pr create`).
3. CI runs lint+test on the PR.
4. Merge to `develop` (or `main` if hotfix).
5. On merge to `develop`, CI auto-builds & pushes `:latest` + `:dev-<sha>` to ECR (if code changed).

### Deploying to Production

1. Merge `develop` into `main` (via PR).
2. CI runs lint+test.
3. `deploy-prod` job requires manual approval (GitHub environment `production`).
4. On approval, builds & pushes `:prod` + `:prod-<sha>`.
5. Manually trigger DAG `camara_ingestion_pipeline_prod` in Airflow (prod infra must exist first).

### Monitoring Extractions

- **Airflow UI (dev)**: http://localhost:8080 — local docker-compose, view DAG runs, task logs, retries.
- **Airflow UI (prod)**: the scheduler and triggerer run 24/7 on a dedicated EC2 instance (see `docs/PROD_AIRFLOW_EC2_RUNBOOK.md`) so the weekly schedule never depends on a local machine, but the webserver itself only runs on demand (`docker compose --profile ui up -d webserver`, reachable only via SSM Session Manager port-forwarding, no public inbound port) — running it 24/7 alongside the scheduler pegged this small instance's CPU.
- **Task logs**: `airflow/logs/dag_id=.../run_id=.../task_id=...` (dev: local filesystem; prod: on the EC2 instance's own volume).
- **CloudWatch**: `/ecs/dataplatform-ingestion-task-{dev,prod}` (prod/fargate).
- **S3 output**: `s3://dataplatform-camara-{dev,prod}-db/raw/{bundle}/{extractor}/` — files named `{extractor}_{run_id}.json`.

## Known Limitations & Future Improvements

1. **Logging**: Currently uses `print()` → CloudWatch. Could migrate to `logging` module + structured JSON logs.
2. **Linting**: Only flake8; no black/isort/mypy (could add to requirements-dev.txt later).
3. **Prod infrastructure**: ECS cluster, task definition, S3 bucket and IAM roles are provisioned and running real weekly ingestion (see `docs/PROD_DEPLOY_RUNBOOK.md`). The Airflow scheduler/webserver itself runs on a dedicated EC2 instance rather than MWAA (cost — MWAA bills a fixed hourly rate even when idle) — see `docs/PROD_AIRFLOW_EC2_RUNBOOK.md`. That instance runs a custom-built image (`airflow/Dockerfile`, pushed to the `camara-airflow` ECR repo) with `apache-airflow-providers-amazon` baked in — using `_PIP_ADDITIONAL_REQUIREMENTS` there (fine for local dev) caused a CPU-credit exhaustion crash loop on the small EC2 instance, since it reinstalls the package via pip on every container start. Even after that fix, running the webserver 24/7 alongside the scheduler still pegged the `t3.micro`'s 2 vCPUs (gunicorn's 4 workers plus frequent DAG re-parsing); the webserver is now on-demand only (`profiles: ["ui"]` in `docker-compose-airflow.prod.yml`) and the scheduler's parsing frequency/process count is tuned down — see the "Access the Airflow UI" step in `docs/PROD_AIRFLOW_EC2_RUNBOOK.md`. No infra-as-code yet; both runbooks are manual AWS CLI checklists.

   **Deferrable execution is what makes the DAG fit this instance.** `EcsRunTaskOperator` used to wait synchronously, so `LocalExecutor` held one forked Python subprocess (~150MB RSS) per task for the *entire* Fargate task duration just to poll `DescribeTasks` — `run_deputados_despesas` alone occupied a slot for 44 minutes. That forced `AIRFLOW__CORE__PARALLELISM: 2`, which stopped the OOM but roughly doubled wall clock (~100-110 min vs. 43-54 min with free parallelism). Prod now sets `AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE: 'true'` and runs a `triggerer` service: the fork submits `ecs:RunTask`, defers, and exits within seconds, and all the waits become coroutines in one asyncio event loop. Two consequences to remember when changing this: the `triggerer` service is **mandatory** whenever that flag is on (deferred tasks otherwise hang in `deferred` forever), and the image must install the amazon provider with its `[aiobotocore]` extra (`TaskDoneTrigger.run()` uses `async_conn`). The host also needs a swapfile — see the runbook's user-data step.

   **Measured, full production run on the EC2 host (2026-09-01):** 56/56 tasks succeeded in **42 minutes**, against a ~100-110min projection for the old synchronous operator at `PARALLELISM: 2`, and against 43-54min for the same DAG on a laptop with parallelism uncapped. Ten tasks sat in `deferred` concurrently while only two executor slots were in use — the point of the change in one number. Resident footprint was scheduler 270MB / triggerer 218MB / postgres 31MB (~518MB), peak swap 807MB of 2048MB, minimum free RAM 31MB, zero OOM kills. The DAG is now bound by `run_deputados_despesas` (~44min on Fargate) rather than by Airflow's fan-out, so raising `PARALLELISM` above 2 would buy nothing and there is no RAM to spend on it.
4. **Caching**: In-memory for extractors; could add distributed cache (Redis) for cross-task deps.
5. **Monitoring**: No metrics/traces yet; could integrate with DataDog/New Relic.

## Quick Reference: Troubleshooting

| Problem | Solution |
|---------|----------|
| Test fails with "event loop already running" | Use `pytest-asyncio`; ensure async tests have `async def test_...()`. |
| OOM during extraction | Use async generator + streaming (see `despesas.py` pattern). |
| Dependency cache miss | Set `STRICT_DEPENDENCY_CACHE=0` to allow recompute, or ensure upstream task succeeded. |
| Extractor times out | Increase runner's timeout override in `bundles/{bundle}/app/runner.py` (e.g., `_TIMEOUT_OVERRIDES = {"despesas": 1800}`). |
| Flake8 fails | Run `flake8 src/ --max-line-length=120` locally; fix unused imports, long lines. |
| ECR push fails in CI | Check `AWS_DEV_DEPLOY_ROLE_ARN` / `AWS_PROD_DEPLOY_ROLE_ARN` secrets; ensure OIDC role exists in AWS. |
| Tasks stuck in `deferred` forever | The `triggerer` service is not running. It is mandatory whenever `AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE` is `true`. |
| Deferred task fails immediately on an `aiobotocore` import | The image was built without the `[aiobotocore]` extra. Rebuild `airflow/Dockerfile` and redeploy (runbook step 9). |
| Tasks stay `running` for the whole Fargate duration | `AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE` did not take effect — check it is set in the compose env the container actually got. |
| A setting changed in `docker-compose-airflow.prod.yml` has no effect on prod | The DAG-sync cron only does `git pull`; containers keep the config they were created with. Recreate them (runbook step 9). This silently defeated the `PARALLELISM` cap for 10 days. |
| `airflow version` prints 3.x after an image rebuild | The pin/constraints in `airflow/Dockerfile` were bypassed. Installing the amazon provider unpinned upgrades Airflow across a major version and still exits 0. |
