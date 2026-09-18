# Câmara Dados Abertos Ingestion

**Weekly full-snapshot ingest of Câmara dos Deputados open data** into S3 `raw/` NDJSON layer via Airflow on ECS Fargate. **Note:** Despite the repo name, there is **no Senado code** here.

## Overview

65 async extractors across 10 bundles fetch from:
- Câmara v2 API (`https://dadosabertos.camara.leg.br/api/v2/`) — limited to 10 req/s per IP.
- Bulk CSV files from Câmara and CEAP cotas.

Data flows: API/CSV → Python extractors → **S3 raw layer** (NDJSON, partitioned by `ingestion_date`) → Glue metadata (via sibling `camara-senado-data-infra` repo) → dbt (future).

## Data Bundles (10)

1. **deputados** — Deputies, expenses (despesas), expense glossary (glossario).
2. **votacoes** — Votes, vote details (votos).
3. **proposicoes** — Proposals, topic codes, historical indices (ids).
4. **frentes** — Parliamentary fronts, members.
5. **grupos** — Groups, members.
6. **eventos** — Events.
7. **blocos** — Blocs.
8. **legislaturas** — Legislative sessions.
9. **órgãos** — Organs/committees.
10. **partidos** — Parties.

Each bundle has its own Airflow ECS task and timeout overrides.

## Prerequisites & Setup

- **Python 3.11+** (3.10+ for CI; local `.venv` on 3.8 fails bulk-client tests).
- **Docker** (for Airflow dev mode or ECR push).

### Local Development

```bash
# Create venv on Python 3.11+
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install dev dependencies for testing
pip install -r requirements-dev.txt
```

### Local Run (Single Extractor)

```bash
# Run one extractor locally
PYTHONPATH=src python bundles/deputados/app/runner.py < bundles/deputados/events/deputados.json

# Or set env var
export EVENT_PAYLOAD='{"bundle":"deputados","name":"deputados",...}'
PYTHONPATH=src python bundles/deputados/app/runner.py
```

## Project Structure

```
.
├── src/                              # Core extraction logic (Docker image)
│   ├── clients/
│   │   ├── camara_client.py         # AsyncCamaraClient (16 concurrent, 8 rps, circuit breaker)
│   │   └── camara_bulk_client.py    # Cached CSV downloads (TTL, flock)
│   ├── extractors/camara/
│   │   ├── base.py                  # CamaraBaseExtractor (shared base)
│   │   └── {bundle}/*.py            # 65 async extractors (Async{Name}Extractor)
│   └── utils/
│       ├── task_io.py               # read_dependency(), write_output() (NDJSON, S3)
│       ├── concurrency.py           # gather_aligned(), gather_with_coverage(), assert_usable()
│       ├── budget.py                # Deadline (task timeout enforcement)
│       ├── bulk.py                  # CSV parsing helpers (to_int, to_float, unflatten)
│       └── periods.py               # Date/year ranges
│
├── bundles/                          # 10 Airflow integration packages
│   └── {bundle}/app/
│       ├── runner.py                # Entry point (160–196 lines, near-duplicates)
│       └── events/                  # Sample event payloads
│
├── airflow/                          # Airflow orchestration
│   ├── dags/
│   │   ├── camera_ingestion_dag.py  # DAG factory (typo "camera" is load-bearing)
│   │   └── config/
│   │       ├── bundles_config.dev.json
│   │       └── bundles_config.prod.json
│   ├── Dockerfile                   # Custom Airflow 2.8.1 image
│   ├── docker-compose-airflow.yml   # Dev Airflow
│   └── docker-compose-airflow.prod.yml  # Prod Airflow (on EC2 host)
│
├── .github/workflows/
│   └── ci.yml                       # Lint, test, deploy-dev, deploy-prod
│
├── tests/unit/                      # pytest + pytest-asyncio
│   ├── clients/
│   ├── extractors/
│   └── utils/
│
├── docs/
│   ├── PROD_DEPLOY_RUNBOOK.md       # ECS/IAM setup checklist
│   └── PROD_AIRFLOW_EC2_RUNBOOK.md  # EC2 host bootstrap checklist
│
├── Dockerfile                       # Multi-stage Python 3.11 build
├── Makefile                         # build, test, push-ecr, etc.
├── requirements.txt                 # Runtime: aiohttp, tenacity, boto3
├── requirements-dev.txt             # Dev/CI: pytest, pytest-asyncio, flake8
└── pytest.ini                       # asyncio_mode=auto
```

## Dependencies

**Runtime** (`requirements.txt`):
- `aiohttp >= 3.8.0` — Async HTTP client.
- `tenacity >= 8.2.0` — Retry with backoff.
- `boto3 >= 1.34.0` — AWS SDK (S3).

**Dev/CI** (`requirements-dev.txt`):
- `pytest==8.3.3` — Test framework.
- `pytest-asyncio==0.24.0` — Async test support.
- `flake8==7.1.1` — Linter (120 char max).

## Rate Limiting & Resilience

**AsyncCamaraClient** (shared via `self.client` in extractors):

| Component | Config |
|---|---|
| **Concurrency** | 16 concurrent requests (`CAMARA_MAX_CONCURRENCY`) |
| **Rate Limit** | 8 rps token bucket (`CAMARA_RATE_LIMIT_RPS`) |
| **Retries** | 5 for 429 (too many requests); 3 for 5xx; jitter 1–8s |
| **Circuit Breaker** | Pauses on 429 or 5xx storms |
| **404 Response** | Returns `{}` (not an error) |

**Do not add custom retry or rate-limit logic in extractors.** Use the shared client.

## Testing

```bash
# All tests
pytest tests/ -v

# Specific test
pytest tests/unit/extractors/deputados/test_despesas.py -v

# With coverage (requires pytest-cov)
pytest tests/ --cov=src --cov-report=html
```

**Layout:** `tests/unit/{clients,extractors/<bundle>,utils}/`
**Mocking:** `mock_client` fixture (AsyncMock), `FakeS3` (custom dict-based mock, no moto).
**Note:** No network calls; all externals are mocked.

## Linting

```bash
# Lint src/ only (not tests, bundles, airflow)
flake8 src/ --max-line-length=120
```

CI runs lint on Python 3.12 only.

## Local Extractors (Legacy)

### Async Extractors (Use These)

All extractors inherit from `CamaraBaseExtractor` and are `Async{Name}Extractor`. Return a list or async generator:

```python
class AsyncDeputadosExtractor(CamaraBaseExtractor):
    ENDPOINT = "deputados"  # Relative to API base
    
    async def extract(self, **params):
        return [...]  # or: async generator
```

Usage:

```python
from src.extractors.camara.deputados.deputados import AsyncDeputadosExtractor
import aiohttp
import asyncio

async def main():
    async with aiohttp.ClientSession() as session:
        from src.clients.camara_client import AsyncCamaraClient
        client = AsyncCamaraClient(session)
        extractor = AsyncDeputadosExtractor(client)
        results = await extractor.extract()
        print(results)

asyncio.run(main())
```
- `AsyncVotacoesOrientacoes` - Voting orientations
- `AsyncVotosExtractor` - Individual votes

### Synchronous Extractors
These use traditional synchronous requests:
- `DeputadosExtractor` - Representative information
- `IdsExtractor` (deputados) - Detailed representative data
- `DespesasExtractor` - Representative expenses
- `DiscursosExtractor` - Speeches
- `EventosExtractor` - Events
- `FrentesExtractor` - Front memberships
- `HistoricoExtractor` - Historical data
- `OcupacoesExtractor` - Occupations
- `ProposicoesExtractor` - Bills and propositions
- `VotacoesExtractor` (proposicoes) - Proposition votes

### Reference Extractors
These fetch static reference data:
- `CodigoSituacaoExtractor` - Status codes
- `CodigoTemaExtractor` - Topic codes
- `CodigoTipoAutorExtractor` - Author type codes
- `CodigoTipoTramitacaoExtractor` - Processing type codes
- `SiglaTipoExtractor` - Type abbreviations
- `SituacoesProposicaoExtractor` - Proposition statuses
- `TiposAutorExtractor` - Author types
- `TiposProposicaoExtractor` - Proposition types
- `TiposTramitacaoExtractor` - Processing types
- `SituacoesOrgaoExtractor` - Committee statuses
- `CodigoSituacaoOrgaoExtractor` - Committee status codes

## CI/CD & Deployment

### GitHub Actions Workflow (`.github/workflows/ci.yml`)

| Job | Trigger | Steps |
|---|---|---|
| `lint` | Any | flake8 src/ (Python 3.12) |
| `test` | Any | pytest (Python 3.10, 3.11, 3.12) |
| `deploy-dev` | Push/PR to `develop` | Build image, push `:latest` and `:dev-<sha8>` |
| `deploy-prod` | Merge to `main` | Build image, push `:prod` and `:prod-<sha8>` (approval required) |

**Paths filter (deploy trigger):** `bundles/** src/** Dockerfile requirements.txt` (DAG-only changes do NOT trigger rebuild).

### Branch & Deployment

1. **Feature branch** → PR to `develop` (lint/test, no deploy).
2. **Merge to `develop`** → Auto-deploy dev (CI pushes ECR image).
3. **PR `develop → main`** → Lint/test (no deploy).
4. **Merge to `main`** (with approval) → Auto-deploy prod.

**Note:** Merges use GitHub merge commits (not squash). `main` is ~12 commits behind `develop` (both have identical trees).

## Airflow Orchestration

### DAG Configuration

**Dev:** `camara_ingestion_pipeline` (on-demand, no schedule).
**Prod:** `camara_ingestion_pipeline_prod` (weekly, Sunday 06:00 UTC).

**Settings:**
- Task timeout: 110 minutes total.
- Parallelism: 2 concurrent tasks (cluster cap).
- Retries: 1, delay 5 min.
- Failure callback: SNS notification (`CAMARA_ALERT_SNS_TOPIC_ARN`).
- Deferrable: Yes (requires `triggerer` service).

**Task IDs:** `run_{bundle}_{extractor}` (56 tasks total, same for dev and prod).

### Runners (10 Bundles)

Each bundle has `bundles/{bundle}/app/runner.py` — **10 near-duplicates** (a pattern change must be applied to all 10). Signature:

```python
async def _run(destination, bundle, name, run_id, ingestion_date):
    extractor = handler(name)
    records = await extractor.extract(...)
    write_output(records, destination, bundle, name, run_id, ingestion_date)
```

**Registries:** `EXTRACTORS` list, `DEPENDENCIES` map (for upstream injection), `_TIMEOUT_OVERRIDES` dict (default 600s).

## S3 Output Format

```
s3://dataplatform-camara-{dev|prod}-db/
  raw/{bundle}/{name}/
    ingestion_date=YYYY-MM-DD/{name}_{run_id}.json
```

- **Format:** NDJSON (one JSON object per line).
- **Partition:** `ingestion_date=YYYY-MM-DD` (Hive-style for Glue).
- **Run ID:** Airflow execution timestamp (`YYYYMMDD_HHMMSS`).
- **Atomicity:** Writes to `.part` then `os.replace()`.
- **Empty guard:** Raises `EmptyExtractionError` if no records; skips write.

## Runbooks

See `docs/`:

- **`PROD_DEPLOY_RUNBOOK.md`** — ECS cluster, task definition, S3 bucket, IAM roles, GitHub secrets and environment setup.
- **`PROD_AIRFLOW_EC2_RUNBOOK.md`** — EC2 host bootstrap (swap, cron sync, user-data), IAM instance role, security group, SSM access, on-demand Airflow UI, `.env` config.

## Conventions & Code Patterns

See `CLAUDE.md` for an overview. See `.claude/rules/` for scoped conventions:

- **extractors.md** — Async pattern, rate-limit resilience, data shape.
- **clients-and-utils.md** — HTTP client, bulk downloads, I/O patterns.
- **bundles-runners.md** — Runner contract, timeout overrides, dependency injection.
- **airflow-and-ci.md** — DAG factory, deferrable execution, CI workflow.
- **tests.md** — Test layout, pytest config, mocking patterns.
- **sensitive-and-legacy.md** — Public repo caveats, tracked `.env.docker`, legacy code (broken `run_local_pipeline.py`, root compose, `.vscode/settings.json`).

## Related Repository

**`camara-senado-data-infra`** (Terraform)

Provisions AWS infrastructure (buckets, Glue, ECR, ECS cluster, IAM, Lambda, alarms). This ingestion project publishes Docker images to the ECR repo created there. See the infra repo for cloud setup and terraform conventions.

## Known Issues & Technical Debt

- **Local `.venv` on Python 3.8:** Fails bulk-client tests; recreate on 3.11+.
- **Compose version:** `3.3` (obsolete); upgrade deferred.
- **Tracked `.env.docker`:** Despite `.gitignore`; placeholder values (safe). Cleanup deferred.
- **Broken `scripts/run_local_pipeline.py`:** References missing `bundles_config.json`; archive or remove.
- **Stale `.vscode/settings.json`:** Auto-approve entry on wrong machine; update or remove.
- **Root `docker-compose.yml`:** Postgres unused; legacy setup (prefer dev DAG or local runner).

## Git & Commits

**Conventional Commits** in English: `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`. Bodies explain the *why* and cite measured evidence. Trailer: `Co-Authored-By: Claude Haiku 4.5 <noreply@anthropic.com>`.

---

*Maintained by the Data Engineering Team.*
