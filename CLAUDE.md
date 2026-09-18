# CLAUDE.md — Camara Dados Abertos Ingestion

## Overview

Weekly full-snapshot ingest of Câmara dos Deputados open data into S3 `raw/` layer via Airflow on ECS Fargate. 65 async extractors across 10 bundles fetch from v2 API (10 req/s limit), bulk CSV files, and CEAP, writing NDJSON to S3. **No Senado code exists despite the repo name.**

Data flows: API/CSV → Python extractors → S3 NDJSON raw layer (`raw/{bundle}/{name}/ingestion_date=YYYY-MM-DD/{name}_{run_id}.json`) → Glue metadata (in sibling `camara-senado-data-infra` repo) → dbt (future).

## Repository Layout

- **`src/`**: Core extraction logic (published to Docker image).
  - `clients/camara_client.py`: `AsyncCamaraClient` (16 concurrent, 8 rps token bucket, global `CircuitBreaker` on 429/5xx).
  - `clients/camara_bulk_client.py`: Cached CSV downloads (TTL, flock, cooperative parse deadline).
  - `extractors/camara/{bundle}/{extractor}.py`: 65 async extractors (`Async{Name}Extractor`), inheriting `CamaraBaseExtractor`.
  - `utils/task_io.py`: `read_dependency()`, `write_output()` (NDJSON, `.part` + `os.replace`, empty-guard, `ingestion_date`).
  - `utils/concurrency.py`, `budget.py`, `bulk.py`, `periods.py`: async patterns, timeouts, CSV parsing.

- **`bundles/`**: 10 Airflow integration packages (one per data domain: deputados, votacoes, proposicoes, etc.).
  - Each has `app/runner.py` (160–196 lines): entry point, `EXTRACTORS` list, `DEPENDENCIES` map, `_TIMEOUT_OVERRIDES`, `handler/_run()`.
  - Sample events in `events/*.json` (shape: `{destination, bundle, name, ...}`).

- **`airflow/`**: Airflow DAG factory + custom image.
  - `dags/camera_ingestion_dag.py` (typo in name is load-bearing): DAG factory, task ids `run_{bundle}_{extractor}`, deferrable + triggerer, parallelism 2, execution_timeout 110 min, SNS failure callback (`CAMARA_ALERT_SNS_TOPIC_ARN`).
  - `config/bundles_config.{dev,prod}.json`: Cluster and ECS task def names.
  - `Dockerfile`: Python 3.11, amazon provider 8.16.0 with `[aiobotocore]`, Airflow 2.8.1.

- **`.github/workflows/ci.yml`**: Lint, test, deploy-dev, deploy-prod (paths-filter: `bundles/** src/** Dockerfile requirements.txt`).

- **`tests/unit/`**: Layout `{clients,extractors/<bundle>,utils}/`, asyncio_mode=auto, `mock_client`, `FakeS3`, no network, regression docstrings.

- **`docs/`**: `PROD_DEPLOY_RUNBOOK.md`, `PROD_AIRFLOW_EC2_RUNBOOK.md` (manual checklists for ECS/IAM, host bootstrap, cron sync).

## Core Patterns

### Extractors

```python
class AsyncDeputadosExtractor(CamaraBaseExtractor):
    async def extract(self, **params):
        return [...]  # or async generator for streaming
```

- No own retry/semaphore (client handles rate-limit via `RateLimiter` + `CircuitBreaker`).
- Fan-out via `gather_aligned()` + `assert_usable()` with `MIN_COVERAGE` 0.95, `Deadline` budget.
- Return camelCase API fields + injected FKs (`idLegislatura`, etc.).
- Bulk extractors: CSV transforms via `utils.bulk` helpers (`to_int`, `to_float`, `intern_str`, `unflatten`).

### Rate Limiting

- **RateLimiter**: 8 rps (not 5; not 10).
- **Concurrency**: 16 concurrent (not 5).
- **Retries**: 5 for 429, 3 for other errors (500, 502, 503, 504), jitter 1–8s.
- **404 → `{}`.** **Circuit breaker:** pauses on 5xx storms.

### Runners (bundles/)

10 near-duplicates; **a change must be applied to all 10**. Signature:

```python
def _run(destination, bundle, name, run_id, ingestion_date):
    ...
```

- `EXTRACTORS`: list of extractor classes.
- `DEPENDENCIES`: map `{name: upstream_name}` (e.g., `despesas: deputados`).
- `_TIMEOUT_OVERRIDES`: dict, default 600s, per-extractor overrides (e.g., `despesas: 3600`).

### DAG

`camera_ingestion_dag.py` (prod: `schedule="0 6 * * 0"` Sunday 06:00 UTC; dev: `None`). Features:

- Task ids: `run_{bundle}_{extractor}`.
- Deferrable: yes (needs `triggerer` service).
- Parallelism: 2 (concurrency cap).
- Execution timeout: 110 min.
- On-failure: SNS notification via `CAMARA_ALERT_SNS_TOPIC_ARN`.
- Container name: `ingestion-container` (hardcoded in ECS operator).

## Commands

```bash
# Local run (Python 3.11+)
PYTHONPATH=src python bundles/{bundle}/app/runner.py

# Tests
pytest tests/ -v   # or: make test

# Lint
flake8 src/ --max-line-length=120

# Docker (legacy compose)
make build-no-cache && make up
```

## Environment Variables

Runtime: `CAMARA_RATE_LIMIT_RPS`, `CAMARA_MAX_CONCURRENCY`, `CAMARA_TASK_BUDGET_S`, `CAMARA_MIN_COVERAGE`, `CAMARA_BULK_CACHE`, `CAMARA_ALERT_SNS_TOPIC_ARN`, others.

Never print `.env*` or `~/.aws/credentials`. The `.env.docker` file is tracked (placeholder values only).

## Git & Commits

**Conventional Commits** in English: `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`. Bodies cite measured evidence or production incidents. Trailer: `Co-Authored-By: Claude Haiku 4.5 <noreply@anthropic.com>`.

Branch flow: feature → PR to `develop` (lint/test) → merge (auto-deploy dev) → PR `develop→main` (approval) → merge (auto-deploy prod).

---

For scoped conventions per file pattern, see `.claude/rules/`. For runbooks, see `docs/`.
