---
name: airflow-and-ci
description: DAG factory, deferrable execution, ECS integration, CI/CD workflow
paths:
  - airflow/**
  - .github/workflows/**
  - airflow/dags/**
---

## DAG Factory (camera_ingestion_dag.py)

The file name has a typo ("camera" instead of "camara") but is load-bearing; do not rename.

### Instances

- **Dev DAG:** `camara_ingestion_pipeline` (on-demand, `schedule_interval=None`).
- **Prod DAG:** `camara_ingestion_pipeline_prod` (weekly, `schedule="0 6 * * 0"` — Sunday 06:00 UTC).

### DAG Configuration

```python
default_args = {
    "execution_timeout": timedelta(minutes=110),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,  # SNS notification
}

dag_config = {
    "bundles_config": {...},
    "catchup": False,
    "max_active_runs": 1,
    "tags": ["camara", "data_ingestion", "fargate"],
}
```

- **Execution timeout:** 110 minutes (total time limit for all tasks).
- **Retries:** 1 (on transient failure).
- **Failure callback:** Posts to SNS topic `CAMARA_ALERT_SNS_TOPIC_ARN`.
- **Deferrable:** Yes (requires Airflow `triggerer` service).

### Task IDs

Task id pattern: `run_{bundle}_{extractor}`.

Example: `run_deputados_deputados`, `run_deputados_despesas`, `run_votacoes_votos`.

### Deferrable Execution

Airflow config requires `AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE: 'true'` to enable. The `EcsRunTaskOperator` defers execution and resumes when the task completes (vs. blocking).

Deferrable tasks require a `triggerer` Airflow service (subprocess or separate container).

### Parallelism

```python
AIRFLOW__CORE__PARALLELISM: 2
```

Max 2 tasks run concurrently (not per DAG, but across all DAGs). This is conservative to avoid overwhelming the ECS cluster. Adjust if capacity increases.

### ECS Integration

Tasks are invoked via `EcsRunTaskOperator` on Fargate:

```python
EcsRunTaskOperator(
    task_id=f"run_{bundle}_{name}",
    cluster="dataplatform-ecs-cluster-{env}",
    task_definition="ingestion-task-def-{env}:latest",
    container_name="ingestion-container",  # Hardcoded
    command=["python", f"/app/bundles/{bundle}/app/runner.py"],
    network_configuration=NETWORK_CONFIGURATION,  # Hardcoded subnets, security group
    overrides={...},  # Task timeout
)
```

Container name is always `ingestion-container` (defined in ECS task definition in `camara-senado-data-infra`).

### Config Shape

```json
{
    "bundles_config": {
        "deputados": {
            "cluster": "dataplatform-ecs-cluster-dev",
            "task_definition": "ingestion-task-def-dev",
            "sequence": ["deputados", "despesas", ...]
        },
        "votacoes": {...},
        ...
    }
}
```

Two configs: `bundles_config.dev.json` and `bundles_config.prod.json` in `airflow/dags/config/`.

## Airflow Infrastructure

### Custom Image

`airflow/Dockerfile` builds a custom Airflow image:

- Base: python:3.11-slim.
- **Airflow 2.8.1** (pinned in requirements).
- **amazon provider 8.16.0** with `[aiobotocore]` extra (for async support in ECS operator).

### Compose (Development)

**Root compose** (`docker-compose.yml`): Legacy, Postgres + 10 extractor services. Not recommended; use dev DAG instead.

**Airflow compose** (`airflow/docker-compose-airflow.yml`): Local Airflow (dev mode).

**Prod compose** (`airflow/docker-compose-airflow.prod.yml`): Used on the prod EC2 host (IAM role creds, no SSH key).

Compose version `3.3` is obsolete (v1 syntax); update is deferred. Compose changes require `--force-recreate` on the host.

### Entry Point

`scripts/entry.sh`: Bundle validator for the docker-compose path. Lists valid bundles and checks the `BUNDLE` env var.

## CI/CD Workflow

`.github/workflows/ci.yml` is the only workflow.

### Jobs

| Job | Trigger | Matrix | Lint | Test | Deploy |
|---|---|---|---|---|---|
| `lint` | Any | Python 3.12 | `flake8 src/ --max-line-length=120` | — | — |
| `test` | Any | Python 3.10, 3.11, 3.12 | — | `pytest tests/ -v` | — |
| `deploy-dev` | Push/PR to `develop` | — | — | — | If filtered paths, build image + push `:latest`, `:dev-<sha8>` |
| `deploy-prod` | Push/PR to `main` | — | — | — | If filtered paths, build image + push `:prod`, `:prod-<sha8>` (needs approval) |

### Paths Filter

`dorny/paths-filter` with `base: github.event.before`:

```yaml
paths: |
  bundles/**
  src/**
  Dockerfile
  requirements.txt
```

Only these path changes trigger deploy. DAG-only changes (`airflow/dags/**`) do NOT trigger a rebuild (DAGs sync via `git pull` cron on the host).

### Deploy Behavior

- **Dev:** Push to `develop` → ECR `:latest` and `:dev-<sha8>`.
- **Prod:** PR merge to `main` with approval → ECR `:prod` and `:prod-<sha8>`.
- OIDC auth (no static keys).

**Not in CI:**
- No `terraform` (lives in sibling `camara-senado-data-infra`).
- No linting of `tests/`, `bundles/`, `airflow/`, `scripts/` (only `src/`).

## Branch & Deployment Strategy

1. **Feature branch** (e.g., `feature/new-extractor`): PR to `develop`.
2. **CI on PR:** Lint + test (no deploy).
3. **Merge to `develop`:** `deploy-dev` job runs (if paths matched).
4. **PR `develop → main`:** Lint + test (no deploy, not the target branch).
5. **Merge to `main`:** `deploy-prod` job runs (if paths matched, plus approval gate).

**Note:** `origin/main` is ~12 commits behind `origin/develop` (merge commits from PR squashes). They are structurally identical (same extractors, DAGs).

## Commit Style

**Conventional Commits** in English: `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`. Bodies explain the *why* and cite measured evidence or production incidents. Trailer: `Co-Authored-By: Claude Haiku 4.5 <noreply@anthropic.com>`.

Merge commits: `Merge pull request #N from DamodaraBarbosa/...`

## Commands

```bash
# Local DAG test
PYTHONPATH=src python -c "from airflow.models import DagBag; DagBag('airflow/dags')"

# Docker build and push (manual, not in CI)
make build-no-cache
make push-ecr

# Local Airflow (dev mode)
docker-compose -f airflow/docker-compose-airflow.yml up
# Webserver at http://localhost:8080 (airflow/airflow login, no password)
```
