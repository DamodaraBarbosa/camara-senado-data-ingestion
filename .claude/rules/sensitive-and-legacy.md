---
name: sensitive-and-legacy
description: Public repo sensitivity, tracked secrets, legacy code, stale config
paths:
  - Dockerfile
  - docker-compose*.yml
  - .env*
  - Makefile
  - scripts/**
  - .vscode/**
  - src/main.py
---

## Public Repository

**This repo is public** (stated in commit `6491f3a`). Never commit:

- Real AWS credentials (`AKIA*` patterns).
- Private keys or certificates.
- Real account IDs, subnet IDs, or security group IDs in code/docs.

Use placeholders like `<AWS_ACCOUNT_ID>`, `<SECURITY_GROUP_ID>` in new documentation.

## .env.docker (Tracked!)

⚠️ `.env.docker` is **tracked in git** (despite being listed in `.gitignore` in commits `1a5e43f` and `4bd2a12`). Its values are **local dev and LocalStack placeholders** (no real credentials):

- `AWS_ACCESS_KEY_ID=test`
- `AWS_SECRET_ACCESS_KEY=test`
- `AWS_ENDPOINT_URL_S3=http://localstack:4566`
- `S3_BUCKET=local_test_bucket`

**Never print or echo it.** List variable names only when documenting.

**Future improvement:** `git rm --cached .env.docker` should be done in a separate, clearly-marked commit.

## docker-compose.yml (Root, Legacy)

The root `docker-compose.yml` is a legacy setup:

- Spins up Postgres 15 (unused; no code connects to it).
- Defines 10 `extractor-*` services (don't match current runner pattern).
- Hard-codes a `POSTGRES_PASSWORD`.

**Status:** Not recommended. Use dev DAG or local `PYTHONPATH=src python` invocations instead.

Future: Consider removing or marking as deprecated.

## docker-compose-airflow.yml (Airflow, Development)

Local Airflow dev setup. Services:

- `postgres`: Airflow metadata store.
- `airflow-webserver`, `airflow-scheduler`, `airflow-triggerer`.

Used for local DAG testing. Compose version `3.3` is obsolete; upgrade is deferred.

## docker-compose-airflow.prod.yml (Airflow, Production)

Used on the prod EC2 host. **Not version-controlled.** See `docs/PROD_AIRFLOW_EC2_RUNBOOK.md` for setup details.

Changes to this file are manual (edit on host, run `docker-compose --force-recreate` to apply).

## Dockerfile (Root)

Multi-stage build, Python 3.11, entry point is `scripts/entry.sh` (bundle validator). Reasonable; no changes needed.

## Makefile

Hard-coded AWS account ID:

```makefile
AWS_ACCOUNT_ID ?= 904464083417
AWS_REGION ?= us-east-1
```

This is the dev account ID. Prod is not mentioned (you push to `camara-ingestion` ECR via OIDC in CI).

Targets:

- `build`, `build-no-cache`, `up`, `down`, `logs`, `ps`, `run-<bundle>` (10).
- `db-shell` (connects to Postgres, unused).
- `test`, `login-ecr`, `push-ecr`, `clean`, `clean-all`, `prune`, `info`.

No lint or format target. Uses `docker-compose` (v1) binary.

## scripts/ (Legacy & Broken)

### entry.sh

Bundle validator (no issues).

### run_local_pipeline.py

507 lines, **broken.** It references `bundles_config.json` (doesn't exist). Only `bundles_config.{dev,prod}.json` exist (in `airflow/dags/config/`). The timeout and semaphore numbers are also stale (650s timeout, semaphore of 15).

**Status:** Archive or remove. Do not use.

### compare_bulk_vs_api.py

Dev-only diff tool. Useful for debugging bulk vs. live API differences. No issues.

## .vscode/settings.json

Auto-approve entry with a stale path (`/home/damodara_barbosa/...` on a different machine). Tracked but obsolete.

**Improve:** Update or remove.

## src/main.py

A leftover demo script. Not used anywhere. Safe to delete if cleanup is desired.

## .env.docker Tracked Despite .gitignore

The `.gitignore` has `.env.docker` but the file is committed. This is a **tracked violation** of the ignore rule. The values are safe (placeholders), but the pattern should be cleaned up.

**Recommendation:** Add `git rm --cached .env.docker` in a future housekeeping commit, with a clear message explaining the change.

## Sensitive Untracked Files (Never Commit)

Outside git (untracked, protected by `.gitignore`):

- `.env` (real AWS credentials when present).
- `~/.aws/credentials` (mounted read-only in local Airflow).
- `airflow/.env` (host-only, holds `POSTGRES_PASSWORD` and `AIRFLOW__CORE__FERNET_KEY`).

Never try to print or commit these.

## AWS Account ID & Infrastructure IDs

Committed in tracked files:

- `Makefile`: `904464083417` (dev account).
- `docs/PROD_DEPLOY_RUNBOOK.md`, `docs/PROD_AIRFLOW_EC2_RUNBOOK.md`: account ID, subnet IDs, security group IDs.
- `airflow/docker-compose-airflow.prod.yml` (not tracked; on host).
- DAG hardcoded defaults (not printed here to protect public repo, but noted in `docs/`).

**Rule:** Use `<PLACEHOLDER>` format in new documentation; do not spread these IDs beyond what's already committed.

## Public Repo Caveats

- Issues, PRs, and discussions are public.
- Anyone can fork and run the code (with their own AWS account).
- Do not assume privacy of any content in the repo.
- Redirect sensitive discussions to private channels (Slack, email).

## Legacy Tech Debt

1. **Python 3.8 local `.venv`**: Fails bulk-client tests. Recreate on 3.11+.
2. **Compose version `3.3`**: Obsolete. Upgrade deferred.
3. **Stale `.vscode/settings.json`**: Auto-approve entry on wrong machine.
4. **Tracked `.env.docker` despite `.gitignore`**: Placeholder values (safe); cleanup deferred.
5. **Broken `run_local_pipeline.py`**: Archive or remove.
6. **Root `docker-compose.yml`**: Legacy, Postgres unused.

None of these are blocking; they are acknowledged as technical debt.
