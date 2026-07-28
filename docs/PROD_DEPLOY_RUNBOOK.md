# Runbook: Provision the production environment

This document is a manual checklist. **Nothing here runs automatically in CI** — the `deploy-prod` job in `.github/workflows/ci.yml` is ready in the workflow, but will fail early (at no cost) until the steps below are completed: first due to missing the `AWS_PROD_DEPLOY_ROLE_ARN` secret, then (if the role exists but resources don't) due to missing cluster/task definition/bucket.

Run the commands below with a user/role that already has admin permission in the AWS account `904464083417` (region `us-east-1`), reviewing each one before running.

## 1. OIDC: Allow GitHub Actions to assume roles in your account

Only needs to be done **once** per AWS account (serves for both dev and prod roles).

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

(The thumbprint above is the current root CA used by GitHub; if the command fails with an invalid thumbprint, get the updated value at https://github.blog/changelog — AWS also accepts validation via `--client-id-list` alone on newer accounts, without requiring the correct thumbprint.)

## 2. IAM Role for the `deploy-dev` job (if it doesn't already exist)

Trust policy restricted to the `develop` branch of this repository:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::904464083417:oidc-provider/token.actions.githubusercontent.com"
    },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
      },
      "StringLike": {
        "token.actions.githubusercontent.com:sub": "repo:DamodaraBarbosa/camara-senado-data-ingestion:ref:refs/heads/develop"
      }
    }
  }]
}
```

Minimum permission (push to ECR, repo `camara-ingestion`):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "ecr:GetAuthorizationToken"
    ],
    "Resource": "*"
  }, {
    "Effect": "Allow",
    "Action": [
      "ecr:BatchCheckLayerAvailability",
      "ecr:PutImage",
      "ecr:InitiateLayerUpload",
      "ecr:UploadLayerPart",
      "ecr:CompleteLayerUpload"
    ],
    "Resource": "arn:aws:ecr:us-east-1:904464083417:repository/camara-ingestion"
  }]
}
```

Create the role (`AWS_DEV_DEPLOY_ROLE_ARN`) and save the resulting ARN as a GitHub Secret (Settings → Secrets and variables → Actions → New repository secret).

## 3. IAM Role for the `deploy-prod` job

Same as step 2, but replacing `ref:refs/heads/develop` with `ref:refs/heads/main` in the trust policy. Save the ARN as the secret `AWS_PROD_DEPLOY_ROLE_ARN`.

## 4. Production ECS cluster

```bash
aws ecs create-cluster --cluster-name dataplatform-ecs-cluster-prod --region us-east-1
```

## 5. Production task definition

The current dev task definition (`dataplatform-ingestion-task-dev`) uses:
- `executionRoleArn`: `arn:aws:iam::904464083417:role/dataplatform_ecs_task_execution_role_dev`
- `taskRoleArn`: `arn:aws:iam::904464083417:role/dataplatform_airflow`
- `cpu: 1024` / `memory: 2048` (Fargate)

Decide whether production reuses the same IAM roles/size or wants dedicated roles (recommended for real isolation, but not required to start). Register the new task definition pointing to the `:prod` image:

```bash
cat > /tmp/task-def-prod.json <<'EOF'
{
  "family": "dataplatform-ingestion-task-prod",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "executionRoleArn": "arn:aws:iam::904464083417:role/dataplatform_ecs_task_execution_role_dev",
  "taskRoleArn": "arn:aws:iam::904464083417:role/dataplatform_airflow",
  "containerDefinitions": [{
    "name": "ingestion-container",
    "image": "904464083417.dkr.ecr.us-east-1.amazonaws.com/camara-ingestion:prod",
    "essential": true,
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group": "/ecs/dataplatform-ingestion-task-prod",
        "awslogs-region": "us-east-1",
        "awslogs-stream-prefix": "ecs"
      }
    }
  }]
}
EOF

aws logs create-log-group --log-group-name /ecs/dataplatform-ingestion-task-prod --region us-east-1
aws ecs register-task-definition --cli-input-json file:///tmp/task-def-prod.json --region us-east-1
```

> If you prefer roles isolated from dev, create `dataplatform_ecs_task_execution_role_prod` / `dataplatform_airflow_prod` (same policies as dev) and swap the ARNs above before registering.

## 6. Production S3 bucket

```bash
aws s3 mb s3://dataplatform-camara-prod-db --region us-east-1
```

## 7. GitHub: Secrets and Environment

- Settings → Secrets and variables → Actions: add `AWS_DEV_DEPLOY_ROLE_ARN` and `AWS_PROD_DEPLOY_ROLE_ARN` (the ARNs from steps 2 and 3).
- Settings → Environments → create `production`, marking **Required reviewers** with yourself (or whoever reviews deploys) — this makes the `deploy-prod` job pause and request manual approval before running, even after everything is provisioned.

## 8. Validate before going live

1. Confirm that `deploy-dev` (branch `develop`) already runs successfully publishing `:latest` + `:dev-<sha>` to ECR — this validates the role/OIDC/paths-filter before touching production.
2. Do a test merge to `main` and confirm that `deploy-prod` builds and publishes `:prod` + `:prod-<sha>` without error.
3. Only after that, in Airflow, manually unpause the DAG `camara_ingestion_pipeline_prod` (it is born paused, with no `schedule_interval`) and/or change its `schedule_interval` in `airflow/dags/camera_ingestion_dag.py` from `None` to `"@weekly"` when you want it to run automatically.
