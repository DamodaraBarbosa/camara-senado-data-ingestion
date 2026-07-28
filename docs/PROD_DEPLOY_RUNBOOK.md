# Runbook: provisionar o ambiente de produção

Este documento é uma checklist manual. **Nada aqui é executado automaticamente pelo CI** — o job `deploy-prod` em `.github/workflows/ci.yml` já está pronto no workflow, mas vai falhar cedo (sem custo) até os passos abaixo serem concluídos: primeiro por falta da secret `AWS_PROD_DEPLOY_ROLE_ARN`, depois (se a role existir mas os recursos não) por falta do cluster/task definition/bucket.

Rode os comandos abaixo com um usuário/role que já tenha permissão de administrador na conta AWS `904464083417` (região `us-east-1`), revisando cada um antes de rodar.

## 1. OIDC: permitir que o GitHub Actions assuma roles na sua conta

Só precisa ser feito **uma vez** por conta AWS (serve tanto para a role de dev quanto a de prod).

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

(O thumbprint acima é o atual da CA raiz usada pelo GitHub; se o comando falhar por thumbprint inválido, pegue o valor atualizado em https://github.blog/changelog — a AWS também aceita validar via `--client-id-list` sozinho em contas mais novas, sem exigir thumbprint correto.)

## 2. IAM Role para o job `deploy-dev` (se ainda não existir)

Trust policy restrita ao branch `develop` deste repositório:

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

Permissão mínima (push no ECR, repo `camara-ingestion`):

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

Crie a role (`AWS_DEV_DEPLOY_ROLE_ARN`) e salve o ARN resultante como GitHub Secret (Settings → Secrets and variables → Actions → New repository secret).

## 3. IAM Role para o job `deploy-prod`

Igual ao passo 2, mas trocando `ref:refs/heads/develop` por `ref:refs/heads/main` na trust policy. Salve o ARN como o secret `AWS_PROD_DEPLOY_ROLE_ARN`.

## 4. Cluster ECS de produção

```bash
aws ecs create-cluster --cluster-name dataplatform-ecs-cluster-prod --region us-east-1
```

## 5. Task definition de produção

A task definition dev atual (`dataplatform-ingestion-task-dev`) usa:
- `executionRoleArn`: `arn:aws:iam::904464083417:role/dataplatform_ecs_task_execution_role_dev`
- `taskRoleArn`: `arn:aws:iam::904464083417:role/dataplatform_airflow`
- `cpu: 1024` / `memory: 2048` (Fargate)

Decida se produção reusa essas mesmas IAM roles/tamanho ou se quer roles dedicadas (recomendado para isolamento real, mas não obrigatório para começar). Registre a nova task definition apontando pra imagem `:prod`:

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

> Se preferir roles isoladas de dev, crie `dataplatform_ecs_task_execution_role_prod` / `dataplatform_airflow_prod` (mesmas policies do dev) e troque os ARNs acima antes de registrar.

## 6. Bucket S3 de produção

```bash
aws s3 mb s3://dataplatform-camara-prod-db --region us-east-1
```

## 7. GitHub: secrets e Environment

- Settings → Secrets and variables → Actions: adicionar `AWS_DEV_DEPLOY_ROLE_ARN` e `AWS_PROD_DEPLOY_ROLE_ARN` (os ARNs dos passos 2 e 3).
- Settings → Environments → criar `production`, marcando **Required reviewers** com você mesmo (ou quem revisar deploys) — isso faz o job `deploy-prod` pausar e pedir aprovação manual antes de rodar, mesmo depois de tudo provisionado.

## 8. Validar antes de ativar de vez

1. Confirme que `deploy-dev` (branch `develop`) já roda com sucesso publicando `:latest` + `:dev-<sha>` no ECR — isso valida a role/OIDC/paths-filter antes de mexer em produção.
2. Faça um merge de teste em `main` e confirme que `deploy-prod` builda e publica `:prod` + `:prod-<sha>` sem erro.
3. Só depois disso, no Airflow, despause manualmente a DAG `camara_ingestion_pipeline_prod` (ela nasce pausada, sem `schedule_interval`) e/ou mude seu `schedule_interval` em `airflow/dags/camera_ingestion_dag.py` de `None` para `"@weekly"` quando quiser que rode automaticamente.
