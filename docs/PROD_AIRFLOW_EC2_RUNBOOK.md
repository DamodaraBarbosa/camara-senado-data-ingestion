# Runbook: Airflow production host (EC2)

This document is a manual checklist, same style as `docs/PROD_DEPLOY_RUNBOOK.md`. **Nothing here runs automatically in CI.** It provisions a single small, always-on EC2 instance to run the Airflow scheduler/webserver for production, so the weekly DAG (`camara_ingestion_pipeline_prod`, `0 6 * * 0`) no longer depends on a local machine staying on. MWAA was ruled out: it bills a fixed hourly rate for the environment even when idle, which doesn't make sense for a single weekly trigger.

Run the commands below with a user/role that has admin permission in AWS account `904464083417` (region `us-east-1`), reviewing each one before running. Assumes `docs/PROD_DEPLOY_RUNBOOK.md` has already been completed (ECS cluster/task definitions/S3 bucket for prod already exist).

## 1. IAM role for the instance

Trust policy (EC2 service):

```bash
cat > /tmp/airflow-ec2-trust-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "ec2.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role \
  --role-name dataplatform_airflow_ec2 \
  --assume-role-policy-document file:///tmp/airflow-ec2-trust-policy.json
```

Attach SSM (Session Manager access, no SSH key pair needed):

```bash
aws iam attach-role-policy \
  --role-name dataplatform_airflow_ec2 \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
```

Inline policy for what `EcsRunTaskOperator` needs (run tasks in both dev/prod clusters, pass the task's execution/task roles):

```bash
cat > /tmp/airflow-ec2-ecs-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "ecs:RunTask",
      "Resource": "arn:aws:ecs:us-east-1:904464083417:task-definition/dataplatform-ingestion-task-*",
      "Condition": {
        "ArnLike": {
          "ecs:cluster": "arn:aws:ecs:us-east-1:904464083417:cluster/dataplatform-ecs-cluster-*"
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": ["ecs:DescribeTasks", "ecs:StopTask"],
      "Resource": "arn:aws:ecs:us-east-1:904464083417:task/dataplatform-ecs-cluster-*/*"
    },
    {
      "Effect": "Allow",
      "Action": "ecs:DescribeTaskDefinition",
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "arn:aws:iam::904464083417:role/dataplatform_ecs_task_execution_role_dev",
        "arn:aws:iam::904464083417:role/dataplatform_airflow"
      ]
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name dataplatform_airflow_ec2 \
  --policy-name dataplatform-airflow-ec2-ecs \
  --policy-document file:///tmp/airflow-ec2-ecs-policy.json
```

> If/when isolated prod IAM roles (`dataplatform_ecs_task_execution_role_prod`, `dataplatform_airflow_prod`) are created per `docs/PROD_DEPLOY_RUNBOOK.md:121`, add their ARNs to the `iam:PassRole` resource list above.

Create the instance profile and attach the role:

```bash
aws iam create-instance-profile --instance-profile-name dataplatform_airflow_ec2_profile

aws iam add-role-to-instance-profile \
  --instance-profile-name dataplatform_airflow_ec2_profile \
  --role-name dataplatform_airflow_ec2
```

## 2. Security group (no inbound rules)

The instance is managed exclusively through SSM Session Manager (shell access and UI port-forwarding both tunnel through the SSM agent, bypassing the security group entirely) — so no inbound rule is needed at all, not even for SSH or port 8080.

```bash
aws ec2 create-security-group \
  --group-name dataplatform-airflow-ec2-sg \
  --description "Airflow production host - no inbound, SSM-only access" \
  --vpc-id <VPC_ID_OF_THE_SUBNETS_BELOW>
```

(Leave the default egress-all-allowed rule; don't add any ingress rule.)

## 3. User-data bootstrap script

```bash
cat > /tmp/airflow-ec2-user-data.sh <<'EOF'
#!/bin/bash
set -euxo pipefail

dnf install -y docker git
systemctl enable --now docker
usermod -aG docker ec2-user

DOCKER_CONFIG=/usr/local/lib/docker/cli-plugins
mkdir -p "$DOCKER_CONFIG"
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o "$DOCKER_CONFIG/docker-compose"
chmod +x "$DOCKER_CONFIG/docker-compose"

mkdir -p /opt/camara-senado-data-ingestion
git clone https://github.com/DamodaraBarbosa/camara-senado-data-ingestion.git /opt/camara-senado-data-ingestion

cd /opt/camara-senado-data-ingestion/airflow
docker compose -f docker-compose-airflow.prod.yml up -d

# Keep DAG code current: Airflow's own scheduler re-parses the dags/ folder
# periodically, so a plain `git pull` is enough — no container restart needed
# for DAG-only changes.
cat > /etc/cron.d/camara-dags-sync <<'CRON'
*/15 * * * * root cd /opt/camara-senado-data-ingestion && git pull -q origin develop >> /var/log/camara-dags-sync.log 2>&1
CRON
EOF
```

## 4. Launch the instance

Reuse one of the subnets already used by the Fargate tasks (`airflow/dags/camera_ingestion_dag.py:23-30`) — same VPC, already has `assignPublicIp: ENABLED` for outbound internet access (ECR, GitHub, SSM endpoints):

```bash
AMI_ID=$(aws ssm get-parameters \
  --names /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query 'Parameters[0].Value' --output text --region us-east-1)

aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type t3.micro \
  --subnet-id subnet-084a2fb67517887b4 \
  --security-group-ids <SG_ID_FROM_STEP_2> \
  --iam-instance-profile Name=dataplatform_airflow_ec2_profile \
  --associate-public-ip-address \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=20,VolumeType=gp3}' \
  --user-data file:///tmp/airflow-ec2-user-data.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=dataplatform-airflow-prod}]' \
  --region us-east-1
```

## 5. First access (via SSM, no SSH key needed)

Wait ~2 minutes for SSM registration and the bootstrap script to finish, then:

```bash
aws ssm start-session --target <INSTANCE_ID> --region us-east-1
```

Inside the session, confirm the stack is up:

```bash
cd /opt/camara-senado-data-ingestion/airflow
docker compose -f docker-compose-airflow.prod.yml ps
```

If the user-data script failed partway (check `/var/log/cloud-init-output.log`), rerun the `git clone` / `docker compose up -d` commands from step 3 manually.

## 6. Access the Airflow UI (SSM port-forwarding, no public port)

From your own machine (no inbound security group rule required — SSM tunnels this directly):

```bash
aws ssm start-session \
  --target <INSTANCE_ID> \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}' \
  --region us-east-1
```

Then open http://localhost:8080 (user `airflow` / password `airflow` — same as the local dev stack; consider changing it via the UI after first login).

## 7. Validate

1. `docker compose -f docker-compose-airflow.prod.yml ps` shows `postgres-airflow`, `webserver`, `scheduler` all healthy.
2. Via the port-forwarded UI: both `camara_ingestion_pipeline` and `camara_ingestion_pipeline_prod` DAGs are visible; the prod one already shows schedule `0 6 * * 0`.
3. Manually trigger a lightweight bundle (e.g. `legislaturas`) from this instance's UI, then confirm from your own machine: `aws ecs list-tasks --cluster dataplatform-ecs-cluster-prod --region us-east-1` shows the task running/succeeded — this validates the instance role credentials (no mounted `~/.aws/credentials`, no static keys) work for `EcsRunTaskOperator`.
4. `aws ec2 reboot-instances --instance-ids <INSTANCE_ID> --region us-east-1`, wait, reconnect via SSM, confirm `docker compose ps` shows the stack back up on its own (Docker's `restart: always` policy plus `docker.service` enabled at boot).
5. Push a trivial DAG change to `develop`, wait for the next cron cycle (≤15 min), and confirm the UI reflects it without any manual restart.
6. Stop the local docker-compose Airflow stack on your own machine and confirm the following Sunday's scheduled run fires from this EC2 instance instead (check Airflow's UI run history or `airflow/logs/dag_id=camara_ingestion_pipeline_prod/run_id=scheduled__.../` on the instance).
