# Runbook: Airflow production host (EC2)

This document is a manual checklist, same style as `docs/PROD_DEPLOY_RUNBOOK.md`. **Nothing here runs automatically in CI.** It provisions a single small, always-on EC2 instance to run the Airflow scheduler/webserver for production, so the weekly DAG (`camara_ingestion_pipeline_prod`, `0 6 * * 0`) no longer depends on a local machine staying on. MWAA was ruled out: it bills a fixed hourly rate for the environment even when idle, which doesn't make sense for a single weekly trigger.

Run the commands below with a user/role that has admin permission in AWS account `904464083417` (region `us-east-1`), reviewing each one before running. Assumes `docs/PROD_DEPLOY_RUNBOOK.md` has already been completed (ECS cluster/task definitions/S3 bucket for prod already exist).

## 1. Build and push the custom Airflow image

The base `apache/airflow` image ships without the `apache-airflow-providers-amazon` package that `EcsRunTaskOperator` needs. Do **not** install it via `_PIP_ADDITIONAL_REQUIREMENTS` at container startup on this host: that reinstalls the package via pip on every single container start (webserver, scheduler, airflow-init), and on a small/burstable instance (`t3.micro`) that's enough repeated CPU load to exhaust CPU credits, throttle the instance to baseline, make the install even slower, blow past Airflow's internal startup retry limit, and crash-loop indefinitely (`restart: always` just repeats the expensive install forever). Bake the provider into the image once instead — see `airflow/Dockerfile`.

That image also pins `apache-airflow==2.8.1` and installs against Apache's constraints file for this exact Airflow/Python pair, and installs the amazon provider with its `[aiobotocore]` extra. None of that is cosmetic: the extra is what `EcsRunTaskOperator`'s deferrable mode needs (its trigger opens `aiobotocore` clients), and without the pin plus constraints, pip resolving that extra will happily upgrade Airflow itself across a major version — 2.8.1 to 3.x — while still reporting a successful build. See `airflow/Dockerfile` for the measured details.

Build from a machine with real (non-burstable) CPU — your own machine or CI, not the EC2 instance being provisioned:

```bash
aws ecr create-repository --repository-name camara-airflow --region us-east-1

aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 904464083417.dkr.ecr.us-east-1.amazonaws.com

docker build -t camara-airflow:latest -f airflow/Dockerfile airflow/
docker tag camara-airflow:latest 904464083417.dkr.ecr.us-east-1.amazonaws.com/camara-airflow:latest
docker push 904464083417.dkr.ecr.us-east-1.amazonaws.com/camara-airflow:latest
```

Re-run the `build`/`tag`/`push` commands whenever `airflow/Dockerfile` changes (e.g. bumping the Airflow or provider version) — the EC2 instance only ever pulls this image, it never builds it.

## 2. IAM role for the instance

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

Inline policy for what `EcsRunTaskOperator` needs (run tasks in both dev/prod clusters, pass the task's execution/task roles) plus permission to pull the custom Airflow image from ECR (step 1):

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
    },
    {
      "Effect": "Allow",
      "Action": "ecr:GetAuthorizationToken",
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability"],
      "Resource": "arn:aws:ecr:us-east-1:904464083417:repository/camara-airflow"
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

## 3. Security group (no inbound rules)

The instance is managed exclusively through SSM Session Manager (shell access and UI port-forwarding both tunnel through the SSM agent, bypassing the security group entirely) — so no inbound rule is needed at all, not even for SSH or port 8080.

```bash
aws ec2 create-security-group \
  --group-name dataplatform-airflow-ec2-sg \
  --description "Airflow production host - no inbound, SSM-only access" \
  --vpc-id <VPC_ID_OF_THE_SUBNETS_BELOW>
```

(Leave the default egress-all-allowed rule; don't add any ingress rule.)

## 4. User-data bootstrap script

```bash
cat > /tmp/airflow-ec2-user-data.sh <<'EOF'
#!/bin/bash
set -euxo pipefail

# Swap. AL2023 boots with none, and this instance has 913MB of RAM shared by
# postgres, the scheduler and the triggerer (~620-690MB resident between them)
# plus the short-lived task subprocesses the scheduler forks during DAG fan-out
# and completion. With no swap those bursts have nowhere to go and the kernel
# OOM-killer picks a victim host-wide — which is how the scheduler died before.
# 2GB on the existing 20GB gp3 volume costs nothing extra; swappiness=10 keeps
# it as a safety valve rather than something the kernel reaches for routinely.
dd if=/dev/zero of=/swapfile bs=1M count=2048
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab
echo 'vm.swappiness=10' > /etc/sysctl.d/99-swap.conf
sysctl -p /etc/sysctl.d/99-swap.conf

# cronie: Amazon Linux 2023 doesn't ship cron by default (no /etc/cron.d
# even exists until this is installed) — needed for the DAG-sync job below.
dnf install -y docker git cronie
systemctl enable --now docker
systemctl enable --now crond
usermod -aG docker ec2-user

DOCKER_CONFIG=/usr/local/lib/docker/cli-plugins
mkdir -p "$DOCKER_CONFIG"
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o "$DOCKER_CONFIG/docker-compose"
chmod +x "$DOCKER_CONFIG/docker-compose"

# Pull the custom image (step 1) using the instance role's credentials —
# no static keys involved.
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 904464083417.dkr.ecr.us-east-1.amazonaws.com

mkdir -p /opt/camara-senado-data-ingestion
git clone -b main https://github.com/DamodaraBarbosa/camara-senado-data-ingestion.git /opt/camara-senado-data-ingestion

cd /opt/camara-senado-data-ingestion/airflow

# The webserver/scheduler containers run as uid 50000 (see
# docker-compose-airflow.prod.yml); the bind-mounted dirs below are created
# by this script (running as root) and must be writable by that uid, or
# Airflow fails to create its log directories and crash-loops.
mkdir -p dags logs plugins
chown -R 50000:0 dags logs plugins

docker compose -f docker-compose-airflow.prod.yml up -d

# Keep DAG code current: Airflow's own scheduler re-parses the dags/ folder
# periodically, so a plain `git pull` is enough — no container restart needed
# for DAG-only changes.
cat > /etc/cron.d/camara-dags-sync <<'CRON'
*/15 * * * * root cd /opt/camara-senado-data-ingestion && git pull -q origin main >> /var/log/camara-dags-sync.log 2>&1
CRON

# Airflow's task logs are a bind mount on this instance's 20GB volume and
# nothing prunes them. A weekly sweep keeps a slow disk-full from taking the
# scheduler down months from now.
cat > /etc/cron.d/camara-airflow-logs <<'CRON'
0 4 * * 1 root find /opt/camara-senado-data-ingestion/airflow/logs -type f -mtime +30 -delete
CRON
EOF
```

## 5. Launch the instance

Reuse one of the subnets already used by the Fargate tasks (`airflow/dags/camera_ingestion_dag.py:23-30`) — same VPC, already has `assignPublicIp: ENABLED` for outbound internet access (ECR, GitHub, SSM endpoints):

```bash
AMI_ID=$(aws ssm get-parameters \
  --names /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query 'Parameters[0].Value' --output text --region us-east-1)

aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type t3.micro \
  --subnet-id subnet-084a2fb67517887b4 \
  --security-group-ids <SG_ID_FROM_STEP_3> \
  --iam-instance-profile Name=dataplatform_airflow_ec2_profile \
  --associate-public-ip-address \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=20,VolumeType=gp3}' \
  --user-data file:///tmp/airflow-ec2-user-data.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=dataplatform-airflow-prod}]' \
  --region us-east-1
```

`t3.micro` is fine now that the provider is baked into the image at build time (step 1) rather than reinstalled via pip on every container start — the earlier crash-loop was caused specifically by that repeated install exhausting CPU credits, not by the instance size itself.

## 6. First access (via SSM, no SSH key needed)

Wait ~2 minutes for SSM registration and the bootstrap script to finish, then:

```bash
aws ssm start-session --target <INSTANCE_ID> --region us-east-1
```

Inside the session, confirm the stack is up:

```bash
cd /opt/camara-senado-data-ingestion/airflow
docker compose -f docker-compose-airflow.prod.yml ps
```

If the user-data script failed partway (check `/var/log/cloud-init-output.log`), rerun the `docker login` / `git clone` / `chown` / `docker compose up -d` commands from step 4 manually.

## 7. Access the Airflow UI (on demand, SSM port-forwarding, no public port)

`docker compose up -d` (step 4) only starts `postgres-airflow` and `scheduler` — the `webserver` service is under the `ui` Compose profile and is **not** part of that default set. Running gunicorn's 4 default workers alongside the scheduler re-parsing `camera_ingestion_dag.py` (it dynamically builds ~60 `EcsRunTaskOperator` tasks) every 30s pegged this `t3.micro`'s 2 vCPUs at 100% and crash-looped, confirmed live via SSM (`load average` 30+, no swap/OOM — genuine CPU contention). The actual requirement is just the scheduler firing the weekly cron; the UI is a convenience, so it only runs when you ask for it:

```bash
aws ssm start-session --target <INSTANCE_ID> --region us-east-1
cd /opt/camara-senado-data-ingestion/airflow
docker compose -f docker-compose-airflow.prod.yml --profile ui up -d webserver
```

Then, from your own machine (no inbound security group rule required — SSM tunnels this directly):

```bash
aws ssm start-session \
  --target <INSTANCE_ID> \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}' \
  --region us-east-1
```

Open http://localhost:8080 (user `airflow` / password `airflow` — same as the local dev stack; consider changing it via the UI after first login). When done, stop it so the instance goes back to its light footprint:

```bash
docker compose -f docker-compose-airflow.prod.yml --profile ui stop webserver
```

If, after this tuning, `load average` (via `uptime`) still stays consistently high even with the webserver stopped, the next step would be resizing to `t3.small` — not attempted here since tuning alone resolved it.

## 8. Validate

1. `docker compose -f docker-compose-airflow.prod.yml ps` shows `postgres-airflow`, `scheduler` and `triggerer` healthy by default (no `webserver` — see step 7), with stable (not repeatedly resetting) uptimes. `uptime` shows a `load average` well under 2.0 (2 vCPUs) a few minutes after startup. `free -m` shows the 2GB swapfile present and the three containers sitting around 620-690MB total in `docker stats`.
2. Start the webserver on demand (step 7): both `camara_ingestion_pipeline` and `camara_ingestion_pipeline_prod` DAGs are visible; the prod one already shows schedule `0 6 * * 0`. Stop it again afterward and confirm `load average` drops back down.
3. Manually trigger a lightweight bundle (e.g. `legislaturas`) from this instance's UI, then confirm from your own machine: `aws ecs list-tasks --cluster dataplatform-ecs-cluster-prod --region us-east-1` shows the task running/succeeded — this validates the instance role credentials (no mounted `~/.aws/credentials`, no static keys) work for `EcsRunTaskOperator`.
   Tasks must reach the **`deferred`** state (pink in the UI) rather than staying `running` for the whole Fargate duration. `deferred` is the proof the wait moved out of the executor and into the triggerer; if tasks stay `running`, `AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE` did not take effect. If they enter `deferred` and never leave, the triggerer is down or missing the `aiobotocore` extra — check `docker compose logs triggerer`.
4. `aws ec2 reboot-instances --instance-ids <INSTANCE_ID> --region us-east-1`, wait, reconnect via SSM, confirm `docker compose ps` shows the stack back up on its own (Docker's `restart: always` policy plus `docker.service` enabled at boot).
5. Merge a trivial DAG change into `main`, wait for the next cron cycle (≤15 min), and confirm the UI reflects it without any manual restart.
6. Stop the local docker-compose Airflow stack on your own machine and confirm the following Sunday's scheduled run fires from this EC2 instance instead (check Airflow's UI run history or `airflow/logs/dag_id=camara_ingestion_pipeline_prod/run_id=scheduled__.../` on the instance).

## 8b. Known-good baseline

Numbers from the first full production run this host ever completed successfully (2026-09-01), for comparison when something looks wrong later:

| | |
|---|---|
| DAG run | 56/56 success, **42 minutes** |
| Concurrent `deferred` tasks | 10 at peak, with `PARALLELISM: 2` |
| Resident | scheduler 270MB, triggerer 218MB, postgres 31MB |
| Peak swap | 807MB of 2048MB |
| Minimum free RAM | 31MB |
| `oom-kill` count | 0 |
| S3 output | 56 objects, 1.48GB |

A run that takes much longer than ~45 minutes, or any non-zero `oom-kill` count in `journalctl -k`, means something regressed. Note the minimum free RAM: this host runs with essentially no spare memory and depends on swap by design, so `free -m` showing little available is normal here, not a symptom.

## 9. Applying changes to an instance that is already running

Steps 1-8 provision a new host. This section is the update path, which the rest of the runbook did not cover, and the gap was not theoretical.

The `camara-dags-sync` cron runs `git pull`, which updates the files on disk and nothing else. Docker does not re-read a compose file on its own, so **an environment change merged to `main` has no effect on a running container until someone recreates it.** On 2026-09-01 the scheduler container on this host was found still running with its 2026-08-22 configuration: `AIRFLOW__CORE__PARALLELISM` was absent entirely, meaning the concurrency cap from commit `45c67f8` had been merged, released, and never applied. The host OOM-killed the scheduler five times between Aug 30 and Aug 31 while that fix sat on disk, unapplied.

Two things therefore always need a deliberate redeploy: changes to `docker-compose-airflow.prod.yml` (recreate the containers) and changes to `airflow/Dockerfile` (the tag is `:latest`, so `up -d` alone will not fetch a new build — you must `pull` first).

**Build and push first** (from your own machine or CI — never on the burstable instance, see step 1):

```bash
docker build -t camara-airflow:latest -f airflow/Dockerfile airflow/
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 904464083417.dkr.ecr.us-east-1.amazonaws.com
docker tag camara-airflow:latest 904464083417.dkr.ecr.us-east-1.amazonaws.com/camara-airflow:latest
docker push 904464083417.dkr.ecr.us-east-1.amazonaws.com/camara-airflow:latest
```

**Then, on the instance** (the compose file comes from `main`, so merge there first):

```bash
aws ssm start-session --target <INSTANCE_ID> --region us-east-1
cd /opt/camara-senado-data-ingestion && sudo git pull origin main
cd airflow
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 904464083417.dkr.ecr.us-east-1.amazonaws.com
docker compose -f docker-compose-airflow.prod.yml pull
docker compose -f docker-compose-airflow.prod.yml up -d
```

**Backfill the swapfile.** An instance provisioned before the swap was added to the user-data script (step 4) does not have it, and that script does not re-run — it is a one-shot at first boot. Apply it by hand once, using the same commands from step 4 (`dd` … `sysctl -p`). Confirm with `free -m` that `Swap:` is no longer `0`. Do this **before** starting the triggerer: it adds roughly 300MB of resident memory to a 913MB host.

**Confirm nothing moved that should not have.** The pinned install exists because an unpinned one silently upgraded Airflow across a major version:

```bash
docker compose -f docker-compose-airflow.prod.yml exec scheduler airflow version   # must print 2.8.1
docker compose -f docker-compose-airflow.prod.yml exec scheduler pip check
```
