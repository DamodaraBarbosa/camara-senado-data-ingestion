from datetime import datetime, timedelta
import json
from pathlib import Path
from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

CONFIG_DIR = Path(__file__).resolve().parent / "config"

# Task default arguments
DEFAULT_ARGS = {
    "Owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Rede compartilhada entre dev e prod: mesma VPC/subnets/security group até
# que um ambiente de produção isolado seja provisionado (ver docs/PROD_DEPLOY_RUNBOOK.md).
NETWORK_CONFIGURATION = {
    "awsvpcConfiguration": {
        "subnets": [
            "subnet-084a2fb67517887b4",
            "subnet-0324dd13835617811",
            "subnet-0c5f308bb0fe40dc4",
            "subnet-09407d2c96c9fa608",
            "subnet-0268d990dacda04a3",
            "subnet-0481c74a8c7d9a274"
        ],
        "securityGroups": ["sg-0a2d626cc7f4efafa"],
        "assignPublicIp": "ENABLED"
    }
}


def build_dag(dag_id: str, config_path: Path, s3_bucket: str, schedule_interval) -> DAG:
    """Constrói a DAG de ingestão para um ambiente (dev ou prod).

    Cada ambiente lê seu próprio ``bundles_config.{env}.json`` (cluster/task
    definition distintos) e grava num bucket S3 próprio — o resto do grafo de
    dependências é idêntico entre ambientes.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        config_data = json.load(f)
    bundles_config = config_data.get("bundles_config", {})

    dag = DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        description="Pipeline for ingesting data from the Brazilian Chamber of Deputies",
        schedule_interval=schedule_interval,
        catchup=False,
        tags=["camara", "data_ingestion", "fargate"],
    )

    with dag:
        # Dinamic generation of tree dependencies
        for bundle_name, config in bundles_config.items():
            tasks_pool = {}

            for step in config.get("sequence", []):
                extractor = step.get("extractor")
                task_id = f"run_{bundle_name}_{extractor}"

                # Dinamic building of EVENT_PAYLOAD
                params = {}
                if config.get("init_legislatura") is not None:
                    params["init_legislatura"] = config["init_legislatura"]

                event_payload = {
                    "extractor": extractor,
                    "params": params,
                    "destination": {
                        "type": "s3",
                        "bucket": s3_bucket,
                        "prefix": f"raw/{bundle_name}/{extractor}"
                    },
                    "run_id": "{{ run_id }}"
                }

                # Create the ECS task operator
                ecs_task = EcsRunTaskOperator(
                    task_id=task_id,
                    cluster=config.get("cluster"),
                    task_definition=config.get("task_definition"),
                    launch_type="FARGATE",
                    overrides={
                        "containerOverrides": [
                            {
                                "name": "ingestion-container",
                                "command": ["python", f"/app/bundles/{bundle_name}/app/runner.py"],
                                "environment": [
                                    {"name": "BUNDLE", "value": bundle_name},
                                    {"name": "EVENT_PAYLOAD", "value": json.dumps(event_payload)}
                                ]
                            }
                        ]
                    },
                    network_configuration=NETWORK_CONFIGURATION,
                    aws_conn_id="aws_default",
                    region_name=config.get("region", "us-east-1"),
                )

                # Local pool registration
                tasks_pool[extractor] = ecs_task

                # Set dependencies based on the sequence defined in the config
                for dep in step.get("depends_on", []):
                    if dep in tasks_pool:
                        tasks_pool[dep] >> ecs_task

    return dag


# Ambiente dev: mesmo dag_id de sempre, preserva o histórico de execuções já
# existente no Airflow. Continua agendada semanalmente, como já era.
camara_ingestion_pipeline = build_dag(
    dag_id="camara_ingestion_pipeline",
    config_path=CONFIG_DIR / "bundles_config.dev.json",
    s3_bucket="dataplatform-camara-dev-db",
    schedule_interval="@weekly",
)

# Ambiente prod: nova DAG, sem agendamento automático até a infraestrutura de
# produção (cluster ECS, task definition, bucket S3 — ver
# docs/PROD_DEPLOY_RUNBOOK.md) existir e ser validada manualmente.
camara_ingestion_pipeline_prod = build_dag(
    dag_id="camara_ingestion_pipeline_prod",
    config_path=CONFIG_DIR / "bundles_config.prod.json",
    s3_bucket="dataplatform-camara-prod-db",
    schedule_interval=None,
)
