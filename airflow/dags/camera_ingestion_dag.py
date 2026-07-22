from datetime import datetime, timedelta
import json
from pathlib import Path
from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator 


# Define the path to the configuration file
CONFIG_FILE_PATH = Path(__file__).resolve().parent / "config" / "bundles_config.json"

with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
    config_data = json.load(f)

BUNDLES_CONFIG = config_data.get("bundles_config", {})

# Task default arguments
DEFAULT_ARGS = {
    "Owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG settings
with DAG(
    dag_id="camara_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline for ingesting data from the Brazilian Chamber of Deputies",
    schedule_interval="@weekly",
    catchup=False,
    tags=["camara", "data_ingestion", "fargate"],
) as dag:

    # Dinamic generation of tree dependencies
    for bundle_name, config in BUNDLES_CONFIG.items():
        tasks_pool = {} 

        for step in config.get("sequence", []):
            extractor = step.get("extractor")
            task_id = f"run_{bundle_name}_{extractor}"

            # Dinamic building of EVENT_PAYLOAD
            event_payload = {
                "extractor": extractor,
                "params": {
                    "init_legislatura": config.get("init_legislatura", 56)
                },
                "destination": {
                    "type": "s3",
                    "bucket": "dataplatform-camara-dev-db",
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

                # Mapping the network configuration for Fargate
                network_configuration={
                    "awsvpcConfiguration": {
                        "subnets": ["subnet-084a2fb67517887b4"],
                        "securityGroups": ["sg-0a2d626cc7f4efafa"],
                        "assignPublicIp": "ENABLED"
                    }
                },
                aws_conn_id="aws_default",
                region_name=config.get("region", "us-east-1"),
            )

            # Local pool registration
            tasks_pool[extractor] = ecs_task

            # Set dependencies based on the sequence defined in the config
            for dep in step.get("depends_on", []):
                if dep in tasks_pool:
                    tasks_pool[dep] >> ecs_task
