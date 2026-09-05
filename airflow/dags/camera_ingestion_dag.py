from datetime import datetime, timedelta
import json
import os
import traceback
from pathlib import Path
from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

CONFIG_DIR = Path(__file__).resolve().parent / "config"

# ARN do topico SNS de alertas (criado em camara-senado-data-infra,
# environments/prod/main.tf). Ausente = callback vira no-op, que e exatamente
# o que se quer no stack local de dev: nada de alerta, nada de excecao.
ALERT_SNS_TOPIC_ARN = os.getenv("CAMARA_ALERT_SNS_TOPIC_ARN", "")


def notify_failure(context) -> None:
    """Publica uma falha no SNS.

    Existe porque ate agora uma falha em producao era completamente silenciosa:
    ``email_on_failure`` esta desligado, nao ha SMTP configurado, nao havia
    topico SNS e nao ha alarme no CloudWatch. Com cadencia semanal, uma falha
    que ninguem ve custa uma semana inteira de dados.

    Nunca levanta excecao: um callback que falha polui o log do scheduler e nao
    conserta nada. O pior caso aceitavel e o alerta se perder — nao a task
    trocar sua causa de falha real por um erro de boto3.
    """
    if not ALERT_SNS_TOPIC_ARN:
        return

    try:
        import boto3

        ti = context.get("task_instance")
        dag_run = context.get("dag_run")
        dag_id = getattr(ti, "dag_id", "?")
        task_id = getattr(ti, "task_id", "<dag-run>")
        run_id = getattr(dag_run, "run_id", "?")
        exception = context.get("exception")

        subject = f"[Airflow][FALHA] {dag_id}.{task_id}"[:100]
        body = "\n".join([
            f"DAG:        {dag_id}",
            f"Task:       {task_id}",
            f"Run:        {run_id}",
            f"Tentativa:  {getattr(ti, 'try_number', '?')}",
            f"Log:        {getattr(ti, 'log_url', 'n/a')}",
            "",
            f"Excecao:    {exception!r}",
            "",
            "Logs do container em /ecs/dataplatform-ingestion-task-prod (CloudWatch).",
        ])

        boto3.client("sns").publish(
            TopicArn=ALERT_SNS_TOPIC_ARN, Subject=subject, Message=body
        )
    except Exception:  # noqa: BLE001
        print(f"[alert] falha ao publicar no SNS (ignorado):\n{traceback.format_exc()}")


# Task default arguments
DEFAULT_ARGS = {
    # Lowercase: "Owner" is not a recognised key, so Airflow silently ignored it
    # and fell back to the default owner (also "airflow", which masked the typo).
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # Backstop on the Airflow side. The runners wrap their work in
    # asyncio.wait_for(), but CamaraBulkClient dispatches CSV parsing via
    # asyncio.to_thread and cancelling that future does not stop the thread —
    # run_votacoes_votos has finished at 27min under a hardcoded 600s timeout.
    # So this is the only ceiling that actually holds. Sized above the
    # waiter ceiling below so the waiter reports the timeout first.
    "execution_timeout": timedelta(minutes=110),
    # Sinal imediato, nomeando a task que quebrou. Dispara so na tentativa
    # final (depois de `retries`), entao um 429 transitorio nao vira e-mail.
    "on_failure_callback": notify_failure,
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
        max_active_runs=1,
        tags=["camara", "data_ingestion", "fargate"],
        # Complementa o callback por task: cobre a dagrun que termina falhada
        # sem nenhuma task ter falhado por si (upstream_failed, deadlock de
        # dependencia), caso em que o callback de task nunca dispararia.
        on_failure_callback=notify_failure,
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
                    # Sem "prefix": ele gerava uma copia adicional da saida, que
                    # era inerte enquanto a chave canonica coincidia com ela.
                    # Com a particao na chave, passaria a duplicar cada arquivo
                    # fora da particao — 632 MB so em despesas — e a espalhar
                    # arquivos soltos na raiz do LOCATION da tabela Glue.
                    "destination": {
                        "type": "s3",
                        "bucket": s3_bucket
                    },
                    "run_id": "{{ run_id }}",
                    # Particao Hive do raw/. `data_interval_end` e nao `ds`:
                    # num pipeline de snapshot a particao deve nomear a data em
                    # que o mundo foi capturado (o fim da janela), nao o inicio
                    # dela. Identico nas 56 tasks da mesma dagrun, que e o que
                    # mantem leitura e escrita de dependencia na mesma particao.
                    "ingestion_date": "{{ data_interval_end | ds }}"
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
                    # Deferrable mode is switched on per-environment via
                    # AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE (the operator reads
                    # it as its `deferrable` default), so this DAG file still
                    # works unchanged in an environment with no triggerer.
                    #
                    # These two are tuned here because the defaults suit neither
                    # case: waiter_delay=6 would have all ~56 deferred tasks
                    # polling DescribeTasks ten times a minute each, and
                    # waiter_max_attempts=1000000 makes the defer timeout
                    # (max_attempts * delay + 60) effectively infinite. 30s x 200
                    # gives a 100-minute ceiling, which clears the longest
                    # observed task (run_deputados_despesas, 44min) with room.
                    waiter_delay=30,
                    waiter_max_attempts=200,
                )

                # Local pool registration
                tasks_pool[extractor] = ecs_task

                # Set dependencies based on the sequence defined in the config
                for dep in step.get("depends_on", []):
                    if dep in tasks_pool:
                        tasks_pool[dep] >> ecs_task

    return dag


# Ambiente dev: mesmo dag_id de sempre, preserva o histórico de execuções já
# existente no Airflow. Sem agendamento automático — dev é usado para testes
# pontuais, então só deve rodar quando disparada manualmente.
camara_ingestion_pipeline = build_dag(
    dag_id="camara_ingestion_pipeline",
    config_path=CONFIG_DIR / "bundles_config.dev.json",
    s3_bucket="dataplatform-camara-dev-db",
    schedule_interval=None,
)

# Ambiente prod: roda uma vez por semana, domingo às 06:00 UTC (03:00 BRT).
# Esse horário fecha a semana legislativa completa (sessões costumam ser
# terça-quinta) e roda em baixo tráfego externo, o que reduz retries nas APIs
# da Câmara/Senado e, consequentemente, o tempo faturável no Fargate. A DAG
# nasce pausada (AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true) até a
# infraestrutura de produção (cluster ECS, task definition, bucket S3 — ver
# docs/PROD_DEPLOY_RUNBOOK.md) existir e ser validada manualmente.
camara_ingestion_pipeline_prod = build_dag(
    dag_id="camara_ingestion_pipeline_prod",
    config_path=CONFIG_DIR / "bundles_config.prod.json",
    s3_bucket="dataplatform-camara-prod-db",
    schedule_interval="0 6 * * 0",
)
