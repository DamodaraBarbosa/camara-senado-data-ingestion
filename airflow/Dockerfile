# Custom production Airflow image: bakes in apache-airflow-providers-amazon
# at build time instead of relying on _PIP_ADDITIONAL_REQUIREMENTS, which
# reinstalls the package via pip on every single container start (webserver,
# scheduler, airflow-init). On a small/burstable EC2 instance that repeated
# install burns through CPU credits fast enough to cause a crash loop: once
# credits hit zero the instance throttles to baseline CPU, pip gets even
# slower, Airflow's internal startup retry limit is exceeded, the container
# exits, and `restart: always` starts the same expensive install over again.
#
# Apache's own docs call _PIP_ADDITIONAL_REQUIREMENTS a quick-testing
# convenience, not a production pattern — this is the production pattern.
FROM apache/airflow:2.8.1-python3.11

# Installed against Apache's official constraints file for this exact Airflow
# and Python version, and with apache-airflow itself pinned. Both are load-
# bearing, not belt-and-braces: pinning only the provider is NOT enough.
#
# Measured, on this base image: `pip install
# "apache-airflow-providers-amazon[aiobotocore]==8.29.0"` resolves aiobotocore
# to 3.9.0, and to satisfy its transitive requirements pip happily UPGRADES
# apache-airflow 2.8.1 -> 3.3.1. The build still reports success. That would
# reach production as an unannounced major-version jump: Airflow 3 removed
# `schedule_interval=` (which camera_ingestion_dag.py uses) and needs a
# metadata DB migration.
#
# 8.16.0 is the amazon provider the 2.8.1 constraints pin, and it has the same
# deferrable API this project relies on (deferrable read from
# AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE, TaskDoneTrigger, waiter_delay /
# waiter_max_attempts). The [aiobotocore] extra is required rather than
# optional: TaskDoneTrigger.run() opens EcsHook(...).async_conn and
# AwsLogsHook(...).async_conn, which are aiobotocore clients, and the provider
# declares aiobotocore only under that extra. Without it every deferred task
# fails on import in the triggerer.
#
# Versions are literal on purpose. Build ARGs are not safe here: the base image
# already declares PYTHON_VERSION as the full patch version (3.11.8), so an
# `ARG PYTHON_VERSION=3.11` here is silently overridden and the constraints URL
# 404s on constraints-3.11.8.txt.
RUN pip install --no-cache-dir \
      "apache-airflow==2.8.1" \
      "apache-airflow-providers-amazon[aiobotocore]==8.16.0" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt" \
 && pip check \
 && airflow version \
 && python -c "import aiobotocore, boto3; from airflow.providers.amazon.aws.triggers.ecs import TaskDoneTrigger"
