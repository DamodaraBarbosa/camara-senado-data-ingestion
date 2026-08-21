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
RUN pip install --no-cache-dir apache-airflow-providers-amazon
