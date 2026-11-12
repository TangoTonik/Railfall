# Airflow image for the Railfall DAGs: the official image plus the railfall
# package, so tasks call the same tested code as `make ingest-*`.
#
# Built with the repository root as context (see docker-compose.yml). The
# railfall install is resolved against Airflow's published constraints so it
# can never upgrade a dependency Airflow itself pins.
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

COPY --chown=airflow:root pyproject.toml README.md /opt/railfall/
COPY --chown=airflow:root src /opt/railfall/src

RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      "apache-airflow==${AIRFLOW_VERSION}" \
      /opt/railfall \
 && python -c "import railfall.ingest.run"
