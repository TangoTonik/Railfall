# Airflow image for the Railfall DAGs: the official image plus the railfall
# package, so tasks call the same tested code as `make ingest-*` / `make silver`.
#
# The Spark tasks run PySpark in local mode inside the worker, so the image
# also carries a Java 17 runtime (Spark 4.0 needs 17+).
#
# Built with the repository root as context (see docker-compose.yml). The
# railfall install is resolved against Airflow's published constraints so it
# can never upgrade a dependency Airflow itself pins.
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/railfall-java
# Architecture-independent (amd64 in CI, arm64 on Apple silicon).
ENV JAVA_HOME=/usr/lib/jvm/railfall-java
USER airflow

COPY --chown=airflow:root pyproject.toml README.md /opt/railfall/
COPY --chown=airflow:root src /opt/railfall/src

RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      "apache-airflow==${AIRFLOW_VERSION}" \
      /opt/railfall \
 && python -c "import railfall.ingest.run"

# Airflow's constraints pin pyspark 3.5 (for its Spark provider, unused here);
# railfall's transforms target Spark 4.0, so the spark extra is installed in a
# second, unconstrained step that can only touch pyspark and py4j.
RUN pip install --no-cache-dir "pyspark>=4.0,<4.1" \
 && python -c "import pyspark, railfall.spark.job, railfall.load.job"

# Resolve hadoop-aws (+ AWS SDK bundle) into the image's ivy cache at build
# time, so a Spark task never waits on Maven Central at run time.
RUN python -c "from railfall.spark.session import build_session; \
from railfall.config import Settings; build_session(Settings.from_env()).stop()"
