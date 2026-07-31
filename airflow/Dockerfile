FROM apache/airflow:2.10.5-python3.11

ARG SCALA_VERSION=2.12
ARG DELTA_VERSION=3.2.1

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# dbt num virtualenv proprio: as deps dele nao encostam nas do Airflow.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

# Jars do Delta no pyspark do venv do dbt (mesma versao do job de streaming).
RUN set -eux; \
    MAVEN=https://repo1.maven.org/maven2; \
    JARS="$(/opt/dbt-venv/bin/python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    curl -fsSL -o "${JARS}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar" \
      "${MAVEN}/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar"; \
    curl -fsSL -o "${JARS}/delta-storage-${DELTA_VERSION}.jar" \
      "${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"

COPY conf/ /opt/spark-conf/
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod -R a+rX /opt/dbt-venv /opt/spark-conf \
 && chmod 0755 /opt/entrypoint.sh \
 && mkdir -p /tmp/dbt/logs /tmp/dbt/target \
 && chown -R 50000:0 /tmp/dbt

USER airflow

# Nao coloque /opt/dbt-venv/bin no PATH: isso sombrearia o python do Airflow.
# O caminho do binario do dbt e passado explicitamente pelas DAGs.
ENV SPARK_CONF_DIR=/opt/spark-conf \
    DBT_BIN=/opt/dbt-venv/bin/dbt

CMD ["bash", "/opt/entrypoint.sh"]
