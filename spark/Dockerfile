FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.3
ARG SCALA_VERSION=2.12
ARG DELTA_VERSION=3.2.1
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps tzdata \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Jars baixados no build (deterministico). Evita depender de --packages/ivy em
# runtime, que e a causa classica de "funciona na minha maquina".
ENV PYSPARK_JARS=/usr/local/lib/python3.11/site-packages/pyspark/jars
RUN set -eux; \
    MAVEN=https://repo1.maven.org/maven2; \
    for url in \
      "${MAVEN}/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar" \
      "${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
      "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
    ; do curl -fsSL -o "${PYSPARK_JARS}/$(basename "${url}")" "${url}"; done

ENV SPARK_CONF_DIR=/opt/spark-conf
COPY conf/ /opt/spark-conf/
COPY jobs/ /opt/app/jobs/

# Mesmo uid do Airflow (50000): os dois containers escrevem na MESMA tabela
# Delta bronze (o Spark faz append, o Airflow roda OPTIMIZE).
RUN useradd --uid 50000 --gid 0 --create-home --home-dir /home/spark spark \
 && mkdir -p /tmp/spark \
 && chown -R 50000:0 /home/spark /opt/app /tmp/spark

ENV HOME=/home/spark
USER 50000
WORKDIR /opt/app

CMD ["python", "-u", "jobs/bronze_stream.py"]
