"""
Camada bronze: Kafka -> Delta Lake via Spark Structured Streaming.

Regras da bronze:
  * append-only, nenhuma regra de negocio;
  * o payload da API e preservado como chegou (tipagem e deduplicacao ficam na silver);
  * metadados de Kafka (particao/offset) e de processamento ficam gravados, o que
    permite auditar de onde veio cada linha;
  * particionada por ingestion_date.

O dbt le esta tabela como `source`, nao a materializa.
"""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "b3.quotes.raw")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/data/delta/bronze/quotes")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/data/checkpoints/bronze_quotes")
TRIGGER_SECONDS = int(os.getenv("TRIGGER_SECONDS", "15"))
HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "30"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-7s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("bronze_stream")

# Schema do payload JSON publicado pelo producer. Timestamps entram como string
# e sao convertidos depois: assim um formato inesperado gera NULL em vez de
# derrubar o micro-batch inteiro.
QUOTE_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("ticker", StringType()),
        StructField("short_name", StringType()),
        StructField("currency", StringType()),
        StructField("price", DoubleType()),
        StructField("previous_close", DoubleType()),
        StructField("day_open", DoubleType()),
        StructField("day_high", DoubleType()),
        StructField("day_low", DoubleType()),
        StructField("volume", LongType()),
        StructField("change_pct_api", DoubleType()),
        StructField("market_cap", DoubleType()),
        StructField("quote_ts", StringType()),
        StructField("ingestion_ts", StringType()),
        StructField("source", StringType()),
        StructField("poll_cycle_id", StringType()),
    ]
)

# Formato das colunas que o conector Kafka expoe e que este job consome.
# Serve para criar a tabela vazia no boot com EXATAMENTE o mesmo schema do stream.
KAFKA_SHAPE = StructType(
    [
        StructField("value", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


def build_session() -> SparkSession:
    # Toda a config (Delta, fuso, memoria) vem de conf/spark-defaults.conf.
    return SparkSession.builder.appName("b3-bronze-stream").getOrCreate()


def to_bronze(df: DataFrame) -> DataFrame:
    """Transformacao unica, usada tanto no stream quanto no bootstrap da tabela."""
    return (
        df.select(
            F.from_json(F.col("value").cast("string"), QUOTE_SCHEMA).alias("q"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("q.*", "kafka_partition", "kafka_offset", "kafka_ts")
        .filter(F.col("ticker").isNotNull())
        .withColumn("quote_ts", F.col("quote_ts").cast("timestamp"))
        .withColumn("ingestion_ts", F.col("ingestion_ts").cast("timestamp"))
        .withColumn("processing_ts", F.current_timestamp())
        .withColumn("ingestion_date", F.to_date("ingestion_ts"))
    )


def bootstrap_table(spark: SparkSession) -> None:
    """Cria a tabela Delta vazia se ela ainda nao existe.

    Sem isso, o dbt (que registra bronze como tabela externa) falharia na primeira
    execucao caso o primeiro micro-batch ainda nao tivesse acontecido.
    `mode("ignore")` torna a operacao idempotente.
    """
    empty = to_bronze(spark.createDataFrame([], KAFKA_SHAPE))
    (
        empty.write.format("delta")
        .mode("ignore")
        .partitionBy("ingestion_date")
        .save(BRONZE_PATH)
    )
    log.info("tabela bronze pronta em %s", BRONZE_PATH)


def main() -> None:
    log.info(
        "bronze_stream | topico=%s | bootstrap=%s | destino=%s | trigger=%ss",
        KAFKA_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
        BRONZE_PATH,
        TRIGGER_SECONDS,
    )

    spark = build_session()
    bootstrap_table(spark)

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        # Retencao do topico e de 24h: se algo for descartado antes de ser lido,
        # o job segue em frente em vez de morrer.
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        to_bronze(kafka_stream)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .partitionBy("ingestion_date")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .queryName("bronze_quotes")
        .start(BRONZE_PATH)
    )

    log.info("streaming ativo. Spark UI em http://localhost:4040")

    total_rows = 0
    try:
        while query.isActive:
            query.awaitTermination(HEARTBEAT_SECONDS)
            progress = query.lastProgress
            if progress:
                total_rows += progress.get("numInputRows", 0)
                log.info(
                    "batch=%s eventos=%s acumulado=%s taxa=%.2f ev/s",
                    progress.get("batchId"),
                    progress.get("numInputRows"),
                    total_rows,
                    progress.get("processedRowsPerSecond") or 0.0,
                )
    except KeyboardInterrupt:
        log.info("interrompido, parando a query...")
        query.stop()

    if query.exception():
        log.error("query terminou com erro: %s", query.exception())
        sys.exit(1)


if __name__ == "__main__":
    main()
