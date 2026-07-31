"""
Manutencao das tabelas Delta (OPTIMIZE).

Por que isso existe: streaming com trigger de 15s cria um arquivo Parquet novo a
cada micro-batch. Em um pregao de 8h sao ~2.000 arquivos por particao. E o
problema classico de small files - a leitura degrada e o `_delta_log` cresce.
OPTIMIZE compacta esses arquivos e e seguro rodar concorrente com o append do
streaming (compactacao nao altera dados; o controle de concorrencia otimista do
Delta resolve).

Roda no minuto 7 de cada hora para nao concorrer com o batch (*/5). Como o
Airflow esta com SequentialExecutor, as duas DAGs nunca executam ao mesmo tempo
de qualquer forma - o que tambem protege o lock do metastore Derby.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from alerting import notify_failure

DBT_BIN = os.getenv("DBT_BIN", "/opt/dbt-venv/bin/dbt")
DBT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/dbt")

with DAG(
    dag_id="b3_lakehouse_maintenance",
    description="OPTIMIZE das tabelas Delta (compactacao de small files)",
    schedule="7 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notify_failure,
    },
    dagrun_timeout=timedelta(minutes=20),
    tags=["b3", "delta", "manutencao"],
) as dag:

    optimize = BashOperator(
        task_id="optimize_delta_tables",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} run-operation optimize_lakehouse "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_DIR}"
        ),
        env={"DBT_TARGET_PATH": "/tmp/dbt/target/maintenance"},
        append_env=True,
    )
