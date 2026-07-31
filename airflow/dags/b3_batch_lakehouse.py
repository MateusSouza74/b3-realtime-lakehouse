"""
DAG da camada batch do lakehouse.

    bronze (streaming, fora do Airflow)
        -> silver_quotes         (dbt: tipagem + deduplicacao + merge incremental)
        -> gold_*                (dbt: candles, snapshot, performance setorial)
        -> testes de qualidade   (dbt build roda os testes junto de cada camada)
        -> resumo da execucao    (le run_results.json e alimenta o dashboard)

Duas decisoes explicitas:

1. `dbt build` em vez de `dbt run` + `dbt test` separados. build roda modelo e
   teste na MESMA SparkSession, respeitando a ordem do grafo: se a silver falha
   no teste, a gold nem comeca. Duas tasks = duas sessoes Spark = ~30s a mais de
   JVM por execucao, sem ganho nenhum.

2. Uma task por camada (silver, gold) em vez de um `dbt build` unico, porque a
   granularidade por camada e o que faz o retry e o alerta serem uteis.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from alerting import notify_failure

log = logging.getLogger(__name__)

DBT_BIN = os.getenv("DBT_BIN", "/opt/dbt-venv/bin/dbt")
DBT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/dbt")
ALERTS_DIR = Path(os.getenv("ALERTS_DIR", "/data/alerts"))
RUNS_FILE = ALERTS_DIR / "pipeline_runs.jsonl"

# Um target-path por camada, para que o resumo consiga ler os dois run_results.
TARGET_PATHS = {
    "silver": "/tmp/dbt/target/silver",
    "gold": "/tmp/dbt/target/gold",
}

SCHEDULE = os.getenv("BATCH_SCHEDULE_CRON", "*/5 * * * *")

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_failure,
}


def _dbt(task_id: str, camada: str, selecao: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} build "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_DIR} "
            f"--select {selecao}"
        ),
        env={"DBT_TARGET_PATH": TARGET_PATHS[camada]},
        append_env=True,
    )


def publish_run_summary(**context) -> None:
    """Le os artefatos do dbt e publica um resumo da execucao.

    run_results.json e a fonte de verdade sobre o que rodou: quantos modelos,
    quantos testes passaram e quanto tempo levou. E o mesmo arquivo que
    ferramentas de observabilidade de dados consomem.
    """
    resumo = {
        "run_id": context["run_id"],
        "logical_date": str(context["logical_date"]),
        "finished_at": pendulum.now("America/Sao_Paulo").to_iso8601_string(),
        "models": 0,
        "seeds": 0,
        "tests_pass": 0,
        "tests_fail": 0,
        "tests_warn": 0,
        "errors": 0,
        "elapsed_seconds": 0.0,
        "camadas": {},
    }

    for camada, caminho in TARGET_PATHS.items():
        arquivo = Path(caminho) / "run_results.json"
        if not arquivo.exists():
            resumo["camadas"][camada] = "sem artefato (nao executou)"
            continue

        dados = json.loads(arquivo.read_text(encoding="utf-8"))
        contagem = {"models": 0, "seeds": 0, "tests_pass": 0, "tests_fail": 0, "tests_warn": 0, "errors": 0}

        for resultado in dados.get("results", []):
            unique_id = resultado.get("unique_id", "")
            status = resultado.get("status")

            if unique_id.startswith("test."):
                if status == "pass":
                    contagem["tests_pass"] += 1
                elif status == "warn":
                    contagem["tests_warn"] += 1
                elif status in ("fail", "error"):
                    contagem["tests_fail"] += 1
            elif unique_id.startswith("seed."):
                contagem["seeds"] += 1
            elif unique_id.startswith("model."):
                contagem["models"] += 1

            if status == "error":
                contagem["errors"] += 1

        for chave, valor in contagem.items():
            resumo[chave] += valor
        resumo["elapsed_seconds"] += round(float(dados.get("elapsed_time", 0.0)), 2)
        resumo["camadas"][camada] = contagem

    resumo["elapsed_seconds"] = round(resumo["elapsed_seconds"], 2)
    resumo["status"] = "ok" if (resumo["tests_fail"] == 0 and resumo["errors"] == 0) else "falha"

    try:
        RUNS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with RUNS_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(resumo, ensure_ascii=False, default=str) + "\n")
    except OSError as exc:
        log.error("nao foi possivel gravar %s: %s", RUNS_FILE, exc)

    log.info(
        "resumo: status=%s modelos=%s seeds=%s testes_ok=%s testes_falha=%s avisos=%s duracao=%ss",
        resumo["status"],
        resumo["models"],
        resumo["seeds"],
        resumo["tests_pass"],
        resumo["tests_fail"],
        resumo["tests_warn"],
        resumo["elapsed_seconds"],
    )


with DAG(
    dag_id="b3_batch_lakehouse",
    description="Silver e gold do lakehouse da B3 com dbt + testes de qualidade",
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=15),
    tags=["b3", "dbt", "delta", "lakehouse"],
) as dag:

    # Inclui `source:bronze` para rodar os testes da bronze (entre eles o
    # assert_bronze_stream_is_fresh, que e o alarme de pipeline parado).
    dbt_silver = _dbt(
        task_id="dbt_build_silver",
        camada="silver",
        selecao="source:bronze dim_tickers silver_quotes",
    )

    dbt_gold = _dbt(
        task_id="dbt_build_gold",
        camada="gold",
        selecao="tag:gold",
    )

    resumo = PythonOperator(
        task_id="publish_run_summary",
        python_callable=publish_run_summary,
        # ALL_DONE: quando um teste falha, o resumo e ainda MAIS necessario.
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_silver >> dbt_gold >> resumo
