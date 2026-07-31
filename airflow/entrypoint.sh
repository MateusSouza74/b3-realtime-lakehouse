#!/usr/bin/env bash
#
# Sobe scheduler + webserver no mesmo container.
#
# Nao usa `airflow standalone` de proposito: standalone gera uma senha aleatoria
# num arquivo, e aqui a credencial precisa ser previsivel (admin/admin) para o
# passo a passo do README funcionar sem consultar log nenhum.
#
set -euo pipefail

STATE_DIR="${AIRFLOW_HOME:-/opt/airflow}/state"
mkdir -p "${STATE_DIR}/logs" /tmp/dbt/logs /tmp/dbt/target /tmp/spark

echo "[entrypoint] migrando banco de metadados do Airflow..."
airflow db migrate

echo "[entrypoint] garantindo usuario admin..."
airflow users create \
  --username "${AIRFLOW_ADMIN_USER:-admin}" \
  --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname B3 \
  --lastname Lakehouse \
  --role Admin \
  --email admin@example.com >/dev/null 2>&1 || echo "[entrypoint] usuario admin ja existia"

echo "[entrypoint] scheduler..."
airflow scheduler &

echo "[entrypoint] webserver em http://localhost:8080 (admin / ${AIRFLOW_ADMIN_PASSWORD:-admin})"
exec airflow webserver --port 8080
