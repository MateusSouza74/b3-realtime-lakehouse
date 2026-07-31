"""
Alertas de falha compartilhados pelas DAGs.

Estrategia: sem depender de SMTP (que exige config externa e nao roda "de
primeira" em ambiente local), o alerta e gravado em /data/alerts/alerts.jsonl e
logado no Airflow. O dashboard le esse arquivo e mostra o painel de alertas -
o ciclo fecha dentro do proprio projeto.

Se ALERT_WEBHOOK_URL estiver definido, o alerta tambem vai para Slack ou Discord
(o payload usa as duas chaves, `text` e `content`; cada servico ignora a outra).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

ALERTS_DIR = Path(os.getenv("ALERTS_DIR", "/data/alerts"))
ALERTS_FILE = ALERTS_DIR / "alerts.jsonl"
WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()


def _append_jsonl(path: Path, payload: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    except OSError as exc:
        log.error("nao foi possivel gravar em %s: %s", path, exc)


def _send_webhook(mensagem: str) -> None:
    if not WEBHOOK_URL:
        return
    try:
        import requests

        requests.post(WEBHOOK_URL, json={"text": mensagem, "content": mensagem}, timeout=5)
    except Exception as exc:  # noqa: BLE001 - alerta nunca deve derrubar a DAG
        log.error("falha ao enviar webhook: %s", exc)


def notify_failure(context: dict) -> None:
    """on_failure_callback: grava o alerta, loga e opcionalmente manda webhook."""
    ti = context.get("task_instance")
    excecao = context.get("exception")

    payload = {
        "severity": "error",
        "detected_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dag_id": getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": getattr(ti, "run_id", None),
        "try_number": getattr(ti, "try_number", None),
        "exception": str(excecao) if excecao else None,
        "log_url": getattr(ti, "log_url", None),
    }

    _append_jsonl(ALERTS_FILE, payload)
    log.error("[ALERTA] %s", json.dumps(payload, ensure_ascii=False, default=str))

    _send_webhook(
        f":rotating_light: b3-lakehouse FALHOU\n"
        f"dag={payload['dag_id']} task={payload['task_id']} "
        f"tentativa={payload['try_number']}\n{payload['exception']}"
    )
