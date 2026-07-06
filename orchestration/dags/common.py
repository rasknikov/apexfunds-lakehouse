"""Shared Airflow DAG configuration and lightweight compatibility helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

DEFAULT_OWNER = "apex-lakehouse"
DEFAULT_RETRIES = 2
DEFAULT_RETRY_DELAY = timedelta(minutes=5)
DEFAULT_CATCHUP = False
CVM_DATASET_NAMES = [
    "cadastro_fundos",
    "informe_diario",
    "perfil_mensal",
]

try:  # pragma: no cover - exercised indirectly when Airflow is available
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    AIRFLOW_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - this is the default in local unit tests
    DAG = None
    PythonOperator = None
    AIRFLOW_AVAILABLE = False


def build_default_args() -> dict[str, object]:
    return {
        "owner": DEFAULT_OWNER,
        "retries": DEFAULT_RETRIES,
        "retry_delay": DEFAULT_RETRY_DELAY,
    }


def days_ago(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)
