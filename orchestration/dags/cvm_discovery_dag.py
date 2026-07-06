"""Airflow DAG for standalone CVM source discovery."""

from __future__ import annotations

from orchestration.dags.common import AIRFLOW_AVAILABLE, DAG, PythonOperator, build_default_args, days_ago
from orchestration.dags.cvm_tasks import run_cvm_discovery

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="cvm_discovery",
        description="Discover CVM source artifacts and classify new or changed files.",
        default_args=build_default_args(),
        start_date=days_ago(1),
        schedule="@daily",
        catchup=False,
        tags=["cvm", "discovery"],
    ) as cvm_discovery_dag:
        PythonOperator(
            task_id="discover_cvm_sources",
            python_callable=run_cvm_discovery,
        )
else:  # pragma: no cover - fallback when Airflow is not installed locally
    cvm_discovery_dag = None
