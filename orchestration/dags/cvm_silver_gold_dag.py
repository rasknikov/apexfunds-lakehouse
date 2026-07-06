"""Airflow DAG for silver, quality, quarantine and gold promotion."""

from __future__ import annotations

from orchestration.dags.common import AIRFLOW_AVAILABLE, DAG, PythonOperator, build_default_args, days_ago
from orchestration.dags.cvm_tasks import run_cvm_silver_gold

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="cvm_silver_gold",
        description="Build CVM silver outputs, quality gates, quarantine and gold marts.",
        default_args=build_default_args(),
        start_date=days_ago(1),
        schedule="@daily",
        catchup=False,
        tags=["cvm", "silver", "gold", "quality"],
    ) as cvm_silver_gold_dag:
        PythonOperator(
            task_id="build_cvm_silver_and_gold",
            python_callable=run_cvm_silver_gold,
        )
else:  # pragma: no cover
    cvm_silver_gold_dag = None
