"""Airflow DAG for CVM raw-to-bronze promotion."""

from __future__ import annotations

from orchestration.dags.common import AIRFLOW_AVAILABLE, DAG, PythonOperator, build_default_args, days_ago
from orchestration.dags.cvm_tasks import run_cvm_bronze

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="cvm_bronze",
        description="Promote ingested CVM raw files into the bronze zone.",
        default_args=build_default_args(),
        start_date=days_ago(1),
        schedule="@daily",
        catchup=False,
        tags=["cvm", "bronze"],
    ) as cvm_bronze_dag:
        PythonOperator(
            task_id="build_cvm_bronze",
            python_callable=run_cvm_bronze,
        )
else:  # pragma: no cover
    cvm_bronze_dag = None
