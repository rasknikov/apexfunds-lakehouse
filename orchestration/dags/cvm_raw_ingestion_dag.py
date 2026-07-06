"""Airflow DAG for CVM raw ingestion."""

from __future__ import annotations

from orchestration.dags.common import AIRFLOW_AVAILABLE, DAG, PythonOperator, build_default_args, days_ago
from orchestration.dags.cvm_tasks import run_cvm_raw_ingestion

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="cvm_raw_ingestion",
        description="Discover and ingest CVM source files into the raw zone.",
        default_args=build_default_args(),
        start_date=days_ago(1),
        schedule="@daily",
        catchup=False,
        tags=["cvm", "raw"],
    ) as cvm_raw_ingestion_dag:
        PythonOperator(
            task_id="ingest_cvm_raw",
            python_callable=run_cvm_raw_ingestion,
        )
else:  # pragma: no cover
    cvm_raw_ingestion_dag = None
