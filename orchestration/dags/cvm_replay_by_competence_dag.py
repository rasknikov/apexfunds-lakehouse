"""Airflow DAG for manual replay by competence."""

from __future__ import annotations

from orchestration.dags.common import AIRFLOW_AVAILABLE, DAG, PythonOperator, build_default_args, days_ago
from orchestration.dags.cvm_tasks import run_cvm_replay_by_competence

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="cvm_replay_by_competence",
        description="Replay CVM bronze, silver, quality, quarantine and gold for one competence.",
        default_args=build_default_args(),
        start_date=days_ago(1),
        schedule=None,
        catchup=False,
        params={"competence": "2024-01"},
        tags=["cvm", "replay", "backfill"],
    ) as cvm_replay_by_competence_dag:
        PythonOperator(
            task_id="replay_cvm_by_competence",
            python_callable=run_cvm_replay_by_competence,
            op_kwargs={"competence": "{{ params.competence }}"},
        )
else:  # pragma: no cover
    cvm_replay_by_competence_dag = None
