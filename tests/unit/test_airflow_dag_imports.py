from __future__ import annotations

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))


def test_dag_modules_import_without_airflow_installed() -> None:
    modules = [
        "orchestration.dags.cvm_discovery_dag",
        "orchestration.dags.cvm_raw_ingestion_dag",
        "orchestration.dags.cvm_bronze_dag",
        "orchestration.dags.cvm_silver_gold_dag",
        "orchestration.dags.cvm_replay_by_competence_dag",
    ]

    for module_name in modules:
        __import__(module_name)
