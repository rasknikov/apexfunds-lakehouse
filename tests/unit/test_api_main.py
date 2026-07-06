from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys
from uuid import uuid4

from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[2]))

from api.app.dependencies import get_control_plane_repository, get_settings
from api.app.main import app
from apex_lakehouse.config import PlatformSettings


class _RepositoryStub:
    def check_health(self) -> bool:
        return True

    def list_latest_pipeline_runs(self, *, limit: int = 20, pipeline_name: str | None = None):
        return [
            {
                "pipeline_run_id": uuid4(),
                "pipeline_name": pipeline_name or "cvm_gold_build",
                "source_system": "cvm",
                "dataset_name": "fato_fundo_diario",
                "trigger_mode": "scheduled",
                "status": "succeeded",
                "started_at": datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
                "finished_at": datetime(2024, 1, 15, 10, 5, 0, tzinfo=timezone.utc),
                "rows_read": 10,
                "rows_written": 10,
                "rows_quarantined": 0,
                "files_discovered": 1,
                "files_processed": 1,
                "files_skipped": 0,
                "error_code": None,
                "error_message": None,
                "details_json": {"stage": "gold"},
            }
        ]

    def list_latest_quality_results(self, *, limit: int = 20, dataset_name: str | None = None):
        return [
            {
                "quality_result_id": uuid4(),
                "pipeline_run_id": uuid4(),
                "dataset_name": dataset_name or "fundos_informe_diario",
                "layer_name": "silver",
                "rule_code": "quota_value_positive",
                "rule_name": "Quota value must be greater than zero",
                "severity": "critical",
                "status": "passed",
                "blocking": True,
                "partition_key": "ano=2024/mes=01",
                "row_count_evaluated": 100,
                "row_count_failed": 0,
                "failure_ratio": 0.0,
                "details_json": {"field_name": "valor_cota"},
                "evaluated_at": datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc),
            }
        ]


def test_health_endpoint_returns_operational_checks() -> None:
    client = _build_client()

    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["checks"]["postgres"]["ok"] is True


def test_ops_pipelines_latest_returns_items() -> None:
    client = _build_client()

    response = client.get(
        "/ops/pipelines/latest",
        params={"limit": 5, "pipeline_name": "cvm_gold_build"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["items"][0]["pipeline_name"] == "cvm_gold_build"
    assert payload["items"][0]["details_json"]["stage"] == "gold"


def test_quality_latest_returns_items() -> None:
    client = _build_client()

    response = client.get("/quality/latest", params={"dataset_name": "fundos_informe_diario"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["items"][0]["dataset_name"] == "fundos_informe_diario"
    assert payload["items"][0]["rule_code"] == "quota_value_positive"


def _build_client() -> TestClient:
    settings = PlatformSettings.from_env()
    repository = _RepositoryStub()
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_control_plane_repository] = lambda: repository
    return TestClient(app)
