from __future__ import annotations

from datetime import datetime, timezone

from apex_lakehouse.control_plane.records import SourceFileRecord
from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildRequest
from apex_lakehouse.ingestion.cvm.bronze_workflow import CvmBronzeBatch, CvmBronzeWorkflow


def test_build_one_delegates_to_service() -> None:
    expected_result = object()
    captured_requests = []
    service = type(
        "ServiceStub",
        (),
        {"build": lambda self, request: captured_requests.append(request) or expected_result},
    )()
    workflow = CvmBronzeWorkflow(service=service)  # type: ignore[arg-type]
    request = BronzeBuildRequest(
        source_file=_build_source_file_record(),
        updated_by="test-suite",
    )

    result = workflow.build_one(request)

    assert result is expected_result
    assert captured_requests == [request]


def test_build_many_returns_batch_with_aggregated_row_count() -> None:
    results = [
        type("ResultStub", (), {"parse_summary": type("ParseSummaryStub", (), {"row_count": 2})()})(),
        type("ResultStub", (), {"parse_summary": type("ParseSummaryStub", (), {"row_count": 3})()})(),
    ]
    service = type(
        "ServiceStub",
        (),
        {
            "__init__": lambda self: setattr(self, "index", 0),
            "build": lambda self, request: results.pop(0),
        },
    )()
    workflow = CvmBronzeWorkflow(service=service)  # type: ignore[arg-type]
    requests = [
        BronzeBuildRequest(source_file=_build_source_file_record(), updated_by="test-suite"),
        BronzeBuildRequest(source_file=_build_source_file_record(), updated_by="test-suite"),
    ]

    batch = workflow.build_many(requests)

    assert isinstance(batch, CvmBronzeBatch)
    assert batch.requests == requests
    assert len(batch.results) == 2
    assert batch.row_count == 5


def _build_source_file_record() -> SourceFileRecord:
    return SourceFileRecord(
        source_system="cvm",
        dataset_name="informe_diario",
        source_url="https://dados.cvm.gov.br/file.csv",
        file_name="file.csv",
        file_hash="hash123",
        first_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        storage_bucket="raw",
        storage_key="cvm/informe_diario/ano=2024/mes=01/file.csv",
    )
