from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from apex_lakehouse.control_plane.records import SourceFileRecord
from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildRequest, BronzeBuildResult, BronzeParseSummary
from apex_lakehouse.ingestion.cvm.silver_models import SilverBuildRequest
from apex_lakehouse.ingestion.cvm.silver_workflow import CvmSilverBatch, CvmSilverWorkflow
from apex_lakehouse.storage.models import ObjectStoragePath


def test_build_many_returns_batch_with_aggregated_row_count() -> None:
    results = [
        type("ResultStub", (), {"transform_summary": type("SummaryStub", (), {"row_count": 2})()})(),
        type("ResultStub", (), {"transform_summary": type("SummaryStub", (), {"row_count": 3})()})(),
    ]
    service = type(
        "ServiceStub",
        (),
        {"build": lambda self, request: results.pop(0)},
    )()
    workflow = CvmSilverWorkflow(service=service)  # type: ignore[arg-type]

    batch = workflow.build_many(
        [
            SilverBuildRequest(
                primary_input=_build_bronze_result("cadastro_fundos"),
                updated_by="test-suite",
            ),
            SilverBuildRequest(
                primary_input=_build_bronze_result("informe_diario"),
                updated_by="test-suite",
            ),
        ]
    )

    assert isinstance(batch, CvmSilverBatch)
    assert len(batch.results) == 2
    assert batch.row_count == 5


def _build_bronze_result(dataset_name: str) -> BronzeBuildResult:
    return BronzeBuildResult(
        request=BronzeBuildRequest(
            source_file=SourceFileRecord(
                source_system="cvm",
                dataset_name=dataset_name,
                source_url="https://dados.cvm.gov.br/file.csv",
                file_name="file.csv",
                file_hash="hash123",
                first_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
                last_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
            ),
            updated_by="test-suite",
        ),
        bronze_dataset_name=f"cvm_{dataset_name}",
        partition_key="ano=2024/mes=01",
        parse_summary=BronzeParseSummary(
            output_path=Path("ignored.csv"),
            schema_path=Path("ignored.schema.json"),
            row_count=1,
            columns=tuple(),
        ),
        data_path=ObjectStoragePath(bucket="lakehouse", key="bronze/ignored/part.csv"),
        schema_path=ObjectStoragePath(bucket="lakehouse", key="bronze/ignored/schema.json"),
    )
