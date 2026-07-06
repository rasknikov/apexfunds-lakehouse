from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import sys
from uuid import uuid4

sys.path.append(str(Path(__file__).resolve().parents[2]))

from apex_lakehouse.control_plane.enums import PipelineRunStatus, TriggerMode
from apex_lakehouse.control_plane.records import PipelineRunRecord, SourceFileRecord
from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildResult
from apex_lakehouse.ingestion.cvm.silver_models import SilverBuildResult
from orchestration.dags.cvm_tasks import (
    _rebuild_bronze_result,
    _row_to_source_file,
    run_cvm_bronze,
    run_cvm_discovery,
    run_cvm_raw_ingestion,
    run_cvm_silver_gold,
)


class _RepositoryStub:
    def __init__(self, source_files=None):
        self.source_files = source_files or []
        self.inserted_runs = []
        self.finished_runs = []

    def insert_pipeline_run(self, record: PipelineRunRecord) -> None:
        self.inserted_runs.append(record)

    def mark_pipeline_run_finished(self, pipeline_run_id, **kwargs) -> None:
        self.finished_runs.append((pipeline_run_id, kwargs))

    def list_source_files(self, **kwargs):
        return self.source_files


def test_run_cvm_discovery_summarizes_batches() -> None:
    repository = _RepositoryStub()
    workflow = type(
        "WorkflowStub",
        (),
        {
            "discover_many": lambda self, datasets: [
                type(
                    "BatchStub",
                    (),
                    {
                        "results": [
                            type("ResultStub", (), {"decision": type("Decision", (), {"value": "new"})()})(),
                            type("ResultStub", (), {"decision": type("Decision", (), {"value": "already_known"})()})(),
                        ]
                    },
                )()
            ]
        },
    )()

    summary = run_cvm_discovery(
        dataset_names=["cadastro_fundos"],
        repository=repository,  # type: ignore[arg-type]
        workflow=workflow,  # type: ignore[arg-type]
    )

    assert summary["files_discovered"] == 2
    assert summary["new_or_changed"] == 1
    assert repository.finished_runs[0][1]["files_processed"] == 1


def test_run_cvm_bronze_builds_requests_from_source_files() -> None:
    source_file = _build_source_file()
    repository = _RepositoryStub(source_files=[_source_file_row(source_file)])
    workflow = type(
        "WorkflowStub",
        (),
        {
            "build_many": lambda self, requests: type(
                "BatchStub",
                (),
                {
                    "results": requests,
                    "row_count": 10,
                },
            )()
        },
    )()

    summary = run_cvm_bronze(
        dataset_names=["informe_diario"],
        repository=repository,  # type: ignore[arg-type]
        workflow=workflow,  # type: ignore[arg-type]
    )

    assert summary["files_processed"] == 1
    assert summary["rows_written"] == 10
    assert repository.finished_runs[0][1]["rows_written"] == 10


def test_run_cvm_raw_ingestion_counts_processed_and_skipped() -> None:
    repository = _RepositoryStub()
    discovery_workflow = type(
        "DiscoveryWorkflowStub",
        (),
        {
            "discover_many": lambda self, datasets: [
                type(
                    "BatchStub",
                    (),
                    {
                        "results": [
                            object(),
                            object(),
                        ]
                    },
                )()
            ]
        },
    )()
    raw_workflow = type(
        "RawWorkflowStub",
        (),
        {
            "ingest_many": lambda self, discovery_results, **kwargs: [
                type("ResultStub", (), {"persisted": True})(),
                type("ResultStub", (), {"persisted": False})(),
            ]
        },
    )()

    summary = run_cvm_raw_ingestion(
        dataset_names=["cadastro_fundos"],
        repository=repository,  # type: ignore[arg-type]
        discovery_workflow=discovery_workflow,  # type: ignore[arg-type]
        raw_workflow=raw_workflow,  # type: ignore[arg-type]
    )

    assert summary["files_processed"] == 1
    assert summary["files_skipped"] == 1


def test_run_cvm_silver_gold_executes_gold_only_when_quality_allows() -> None:
    cadastro = _build_source_file(dataset_name="cadastro_fundos", competence=None, business_date=date(2024, 1, 15))
    informe = _build_source_file(dataset_name="informe_diario", competence="2024-01")
    repository = _RepositoryStub(
        source_files=[_source_file_row(cadastro), _source_file_row(informe)],
    )
    silver_workflow = type(
        "SilverWorkflowStub",
        (),
        {
            "build_one": lambda self, request: type(
                "SilverResultStub",
                (),
                {
                    "silver_dataset_name": "fundos" if request.primary_input.request.source_file.dataset_name == "cadastro_fundos" else "fundos_informe_diario",
                    "request": request,
                    "partition_key": request.primary_input.partition_key,
                    "transform_summary": type("SummaryStub", (), {"row_count": 5, "output_path": Path("missing.csv")})(),
                    "data_path": request.primary_input.data_path,
                },
            )()
        },
    )()
    quality_workflow = type(
        "QualityWorkflowStub",
        (),
        {
            "evaluate_many": lambda self, requests: type(
                "QualityBatchStub",
                (),
                {
                    "evaluations": [
                        type(
                            "EvaluationStub",
                            (),
                            {"gate": type("Gate", (), {"allowed": True})(), "request": request},
                        )()
                        for request in requests
                    ],
                    "promotion_allowed": True,
                },
            )()
        },
    )()
    quarantine_workflow = type(
        "QuarantineWorkflowStub",
        (),
        {
            "build_many": lambda self, requests: type("QuarantineBatchStub", (), {"record_count": 0})(),
        },
    )()
    gold_calls = []
    gold_workflow = type(
        "GoldWorkflowStub",
        (),
        {
            "build_one": lambda self, request: gold_calls.append(request) or type("GoldResultStub", (), {"outputs": [1, 2, 3]})(),
        },
    )()

    summary = run_cvm_silver_gold(
        competence="2024-01",
        repository=repository,  # type: ignore[arg-type]
        silver_workflow=silver_workflow,  # type: ignore[arg-type]
        quality_workflow=quality_workflow,  # type: ignore[arg-type]
        quarantine_workflow=quarantine_workflow,  # type: ignore[arg-type]
        gold_workflow=gold_workflow,  # type: ignore[arg-type]
    )

    assert summary["silver_results"] == 2
    assert summary["gold_results"] == 1
    assert len(gold_calls) == 1


def test_row_to_source_file_and_rebuild_bronze_result_preserve_conventions() -> None:
    source_file = _build_source_file(dataset_name="informe_diario", competence="2024-01")
    row = _source_file_row(source_file)

    rebuilt = _row_to_source_file(row)
    bronze_result = _rebuild_bronze_result(
        rebuilt,
        updated_by="test-suite",
        path_builder=__import__("apex_lakehouse.storage.paths", fromlist=["StoragePathBuilder"]).StoragePathBuilder.from_settings(),
    )

    assert isinstance(bronze_result, BronzeBuildResult)
    assert bronze_result.partition_key == "ano=2024/mes=01"
    assert bronze_result.data_path.key.endswith(f"source_file_id={source_file.source_file_id}/part-00000.csv")


def _build_source_file(
    *,
    dataset_name: str = "informe_diario",
    competence: str | None = "2024-01",
    business_date: date | None = None,
) -> SourceFileRecord:
    return SourceFileRecord(
        source_system="cvm",
        dataset_name=dataset_name,
        source_url="https://dados.cvm.gov.br/file.csv",
        file_name="file.csv",
        file_hash="hash123",
        first_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        source_file_id=uuid4(),
        storage_bucket="raw",
        storage_key="cvm/file.csv",
        competence=competence,
        business_date=business_date,
        status=__import__("apex_lakehouse.control_plane.enums", fromlist=["SourceFileStatus"]).SourceFileStatus.INGESTED,
    )


def _source_file_row(source_file: SourceFileRecord) -> dict[str, object]:
    return {
        "source_file_id": source_file.source_file_id,
        "source_system": source_file.source_system,
        "dataset_name": source_file.dataset_name,
        "source_url": source_file.source_url,
        "file_name": source_file.file_name,
        "storage_bucket": source_file.storage_bucket,
        "storage_key": source_file.storage_key,
        "competence": source_file.competence,
        "business_date": source_file.business_date,
        "content_type": source_file.content_type,
        "file_hash": source_file.file_hash,
        "file_size_bytes": source_file.file_size_bytes,
        "source_last_modified_at": source_file.source_last_modified_at,
        "first_seen_at": source_file.first_seen_at,
        "last_seen_at": source_file.last_seen_at,
        "first_ingested_at": source_file.first_ingested_at,
        "latest_ingested_at": source_file.latest_ingested_at,
        "status": source_file.status.value,
        "last_pipeline_run_id": source_file.last_pipeline_run_id,
    }
