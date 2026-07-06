"""Pure Python task functions used by Airflow DAGs for the CVM pipeline."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence
from uuid import UUID

from apex_lakehouse.control_plane.enums import PipelineRunStatus, SourceFileStatus, TriggerMode
from apex_lakehouse.control_plane.records import PipelineRunRecord, SourceFileRecord
from apex_lakehouse.control_plane.repository import ControlPlaneRepository
from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildRequest, BronzeBuildResult, BronzeParseSummary
from apex_lakehouse.ingestion.cvm.bronze_service import (
    _build_bronze_dataset_name,
    _resolve_partition_key,
)
from apex_lakehouse.ingestion.cvm.bronze_workflow import CvmBronzeWorkflow
from apex_lakehouse.ingestion.cvm.discovery_models import CvmDataset
from apex_lakehouse.ingestion.cvm.discovery_workflow import CvmDiscoveryWorkflow
from apex_lakehouse.ingestion.cvm.gold_models import GoldMartBuildRequest
from apex_lakehouse.ingestion.cvm.gold_workflow import CvmGoldWorkflow
from apex_lakehouse.ingestion.cvm.raw_ingestion_workflow import CvmRawIngestionWorkflow
from apex_lakehouse.ingestion.cvm.silver_models import (
    SilverBuildRequest,
    SilverBuildResult,
    SilverTransformSummary,
)
from apex_lakehouse.ingestion.cvm.silver_service import SILVER_DATASET_NAME_BY_BRONZE_DATASET
from apex_lakehouse.ingestion.cvm.silver_workflow import CvmSilverWorkflow
from apex_lakehouse.quality.models import DatasetQualityRequest
from apex_lakehouse.quality.workflow import DatasetQualityWorkflow
from apex_lakehouse.quarantine.models import QuarantineBuildRequest, QuarantineReplayRequest
from apex_lakehouse.quarantine.workflow import DatasetQuarantineWorkflow
from apex_lakehouse.storage.models import ObjectStoragePath
from apex_lakehouse.storage.paths import StoragePathBuilder


def run_cvm_discovery(
    *,
    dataset_names: Sequence[str] | None = None,
    requested_by: str = "airflow",
    repository: ControlPlaneRepository | None = None,
    workflow: CvmDiscoveryWorkflow | None = None,
) -> dict[str, object]:
    resolved_repository = repository or ControlPlaneRepository.from_settings()
    run_record = _start_pipeline_run(
        resolved_repository,
        pipeline_name="cvm_discovery",
        dataset_name="cvm_discovery",
        trigger_mode=TriggerMode.SCHEDULED,
        requested_by=requested_by,
    )

    resolved_dataset_names = list(dataset_names or ["cadastro_fundos", "informe_diario", "perfil_mensal"])
    resolved_workflow = workflow or CvmDiscoveryWorkflow.from_repository(resolved_repository)
    datasets = [_to_dataset(dataset_name) for dataset_name in resolved_dataset_names]
    batches = resolved_workflow.discover_many(datasets)
    files_discovered = sum(len(batch.results) for batch in batches)
    new_or_changed = sum(
        1
        for batch in batches
        for result in batch.results
        if result.decision.value in {"new", "changed"}
    )

    _finish_pipeline_run(
        resolved_repository,
        run_record,
        status=PipelineRunStatus.SUCCEEDED,
        files_discovered=files_discovered,
        files_processed=new_or_changed,
        details_json={
            "datasets": resolved_dataset_names,
            "new_or_changed": new_or_changed,
        },
    )
    return {
        "pipeline_run_id": str(run_record.pipeline_run_id),
        "datasets": resolved_dataset_names,
        "files_discovered": files_discovered,
        "new_or_changed": new_or_changed,
    }


def run_cvm_raw_ingestion(
    *,
    dataset_names: Sequence[str] | None = None,
    requested_by: str = "airflow",
    repository: ControlPlaneRepository | None = None,
    discovery_workflow: CvmDiscoveryWorkflow | None = None,
    raw_workflow: CvmRawIngestionWorkflow | None = None,
) -> dict[str, object]:
    resolved_repository = repository or ControlPlaneRepository.from_settings()
    run_record = _start_pipeline_run(
        resolved_repository,
        pipeline_name="cvm_raw_ingestion",
        dataset_name="cvm_raw_ingestion",
        trigger_mode=TriggerMode.SCHEDULED,
        requested_by=requested_by,
    )
    resolved_dataset_names = list(dataset_names or ["cadastro_fundos", "informe_diario", "perfil_mensal"])
    resolved_discovery = discovery_workflow or CvmDiscoveryWorkflow.from_repository(resolved_repository)
    resolved_raw = raw_workflow or CvmRawIngestionWorkflow.from_repository(resolved_repository)

    discovery_batches = resolved_discovery.discover_many([_to_dataset(name) for name in resolved_dataset_names])
    discovery_results = [result for batch in discovery_batches for result in batch.results]
    workflow_results = resolved_raw.ingest_many(
        discovery_results,
        updated_by=requested_by,
        pipeline_run_id=run_record.pipeline_run_id,
    )
    processed = sum(1 for result in workflow_results if result.persisted)
    skipped = len(workflow_results) - processed

    _finish_pipeline_run(
        resolved_repository,
        run_record,
        status=PipelineRunStatus.SUCCEEDED,
        files_discovered=len(discovery_results),
        files_processed=processed,
        files_skipped=skipped,
        details_json={"datasets": resolved_dataset_names},
    )
    return {
        "pipeline_run_id": str(run_record.pipeline_run_id),
        "files_discovered": len(discovery_results),
        "files_processed": processed,
        "files_skipped": skipped,
    }


def run_cvm_bronze(
    *,
    dataset_names: Sequence[str] | None = None,
    competence: str | None = None,
    requested_by: str = "airflow",
    repository: ControlPlaneRepository | None = None,
    workflow: CvmBronzeWorkflow | None = None,
) -> dict[str, object]:
    resolved_repository = repository or ControlPlaneRepository.from_settings()
    run_record = _start_pipeline_run(
        resolved_repository,
        pipeline_name="cvm_bronze",
        dataset_name="cvm_bronze",
        trigger_mode=TriggerMode.BACKFILL if competence is not None else TriggerMode.SCHEDULED,
        requested_by=requested_by,
    )
    source_files = _load_source_files(
        resolved_repository,
        dataset_names=dataset_names or ["cadastro_fundos", "informe_diario", "perfil_mensal"],
        competence=competence,
    )
    resolved_workflow = workflow or CvmBronzeWorkflow.from_settings()
    batch = resolved_workflow.build_many(
        [
            BronzeBuildRequest(
                source_file=source_file,
                updated_by=requested_by,
                pipeline_run_id=run_record.pipeline_run_id,
            )
            for source_file in source_files
        ]
    )

    _finish_pipeline_run(
        resolved_repository,
        run_record,
        status=PipelineRunStatus.SUCCEEDED,
        rows_written=batch.row_count,
        files_processed=len(batch.results),
        details_json={"competence": competence, "datasets": list(dataset_names or [])},
    )
    return {
        "pipeline_run_id": str(run_record.pipeline_run_id),
        "files_processed": len(batch.results),
        "rows_written": batch.row_count,
    }


def run_cvm_silver_gold(
    *,
    competence: str | None = None,
    requested_by: str = "airflow",
    repository: ControlPlaneRepository | None = None,
    silver_workflow: CvmSilverWorkflow | None = None,
    quality_workflow: DatasetQualityWorkflow | None = None,
    quarantine_workflow: DatasetQuarantineWorkflow | None = None,
    gold_workflow: CvmGoldWorkflow | None = None,
) -> dict[str, object]:
    resolved_repository = repository or ControlPlaneRepository.from_settings()
    run_record = _start_pipeline_run(
        resolved_repository,
        pipeline_name="cvm_silver_gold",
        dataset_name="cvm_silver_gold",
        trigger_mode=TriggerMode.BACKFILL if competence is not None else TriggerMode.SCHEDULED,
        requested_by=requested_by,
    )
    path_builder = StoragePathBuilder.from_settings()

    source_files = _load_source_files(
        resolved_repository,
        dataset_names=["cadastro_fundos", "informe_diario", "perfil_mensal"],
        competence=competence,
    )
    source_files_by_dataset: dict[str, list[SourceFileRecord]] = {}
    for source_file in source_files:
        source_files_by_dataset.setdefault(source_file.dataset_name, []).append(source_file)

    latest_cadastro = next(iter(source_files_by_dataset.get("cadastro_fundos", [])), None)
    cadastro_bronze = (
        _rebuild_bronze_result(latest_cadastro, updated_by=requested_by, path_builder=path_builder)
        if latest_cadastro is not None
        else None
    )

    resolved_silver = silver_workflow or CvmSilverWorkflow.from_settings()
    silver_results: list[SilverBuildResult] = []
    if cadastro_bronze is not None:
        silver_results.append(
            resolved_silver.build_one(
                SilverBuildRequest(
                    primary_input=cadastro_bronze,
                    updated_by=requested_by,
                    pipeline_run_id=run_record.pipeline_run_id,
                )
            )
        )

    for dataset_name in ("informe_diario", "perfil_mensal"):
        for source_file in source_files_by_dataset.get(dataset_name, []):
            bronze_result = _rebuild_bronze_result(
                source_file,
                updated_by=requested_by,
                path_builder=path_builder,
            )
            silver_results.append(
                resolved_silver.build_one(
                    SilverBuildRequest(
                        primary_input=bronze_result,
                        updated_by=requested_by,
                        pipeline_run_id=run_record.pipeline_run_id,
                        cadastro_input=cadastro_bronze,
                    )
                )
            )

    resolved_quality = quality_workflow or DatasetQualityWorkflow.from_repository(resolved_repository)
    quality_batch = resolved_quality.evaluate_many(
        [
            DatasetQualityRequest(
                silver_result=result,
                pipeline_run_id=run_record.pipeline_run_id,
                dataset_name=result.silver_dataset_name,
            )
            for result in silver_results
        ]
    )

    resolved_quarantine = quarantine_workflow or DatasetQuarantineWorkflow.from_repository(resolved_repository)
    quarantine_results = resolved_quarantine.build_many(
        [
            QuarantineBuildRequest(
                evaluation=evaluation,
                source_system="cvm",
                dataset_name=evaluation.request.dataset_name,
                layer_name=evaluation.request.layer_name,
            )
            for evaluation in quality_batch.evaluations
            if not evaluation.gate.allowed
        ]
    )

    gold_results = []
    resolved_gold = gold_workflow or CvmGoldWorkflow.from_settings()
    funds_result = next(
        (result for result in silver_results if result.silver_dataset_name == "fundos"),
        None,
    )
    if funds_result is not None:
        for evaluation in quality_batch.evaluations:
            if not evaluation.gate.allowed:
                continue
            silver_result = evaluation.request.silver_result
            if silver_result.silver_dataset_name != "fundos_informe_diario":
                continue
            gold_results.append(
                resolved_gold.build_one(
                    GoldMartBuildRequest(
                        funds_input=funds_result,
                        informe_input=silver_result,
                        updated_by=requested_by,
                        pipeline_run_id=run_record.pipeline_run_id,
                    )
                )
            )

    _finish_pipeline_run(
        resolved_repository,
        run_record,
        status=PipelineRunStatus.SUCCEEDED,
        rows_written=sum(result.transform_summary.row_count for result in silver_results),
        rows_quarantined=quarantine_results.record_count,
        files_processed=len(silver_results),
        details_json={
            "silver_results": len(silver_results),
            "gold_results": len(gold_results),
            "quality_allowed": quality_batch.promotion_allowed,
            "competence": competence,
        },
    )
    return {
        "pipeline_run_id": str(run_record.pipeline_run_id),
        "silver_results": len(silver_results),
        "quality_allowed": quality_batch.promotion_allowed,
        "quarantine_records": quarantine_results.record_count,
        "gold_results": len(gold_results),
    }


def run_cvm_replay_by_competence(
    *,
    competence: str,
    requested_by: str = "airflow-replay",
    repository: ControlPlaneRepository | None = None,
) -> dict[str, object]:
    resolved_repository = repository or ControlPlaneRepository.from_settings()
    bronze_summary = run_cvm_bronze(
        competence=competence,
        requested_by=requested_by,
        repository=resolved_repository,
    )
    silver_gold_summary = run_cvm_silver_gold(
        competence=competence,
        requested_by=requested_by,
        repository=resolved_repository,
    )
    return {
        "competence": competence,
        "bronze": bronze_summary,
        "silver_gold": silver_gold_summary,
    }


def mark_quarantine_for_replay(
    *,
    quarantine_id: str,
    requested_by: str = "airflow-replay",
    repository: ControlPlaneRepository | None = None,
) -> dict[str, object]:
    resolved_repository = repository or ControlPlaneRepository.from_settings()
    workflow = DatasetQuarantineWorkflow.from_repository(resolved_repository)
    workflow.mark_for_replay(
        QuarantineReplayRequest(
            quarantine_id=UUID(quarantine_id),
            requested_at=datetime.now(timezone.utc),
            resolution_note=f"Requested by {requested_by}",
        )
    )
    return {"quarantine_id": quarantine_id, "status": "replay_pending"}


def _start_pipeline_run(
    repository: ControlPlaneRepository,
    *,
    pipeline_name: str,
    dataset_name: str,
    trigger_mode: TriggerMode,
    requested_by: str,
) -> PipelineRunRecord:
    record = PipelineRunRecord(
        pipeline_name=pipeline_name,
        source_system="cvm",
        dataset_name=dataset_name,
        trigger_mode=trigger_mode,
        status=PipelineRunStatus.RUNNING,
        started_at=datetime.now(timezone.utc),
        requested_by=requested_by,
    )
    repository.insert_pipeline_run(record)
    return record


def _finish_pipeline_run(
    repository: ControlPlaneRepository,
    run_record: PipelineRunRecord,
    *,
    status: PipelineRunStatus,
    rows_written: int = 0,
    rows_quarantined: int = 0,
    files_discovered: int = 0,
    files_processed: int = 0,
    files_skipped: int = 0,
    details_json: dict[str, object] | None = None,
) -> None:
    repository.mark_pipeline_run_finished(
        run_record.pipeline_run_id,
        status=status.value,
        finished_at=datetime.now(timezone.utc),
        rows_read=0,
        rows_written=rows_written,
        rows_quarantined=rows_quarantined,
        files_discovered=files_discovered,
        files_processed=files_processed,
        files_skipped=files_skipped,
        error_code=None,
        error_message=None,
    )


def _load_source_files(
    repository: ControlPlaneRepository,
    *,
    dataset_names: Sequence[str],
    competence: str | None = None,
) -> list[SourceFileRecord]:
    rows = repository.list_source_files(
        source_system="cvm",
        dataset_names=list(dataset_names),
        status=SourceFileStatus.INGESTED.value,
        competence=competence,
        limit=500,
    )
    return [_row_to_source_file(record) for record in rows]


def _row_to_source_file(row: dict[str, object]) -> SourceFileRecord:
    return SourceFileRecord(
        source_file_id=row["source_file_id"],
        source_system=row["source_system"],
        dataset_name=row["dataset_name"],
        source_url=row["source_url"],
        file_name=row["file_name"],
        storage_bucket=row.get("storage_bucket"),
        storage_key=row.get("storage_key"),
        competence=row.get("competence"),
        business_date=row.get("business_date"),
        content_type=row.get("content_type"),
        file_hash=row["file_hash"],
        file_size_bytes=row.get("file_size_bytes"),
        source_last_modified_at=row.get("source_last_modified_at"),
        first_seen_at=row["first_seen_at"],
        last_seen_at=row["last_seen_at"],
        first_ingested_at=row.get("first_ingested_at"),
        latest_ingested_at=row.get("latest_ingested_at"),
        status=SourceFileStatus(row["status"]),
        last_pipeline_run_id=row.get("last_pipeline_run_id"),
    )


def _rebuild_bronze_result(
    source_file: SourceFileRecord,
    *,
    updated_by: str,
    path_builder: StoragePathBuilder,
) -> BronzeBuildResult:
    partition_key = _resolve_partition_key(source_file)
    bronze_dataset_name = _build_bronze_dataset_name(source_file)
    bronze_prefix = path_builder.bronze(bronze_dataset_name, partition_key=partition_key)
    source_prefix = f"{bronze_prefix.key}/source_file_id={source_file.source_file_id}"
    return BronzeBuildResult(
        request=BronzeBuildRequest(source_file=source_file, updated_by=updated_by),
        bronze_dataset_name=bronze_dataset_name,
        partition_key=partition_key,
        parse_summary=BronzeParseSummary(
            output_path=Path("bronze.csv"),
            schema_path=Path("bronze.schema.json"),
            row_count=0,
            columns=tuple(),
        ),
        data_path=ObjectStoragePath(
            bucket=bronze_prefix.bucket,
            key=f"{source_prefix}/part-00000.csv",
        ),
        schema_path=ObjectStoragePath(
            bucket=bronze_prefix.bucket,
            key=f"{source_prefix}/schema.json",
        ),
    )


def _to_dataset(dataset_name: str) -> CvmDataset:
    return CvmDataset(dataset_name)
