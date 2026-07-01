"""Runtime records used by the operational control plane."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Mapping
from uuid import UUID, uuid4

from apex_lakehouse.control_plane.enums import (
    PipelineRunStatus,
    QualityCheckStatus,
    QualitySeverity,
    QuarantineStatus,
    SourceFileStatus,
    TriggerMode,
)
from apex_lakehouse.types import JsonDict


@dataclass(frozen=True)
class SourceFileRecord:
    source_system: str
    dataset_name: str
    source_url: str
    file_name: str
    file_hash: str
    first_seen_at: datetime
    last_seen_at: datetime
    source_file_id: UUID = field(default_factory=uuid4)
    storage_bucket: str | None = None
    storage_key: str | None = None
    competence: str | None = None
    business_date: date | None = None
    content_type: str | None = None
    file_size_bytes: int | None = None
    source_last_modified_at: datetime | None = None
    first_ingested_at: datetime | None = None
    latest_ingested_at: datetime | None = None
    status: SourceFileStatus = SourceFileStatus.DISCOVERED
    last_pipeline_run_id: UUID | None = None


@dataclass(frozen=True)
class IngestionStateRecord:
    source_system: str
    dataset_name: str
    updated_at: datetime
    updated_by: str
    watermark_business_date: date | None = None
    watermark_competence: str | None = None
    last_successful_run_id: UUID | None = None
    last_attempted_run_id: UUID | None = None
    lock_version: int = 0


@dataclass(frozen=True)
class PipelineRunRecord:
    pipeline_name: str
    source_system: str
    dataset_name: str
    trigger_mode: TriggerMode
    status: PipelineRunStatus
    started_at: datetime
    pipeline_run_id: UUID = field(default_factory=uuid4)
    orchestration_job_name: str | None = None
    orchestration_run_key: str | None = None
    requested_by: str | None = None
    requested_start_date: date | None = None
    requested_end_date: date | None = None
    finished_at: datetime | None = None
    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0
    files_discovered: int = 0
    files_processed: int = 0
    files_skipped: int = 0
    error_code: str | None = None
    error_message: str | None = None
    details_json: JsonDict = field(default_factory=dict)


@dataclass(frozen=True)
class DataQualityResultRecord:
    pipeline_run_id: UUID
    dataset_name: str
    layer_name: str
    rule_code: str
    rule_name: str
    severity: QualitySeverity
    status: QualityCheckStatus
    blocking: bool
    row_count_evaluated: int
    row_count_failed: int
    evaluated_at: datetime
    quality_result_id: UUID = field(default_factory=uuid4)
    partition_key: str | None = None
    failure_ratio: float | None = None
    details_json: JsonDict = field(default_factory=dict)


@dataclass(frozen=True)
class QuarantineRecord:
    pipeline_run_id: UUID
    source_system: str
    dataset_name: str
    layer_name: str
    rule_code: str
    reason: str
    payload_json: JsonDict
    quarantined_at: datetime
    quarantine_id: UUID = field(default_factory=uuid4)
    record_locator: str | None = None
    quarantine_status: QuarantineStatus = QuarantineStatus.OPEN
    resolved_at: datetime | None = None
    resolution_note: str | None = None


def merge_details(base: Mapping[str, object], extra: Mapping[str, object]) -> JsonDict:
    """
    Build a new JSON payload without mutating the input mappings.
    """
    merged: JsonDict = dict(base)
    merged.update(extra)
    return merged