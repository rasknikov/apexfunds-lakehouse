"""Response schemas for operational API endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class HealthCheckModel(BaseModel):
    ok: bool
    detail: str


class HealthResponseModel(BaseModel):
    status: str
    environment: str
    version: str
    checked_at: datetime
    checks: dict[str, HealthCheckModel]


class PipelineRunSummaryModel(BaseModel):
    pipeline_run_id: str
    pipeline_name: str
    source_system: str
    dataset_name: str
    trigger_mode: str
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    rows_read: int
    rows_written: int
    rows_quarantined: int
    files_discovered: int
    files_processed: int
    files_skipped: int
    error_code: str | None = None
    error_message: str | None = None
    details_json: dict[str, Any] = Field(default_factory=dict)


class PipelineRunListResponseModel(BaseModel):
    items: list[PipelineRunSummaryModel]
    count: int


class QualityResultSummaryModel(BaseModel):
    quality_result_id: str
    pipeline_run_id: str
    dataset_name: str
    layer_name: str
    rule_code: str
    rule_name: str
    severity: str
    status: str
    blocking: bool
    partition_key: str | None = None
    row_count_evaluated: int
    row_count_failed: int
    failure_ratio: float | None = None
    details_json: dict[str, Any] = Field(default_factory=dict)
    evaluated_at: datetime


class QualityResultListResponseModel(BaseModel):
    items: list[QualityResultSummaryModel]
    count: int
