"""Contracts for quarantine generation and replay operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from apex_lakehouse.control_plane.records import QuarantineRecord
from apex_lakehouse.quality.models import DatasetQualityEvaluation
from apex_lakehouse.types import JsonDict


@dataclass(frozen=True)
class QuarantineBuildRequest:
    evaluation: DatasetQualityEvaluation
    source_system: str
    dataset_name: str
    layer_name: str
    include_non_blocking: bool = False


@dataclass(frozen=True)
class QuarantineBuildResult:
    request: QuarantineBuildRequest
    records: list[QuarantineRecord]
    created_at: datetime
    details: JsonDict = field(default_factory=dict)


@dataclass(frozen=True)
class QuarantineReplayRequest:
    quarantine_id: UUID
    requested_at: datetime
    resolution_note: str | None = None
