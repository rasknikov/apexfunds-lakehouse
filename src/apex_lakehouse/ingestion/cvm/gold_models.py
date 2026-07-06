"""Contracts for CVM gold marts and analytical datasets."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from uuid import UUID

from apex_lakehouse.ingestion.cvm.silver_models import SilverBuildResult
from apex_lakehouse.storage.models import ObjectStoragePath
from apex_lakehouse.types import JsonDict

GoldColumnType = Literal["string", "integer", "decimal", "date", "timestamp"]

DEFAULT_GOLD_SCHEMA_VERSION = "v1"


@dataclass(frozen=True)
class GoldColumnSchema:
    name: str
    data_type: GoldColumnType
    nullable: bool
    technical: bool = False


@dataclass(frozen=True)
class GoldDatasetSummary:
    dataset_name: str
    output_path: Path
    schema_path: Path
    row_count: int
    columns: tuple[GoldColumnSchema, ...]
    partition_key: str | None = None
    schema_version: str = DEFAULT_GOLD_SCHEMA_VERSION


@dataclass(frozen=True)
class GoldMartBuildRequest:
    funds_input: SilverBuildResult
    informe_input: SilverBuildResult
    updated_by: str
    pipeline_run_id: UUID | None = None


@dataclass(frozen=True)
class GoldDatasetResult:
    summary: GoldDatasetSummary
    data_path: ObjectStoragePath
    schema_path: ObjectStoragePath


@dataclass(frozen=True)
class GoldMartBuildResult:
    request: GoldMartBuildRequest
    outputs: list[GoldDatasetResult]
    details: JsonDict = field(default_factory=dict)
