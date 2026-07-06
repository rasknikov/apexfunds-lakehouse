"""Contracts for CVM silver transformations."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from uuid import UUID

from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildResult
from apex_lakehouse.storage.models import ObjectStoragePath
from apex_lakehouse.types import JsonDict

SilverColumnType = Literal["string", "integer", "decimal", "date", "timestamp"]

DEFAULT_SILVER_SCHEMA_VERSION = "v1"


@dataclass(frozen=True)
class SilverColumnSchema:
    name: str
    data_type: SilverColumnType
    nullable: bool
    technical: bool = False


@dataclass(frozen=True)
class SilverTransformSummary:
    output_path: Path
    schema_path: Path
    row_count: int
    deduplicated_rows: int
    columns: tuple[SilverColumnSchema, ...]
    schema_version: str = DEFAULT_SILVER_SCHEMA_VERSION
    input_dataset_name: str = ""


@dataclass(frozen=True)
class SilverBuildRequest:
    primary_input: BronzeBuildResult
    updated_by: str
    pipeline_run_id: UUID | None = None
    cadastro_input: BronzeBuildResult | None = None


@dataclass(frozen=True)
class SilverBuildResult:
    request: SilverBuildRequest
    silver_dataset_name: str
    partition_key: str | None
    transform_summary: SilverTransformSummary
    data_path: ObjectStoragePath
    schema_path: ObjectStoragePath
    details: JsonDict = field(default_factory=dict)
