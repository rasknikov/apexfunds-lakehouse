"""Contracts for CVM bronze parsing and publication."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from uuid import UUID

from apex_lakehouse.control_plane.records import SourceFileRecord
from apex_lakehouse.storage.models import ObjectStoragePath
from apex_lakehouse.types import JsonDict

BronzeColumnType = Literal["string", "integer", "decimal", "date", "timestamp"]

DEFAULT_BRONZE_SCHEMA_VERSION = "v1"


@dataclass(frozen=True)
class BronzeColumnSchema:
    name: str
    data_type: BronzeColumnType
    nullable: bool
    technical: bool = False


@dataclass(frozen=True)
class BronzeParseSummary:
    output_path: Path
    schema_path: Path
    row_count: int
    columns: tuple[BronzeColumnSchema, ...]
    schema_version: str = DEFAULT_BRONZE_SCHEMA_VERSION
    delimiter: str = ","
    source_format: str = "csv"


@dataclass(frozen=True)
class BronzeBuildRequest:
    source_file: SourceFileRecord
    updated_by: str
    pipeline_run_id: UUID | None = None


@dataclass(frozen=True)
class BronzeBuildResult:
    request: BronzeBuildRequest
    bronze_dataset_name: str
    partition_key: str | None
    parse_summary: BronzeParseSummary
    data_path: ObjectStoragePath
    schema_path: ObjectStoragePath
    details: JsonDict = field(default_factory=dict)
