"""Shared type aliases and lightweight records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Mapping, NewType


CNPJ = NewType("CNPJ", str)
DatasetName = NewType("DatasetName", str)
PipelineRunId = NewType("PipelineRunId", str)
FileHash = NewType("FileHash", str)

JsonDict = Dict[str, Any]
JsonMapping = Mapping[str, Any]


@dataclass(frozen=True)
class SourceFileMetadata:
    """
    Metadata describing a source artifact before or after ingestion.
    """

    dataset_name: DatasetName
    source_system: str
    source_path: str
    local_path: Path | None
    business_date: date | None
    file_hash: FileHash | None
    detected_at: datetime