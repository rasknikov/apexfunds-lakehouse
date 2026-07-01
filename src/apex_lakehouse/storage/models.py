from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum

from apex_lakehouse.time import Competence


class StorageZone(str, Enum):
    RAW = "raw"
    LAKEHOUSE = "lakehouse"
    ARTIFACTS = "artifacts"


class LakehouseLayer(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass(frozen=True)
class ObjectStoragePath:
    bucket: str
    key: str

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"
    

@dataclass(frozen=True)
class RawDatasetLayout:
    source_system: str
    dataset_name: str
    file_name: str
    competence: Competence | None = None
    business_date: date | None = None


@dataclass(frozen=True)
class LakehouseDatasetLayout:
    layer: LakehouseLayer
    dataset_name: str
    partition_key: str | None = None


@dataclass(frozen=True)
class ArtifactLayout:
    artifact_type: str
    artifact_name: str
    generated_at: datetime


