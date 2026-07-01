"""Canonical path builders for object storage and lakehouse datasets."""

from __future__ import annotations

from datetime import date, datetime

from apex_lakehouse.config import PlatformSettings, load_settings
from apex_lakehouse.storage.models import (
    ArtifactLayout,
    LakehouseDatasetLayout,
    LakehouseLayer,
    ObjectStoragePath,
    RawDatasetLayout,
)
from apex_lakehouse.time import Competence


def _sanitize_path_token(value: str) -> str:
    """
    Normalize one path token into a stable storage-safe value.

    We keep it simple on purpose:
    - trim spaces;
    - lowercase;
    - replace spaces and dashes with underscores.
    """
    normalized = value.strip().lower()
    normalized = normalized.replace(" ", "_")
    normalized = normalized.replace("-", "_")
    return normalized


def build_year_month_partition(value: date) -> str:
    """Return ano=YYYY/mes=MM for date-based layouts."""
    return f"ano={value.year:04d}/mes={value.month:02d}"


def build_raw_object_key(layout: RawDatasetLayout) -> str:
    """
    Build the canonical raw-zone object key.

    Examples:
    - cvm/informe_diario/ano=2024/mes=01/arquivo.zip
    - bcb/selic/ano=2024/mes=01/payload.json
    """
    source_system = _sanitize_path_token(layout.source_system)
    dataset_name = _sanitize_path_token(layout.dataset_name)

    parts = [source_system, dataset_name]

    if layout.competence is not None:
        parts.append(layout.competence.to_partition())
    elif layout.business_date is not None:
        parts.append(build_year_month_partition(layout.business_date))

    parts.append(layout.file_name)
    return "/".join(parts)


def build_lakehouse_object_key(layout: LakehouseDatasetLayout) -> str:
    """
    Build the canonical lakehouse dataset prefix.

    Examples:
    - bronze/cvm_informe_diario
    - silver/fundos_informe_diario/ano=2024/mes=01
    """
    dataset_name = _sanitize_path_token(layout.dataset_name)
    parts = [layout.layer.value, dataset_name]

    if layout.partition_key:
        parts.append(layout.partition_key)

    return "/".join(parts)


def build_artifact_object_key(layout: ArtifactLayout) -> str:
    """
    Build the canonical artifact key.

    Example:
    - quality_reports/ano=2024/mes=01/dia=15/quality_report.json
    """
    artifact_type = _sanitize_path_token(layout.artifact_type)
    generated_at = layout.generated_at

    return "/".join(
        [
            artifact_type,
            f"ano={generated_at.year:04d}",
            f"mes={generated_at.month:02d}",
            f"dia={generated_at.day:02d}",
            layout.artifact_name,
        ]
    )


class StoragePathBuilder:
    """
    Resolve logical storage layouts into concrete bucket/key addresses.

    This class knows bucket names from platform settings and keeps all path
    conventions in one place.
    """

    def __init__(self, settings: PlatformSettings):
        self._settings = settings

    @classmethod
    def from_settings(
        cls,
        settings: PlatformSettings | None = None,
    ) -> "StoragePathBuilder":
        return cls(settings or load_settings())

    def raw(self, layout: RawDatasetLayout) -> ObjectStoragePath:
        return ObjectStoragePath(
            bucket=self._settings.object_storage.raw_bucket,
            key=build_raw_object_key(layout),
        )

    def bronze(
        self,
        dataset_name: str,
        *,
        partition_key: str | None = None,
    ) -> ObjectStoragePath:
        return self._lakehouse(
            LakehouseDatasetLayout(
                layer=LakehouseLayer.BRONZE,
                dataset_name=dataset_name,
                partition_key=partition_key,
            )
        )

    def silver(
        self,
        dataset_name: str,
        *,
        partition_key: str | None = None,
    ) -> ObjectStoragePath:
        return self._lakehouse(
            LakehouseDatasetLayout(
                layer=LakehouseLayer.SILVER,
                dataset_name=dataset_name,
                partition_key=partition_key,
            )
        )

    def gold(
        self,
        dataset_name: str,
        *,
        partition_key: str | None = None,
    ) -> ObjectStoragePath:
        return self._lakehouse(
            LakehouseDatasetLayout(
                layer=LakehouseLayer.GOLD,
                dataset_name=dataset_name,
                partition_key=partition_key,
            )
        )

    def artifact(self, layout: ArtifactLayout) -> ObjectStoragePath:
        return ObjectStoragePath(
            bucket=self._settings.object_storage.artifacts_bucket,
            key=build_artifact_object_key(layout),
        )

    def _lakehouse(self, layout: LakehouseDatasetLayout) -> ObjectStoragePath:
        return ObjectStoragePath(
            bucket=self._settings.object_storage.lakehouse_bucket,
            key=build_lakehouse_object_key(layout),
        )


def build_competence_partition(competence: Competence) -> str:
    """Alias kept explicit because competence is a core business partition."""
    return competence.to_partition()