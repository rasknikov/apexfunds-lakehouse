from __future__ import annotations

from datetime import date, datetime

from apex_lakehouse.config import PlatformSettings
from apex_lakehouse.storage.models import (
    ArtifactLayout,
    LakehouseDatasetLayout,
    LakehouseLayer,
    ObjectStoragePath,
    RawDatasetLayout,
)
from apex_lakehouse.storage.paths import (
    StoragePathBuilder,
    build_artifact_object_key,
    build_competence_partition,
    build_lakehouse_object_key,
    build_raw_object_key,
    build_year_month_partition,
)
from apex_lakehouse.time import Competence


def test_object_storage_path_uri_builds_s3_uri() -> None:
    path = ObjectStoragePath(bucket="raw", key="cvm/informe/arquivo.zip")

    assert path.uri == "s3://raw/cvm/informe/arquivo.zip"


def test_build_year_month_partition_formats_date() -> None:
    assert build_year_month_partition(date(2024, 1, 15)) == "ano=2024/mes=01"


def test_build_competence_partition_delegates_to_competence() -> None:
    assert build_competence_partition(Competence(2024, 2)) == "ano=2024/mes=02"


def test_build_raw_object_key_uses_competence_partition() -> None:
    layout = RawDatasetLayout(
        source_system="CVM",
        dataset_name="Informe Diario",
        file_name="arquivo.zip",
        competence=Competence(2024, 1),
    )

    assert (
        build_raw_object_key(layout)
        == "cvm/informe_diario/ano=2024/mes=01/arquivo.zip"
    )


def test_build_raw_object_key_falls_back_to_business_date_partition() -> None:
    layout = RawDatasetLayout(
        source_system="BCB",
        dataset_name="Selic-Diaria",
        file_name="payload.json",
        business_date=date(2024, 3, 7),
    )

    assert build_raw_object_key(layout) == "bcb/selic_diaria/ano=2024/mes=03/payload.json"


def test_build_lakehouse_object_key_handles_partition() -> None:
    layout = LakehouseDatasetLayout(
        layer=LakehouseLayer.SILVER,
        dataset_name="Fundos Informe Diario",
        partition_key="ano=2024/mes=01",
    )

    assert (
        build_lakehouse_object_key(layout)
        == "silver/fundos_informe_diario/ano=2024/mes=01"
    )


def test_build_artifact_object_key_uses_day_hierarchy() -> None:
    layout = ArtifactLayout(
        artifact_type="Quality Reports",
        artifact_name="quality_report.json",
        generated_at=datetime(2024, 1, 15, 12, 0, 0),
    )

    assert (
        build_artifact_object_key(layout)
        == "quality_reports/ano=2024/mes=01/dia=15/quality_report.json"
    )


def test_storage_path_builder_resolves_raw_bucket_from_settings() -> None:
    settings = PlatformSettings.from_env()
    builder = StoragePathBuilder(settings)
    layout = RawDatasetLayout(
        source_system="cvm",
        dataset_name="informe_diario",
        file_name="arquivo.zip",
        competence=Competence(2024, 1),
    )

    path = builder.raw(layout)

    assert path.bucket == settings.object_storage.raw_bucket
    assert path.key == "cvm/informe_diario/ano=2024/mes=01/arquivo.zip"


def test_storage_path_builder_resolves_lakehouse_layers() -> None:
    settings = PlatformSettings.from_env()
    builder = StoragePathBuilder(settings)

    bronze = builder.bronze("cvm_informe_diario")
    silver = builder.silver("fundos_informe_diario", partition_key="ano=2024/mes=01")
    gold = builder.gold("fato_fundo_diario")

    assert bronze.bucket == settings.object_storage.lakehouse_bucket
    assert bronze.key == "bronze/cvm_informe_diario"
    assert silver.key == "silver/fundos_informe_diario/ano=2024/mes=01"
    assert gold.key == "gold/fato_fundo_diario"


def test_storage_path_builder_resolves_artifacts_bucket() -> None:
    settings = PlatformSettings.from_env()
    builder = StoragePathBuilder(settings)
    layout = ArtifactLayout(
        artifact_type="quality_reports",
        artifact_name="report.json",
        generated_at=datetime(2024, 1, 15, 12, 0, 0),
    )

    path = builder.artifact(layout)

    assert path.bucket == settings.object_storage.artifacts_bucket
    assert path.key == "quality_reports/ano=2024/mes=01/dia=15/report.json"
