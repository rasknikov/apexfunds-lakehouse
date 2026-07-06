"""Service that promotes raw CVM artifacts into bronze lakehouse datasets."""

from __future__ import annotations

from pathlib import Path

from apex_lakehouse.control_plane.records import SourceFileRecord
from apex_lakehouse.exceptions import IngestionStateError
from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildRequest, BronzeBuildResult
from apex_lakehouse.ingestion.cvm.bronze_parser import CvmBronzeParser
from apex_lakehouse.storage.client import ObjectStorageClient
from apex_lakehouse.storage.models import ObjectStoragePath
from apex_lakehouse.storage.paths import (
    StoragePathBuilder,
    build_competence_partition,
    build_year_month_partition,
)
from apex_lakehouse.storage.staging import LocalStagingManager
from apex_lakehouse.time import Competence, utc_now
from apex_lakehouse.types import JsonDict


class CvmBronzeService:
    """Build canonical bronze CSV datasets from raw CVM source files."""

    def __init__(
        self,
        *,
        storage_client: ObjectStorageClient,
        path_builder: StoragePathBuilder,
        staging_manager: LocalStagingManager,
        parser: CvmBronzeParser,
    ):
        self._storage_client = storage_client
        self._path_builder = path_builder
        self._staging_manager = staging_manager
        self._parser = parser

    @classmethod
    def from_settings(cls) -> "CvmBronzeService":
        return cls(
            storage_client=ObjectStorageClient.from_settings(),
            path_builder=StoragePathBuilder.from_settings(),
            staging_manager=LocalStagingManager.from_project_paths(),
            parser=CvmBronzeParser(),
        )

    def build(self, request: BronzeBuildRequest) -> BronzeBuildResult:
        source_file = request.source_file
        raw_storage_path = _resolve_raw_storage_path(source_file)
        processed_at = utc_now()

        raw_stage_directory = self._staging_manager.prepare_directory(
            source_system=source_file.source_system,
            dataset_name=source_file.dataset_name,
        )
        staged_raw_path = raw_stage_directory / source_file.file_name
        self._storage_client.download_file(raw_storage_path, staged_raw_path)

        bronze_dataset_name = _build_bronze_dataset_name(source_file)
        bronze_stage_directory = self._staging_manager.prepare_directory(
            source_system=source_file.source_system,
            dataset_name=f"bronze_{source_file.dataset_name}",
        )
        output_path = bronze_stage_directory / _build_bronze_data_file_name(source_file)
        schema_path = bronze_stage_directory / _build_bronze_schema_file_name(source_file)

        parse_summary = self._parser.parse(
            source_path=staged_raw_path,
            output_path=output_path,
            schema_path=schema_path,
            source_file=source_file,
            pipeline_run_id=request.pipeline_run_id,
            processed_at=processed_at,
        )

        partition_key = _resolve_partition_key(source_file)
        data_path, published_schema_path = self._build_target_paths(
            source_file=source_file,
            bronze_dataset_name=bronze_dataset_name,
            partition_key=partition_key,
        )
        self._storage_client.upload_file(parse_summary.output_path, data_path, content_type="text/csv")
        self._storage_client.upload_file(
            parse_summary.schema_path,
            published_schema_path,
            content_type="application/json",
        )

        details: JsonDict = {
            "row_count": parse_summary.row_count,
            "column_count": len(parse_summary.columns),
            "schema_version": parse_summary.schema_version,
            "source_format": parse_summary.source_format,
            "delimiter": parse_summary.delimiter,
            "data_uri": data_path.uri,
            "schema_uri": published_schema_path.uri,
        }

        return BronzeBuildResult(
            request=request,
            bronze_dataset_name=bronze_dataset_name,
            partition_key=partition_key,
            parse_summary=parse_summary,
            data_path=data_path,
            schema_path=published_schema_path,
            details=details,
        )

    def _build_target_paths(
        self,
        *,
        source_file: SourceFileRecord,
        bronze_dataset_name: str,
        partition_key: str | None,
    ) -> tuple[ObjectStoragePath, ObjectStoragePath]:
        bronze_prefix = self._path_builder.bronze(
            bronze_dataset_name,
            partition_key=partition_key,
        )
        source_prefix = f"{bronze_prefix.key}/source_file_id={source_file.source_file_id}"
        return (
            ObjectStoragePath(
                bucket=bronze_prefix.bucket,
                key=f"{source_prefix}/part-00000.csv",
            ),
            ObjectStoragePath(
                bucket=bronze_prefix.bucket,
                key=f"{source_prefix}/schema.json",
            ),
        )


def _resolve_raw_storage_path(source_file: SourceFileRecord) -> ObjectStoragePath:
    if source_file.storage_bucket is None or source_file.storage_key is None:
        raise IngestionStateError(
            "Source file does not have a raw storage location and cannot be promoted to bronze."
        )
    return ObjectStoragePath(
        bucket=source_file.storage_bucket,
        key=source_file.storage_key,
    )


def _build_bronze_dataset_name(source_file: SourceFileRecord) -> str:
    return f"{source_file.source_system}_{source_file.dataset_name}"


def _build_bronze_data_file_name(source_file: SourceFileRecord) -> str:
    return f"{source_file.source_file_id}.csv"


def _build_bronze_schema_file_name(source_file: SourceFileRecord) -> str:
    return f"{source_file.source_file_id}.schema.json"


def _resolve_partition_key(source_file: SourceFileRecord) -> str | None:
    if source_file.competence is not None:
        return build_competence_partition(Competence.from_string(source_file.competence))
    if source_file.business_date is not None:
        return build_year_month_partition(source_file.business_date)
    return build_year_month_partition(source_file.first_seen_at.date())
