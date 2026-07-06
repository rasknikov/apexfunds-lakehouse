"""Service that promotes CVM bronze datasets into conformed silver outputs."""

from __future__ import annotations

from pathlib import Path

from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildResult
from apex_lakehouse.ingestion.cvm.silver_models import SilverBuildRequest, SilverBuildResult
from apex_lakehouse.ingestion.cvm.silver_transformer import (
    CvmSilverTransformer,
    SilverTransformRequest,
)
from apex_lakehouse.storage.client import ObjectStorageClient
from apex_lakehouse.storage.models import ObjectStoragePath
from apex_lakehouse.storage.paths import StoragePathBuilder
from apex_lakehouse.storage.staging import LocalStagingManager
from apex_lakehouse.time import utc_now
from apex_lakehouse.types import JsonDict

SILVER_DATASET_NAME_BY_BRONZE_DATASET = {
    "cadastro_fundos": "fundos",
    "informe_diario": "fundos_informe_diario",
    "perfil_mensal": "fundos_perfil_mensal",
}


class CvmSilverService:
    """Build canonical silver outputs from CVM bronze datasets."""

    def __init__(
        self,
        *,
        storage_client: ObjectStorageClient,
        path_builder: StoragePathBuilder,
        staging_manager: LocalStagingManager,
        transformer: CvmSilverTransformer,
    ):
        self._storage_client = storage_client
        self._path_builder = path_builder
        self._staging_manager = staging_manager
        self._transformer = transformer

    @classmethod
    def from_settings(cls) -> "CvmSilverService":
        return cls(
            storage_client=ObjectStorageClient.from_settings(),
            path_builder=StoragePathBuilder.from_settings(),
            staging_manager=LocalStagingManager.from_project_paths(),
            transformer=CvmSilverTransformer(),
        )

    def build(self, request: SilverBuildRequest) -> SilverBuildResult:
        primary_input = request.primary_input
        source_file = primary_input.request.source_file
        processed_at = utc_now()
        primary_stage_path = self._stage_input_file(primary_input, source_file.dataset_name)
        cadastro_stage_path = (
            self._stage_input_file(request.cadastro_input, "cadastro_reference")
            if request.cadastro_input is not None
            else None
        )

        silver_dataset_name = SILVER_DATASET_NAME_BY_BRONZE_DATASET[source_file.dataset_name]
        silver_stage_directory = self._staging_manager.prepare_directory(
            source_system=source_file.source_system,
            dataset_name=f"silver_{source_file.dataset_name}",
        )
        output_path = silver_stage_directory / f"{source_file.source_file_id}.csv"
        schema_path = silver_stage_directory / f"{source_file.source_file_id}.schema.json"

        transform_summary = self._transformer.transform(
            SilverTransformRequest(
                dataset_name=source_file.dataset_name,
                input_path=primary_stage_path,
                output_path=output_path,
                schema_path=schema_path,
                source_file_id=source_file.source_file_id,
                source_file_name=source_file.file_name,
                source_url=source_file.source_url,
                processed_at=processed_at,
                pipeline_run_id=request.pipeline_run_id,
                cadastro_input_path=cadastro_stage_path,
            )
        )

        data_path, published_schema_path = self._build_target_paths(
            silver_dataset_name=silver_dataset_name,
            partition_key=primary_input.partition_key,
            source_file_id=str(source_file.source_file_id),
        )
        self._storage_client.upload_file(transform_summary.output_path, data_path, content_type="text/csv")
        self._storage_client.upload_file(
            transform_summary.schema_path,
            published_schema_path,
            content_type="application/json",
        )

        details: JsonDict = {
            "row_count": transform_summary.row_count,
            "deduplicated_rows": transform_summary.deduplicated_rows,
            "column_count": len(transform_summary.columns),
            "schema_version": transform_summary.schema_version,
            "data_uri": data_path.uri,
            "schema_uri": published_schema_path.uri,
            "cadastro_enrichment": request.cadastro_input is not None,
        }

        return SilverBuildResult(
            request=request,
            silver_dataset_name=silver_dataset_name,
            partition_key=primary_input.partition_key,
            transform_summary=transform_summary,
            data_path=data_path,
            schema_path=published_schema_path,
            details=details,
        )

    def _stage_input_file(
        self,
        bronze_result: BronzeBuildResult,
        dataset_name: str,
    ) -> Path:
        staging_directory = self._staging_manager.prepare_directory(
            source_system=bronze_result.request.source_file.source_system,
            dataset_name=dataset_name,
        )
        destination_path = staging_directory / Path(bronze_result.data_path.key).name
        return self._storage_client.download_file(bronze_result.data_path, destination_path)

    def _build_target_paths(
        self,
        *,
        silver_dataset_name: str,
        partition_key: str | None,
        source_file_id: str,
    ) -> tuple[ObjectStoragePath, ObjectStoragePath]:
        silver_prefix = self._path_builder.silver(
            silver_dataset_name,
            partition_key=partition_key,
        )
        source_prefix = f"{silver_prefix.key}/source_file_id={source_file_id}"
        return (
            ObjectStoragePath(
                bucket=silver_prefix.bucket,
                key=f"{source_prefix}/part-00000.csv",
            ),
            ObjectStoragePath(
                bucket=silver_prefix.bucket,
                key=f"{source_prefix}/schema.json",
            ),
        )
