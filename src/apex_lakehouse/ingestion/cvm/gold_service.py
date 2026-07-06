"""Service that publishes CVM gold marts from silver inputs."""

from __future__ import annotations

from pathlib import Path

from apex_lakehouse.ingestion.cvm.gold_models import (
    GoldDatasetResult,
    GoldMartBuildRequest,
    GoldMartBuildResult,
)
from apex_lakehouse.ingestion.cvm.gold_transformer import (
    CvmGoldTransformer,
    GoldMartTransformRequest,
)
from apex_lakehouse.ingestion.cvm.silver_models import SilverBuildResult
from apex_lakehouse.storage.client import ObjectStorageClient
from apex_lakehouse.storage.models import ObjectStoragePath
from apex_lakehouse.storage.paths import StoragePathBuilder
from apex_lakehouse.storage.staging import LocalStagingManager
from apex_lakehouse.time import utc_now
from apex_lakehouse.types import JsonDict


class CvmGoldService:
    """Build and publish analytical gold datasets for funds."""

    def __init__(
        self,
        *,
        storage_client: ObjectStorageClient,
        path_builder: StoragePathBuilder,
        staging_manager: LocalStagingManager,
        transformer: CvmGoldTransformer,
    ):
        self._storage_client = storage_client
        self._path_builder = path_builder
        self._staging_manager = staging_manager
        self._transformer = transformer

    @classmethod
    def from_settings(cls) -> "CvmGoldService":
        return cls(
            storage_client=ObjectStorageClient.from_settings(),
            path_builder=StoragePathBuilder.from_settings(),
            staging_manager=LocalStagingManager.from_project_paths(),
            transformer=CvmGoldTransformer(),
        )

    def build(self, request: GoldMartBuildRequest) -> GoldMartBuildResult:
        generated_at = utc_now()
        funds_path = self._resolve_local_silver_path(request.funds_input, "gold_funds")
        informe_path = self._resolve_local_silver_path(request.informe_input, "gold_informe")
        output_directory = self._staging_manager.prepare_directory(
            source_system="cvm",
            dataset_name="gold_funds_mart",
        )
        summaries = self._transformer.transform(
            GoldMartTransformRequest(
                funds_input_path=funds_path,
                informe_input_path=informe_path,
                output_directory=output_directory,
                generated_at=generated_at,
                pipeline_run_id=request.pipeline_run_id,
                partition_key=request.informe_input.partition_key,
            )
        )

        outputs: list[GoldDatasetResult] = []
        for summary in summaries:
            source_file_id = self._resolve_source_file_id_for_dataset(summary.dataset_name, request)
            data_path, schema_path = self._build_target_paths(
                dataset_name=summary.dataset_name,
                partition_key=summary.partition_key,
                source_file_id=source_file_id,
            )
            self._storage_client.upload_file(summary.output_path, data_path, content_type="text/csv")
            self._storage_client.upload_file(summary.schema_path, schema_path, content_type="application/json")
            outputs.append(
                GoldDatasetResult(
                    summary=summary,
                    data_path=data_path,
                    schema_path=schema_path,
                )
            )

        details: JsonDict = {
            "dataset_count": len(outputs),
            "total_rows": sum(output.summary.row_count for output in outputs),
        }
        return GoldMartBuildResult(
            request=request,
            outputs=outputs,
            details=details,
        )

    def _resolve_local_silver_path(
        self,
        silver_result: SilverBuildResult,
        dataset_name: str,
    ) -> Path:
        local_path = silver_result.transform_summary.output_path
        if local_path.exists():
            return local_path.resolve()

        staging_directory = self._staging_manager.prepare_directory(
            source_system="cvm",
            dataset_name=dataset_name,
        )
        destination_path = staging_directory / Path(silver_result.data_path.key).name
        return self._storage_client.download_file(silver_result.data_path, destination_path)

    def _build_target_paths(
        self,
        *,
        dataset_name: str,
        partition_key: str | None,
        source_file_id: str,
    ) -> tuple[ObjectStoragePath, ObjectStoragePath]:
        gold_prefix = self._path_builder.gold(
            dataset_name,
            partition_key=partition_key,
        )
        source_prefix = f"{gold_prefix.key}/source_file_id={source_file_id}"
        return (
            ObjectStoragePath(
                bucket=gold_prefix.bucket,
                key=f"{source_prefix}/part-00000.csv",
            ),
            ObjectStoragePath(
                bucket=gold_prefix.bucket,
                key=f"{source_prefix}/schema.json",
            ),
        )

    def _resolve_source_file_id_for_dataset(
        self,
        dataset_name: str,
        request: GoldMartBuildRequest,
    ) -> str:
        if dataset_name == "dim_fundo":
            return str(request.funds_input.request.primary_input.request.source_file.source_file_id)
        return str(request.informe_input.request.primary_input.request.source_file.source_file_id)
