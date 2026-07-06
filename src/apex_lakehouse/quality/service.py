"""Service layer for quality evaluation plus persistence."""

from __future__ import annotations

from pathlib import Path

from apex_lakehouse.control_plane.repository import ControlPlaneRepository
from apex_lakehouse.ingestion.cvm.silver_models import SilverBuildResult
from apex_lakehouse.quality.engine import DatasetQualityEngine
from apex_lakehouse.quality.models import DatasetQualityEvaluation, DatasetQualityRequest
from apex_lakehouse.storage.client import ObjectStorageClient
from apex_lakehouse.storage.staging import LocalStagingManager


class DatasetQualityService:
    """Evaluate a dataset, persist results and compute promotion gating."""

    def __init__(
        self,
        *,
        repository: ControlPlaneRepository,
        engine: DatasetQualityEngine,
        storage_client: ObjectStorageClient,
        staging_manager: LocalStagingManager,
    ):
        self._repository = repository
        self._engine = engine
        self._storage_client = storage_client
        self._staging_manager = staging_manager

    @classmethod
    def from_repository(
        cls,
        repository: ControlPlaneRepository,
    ) -> "DatasetQualityService":
        return cls(
            repository=repository,
            engine=DatasetQualityEngine(),
            storage_client=ObjectStorageClient.from_settings(),
            staging_manager=LocalStagingManager.from_project_paths(),
        )

    def evaluate(self, request: DatasetQualityRequest) -> DatasetQualityEvaluation:
        local_dataset_path = self._resolve_local_dataset_path(request.silver_result)
        evaluation = self._engine.evaluate(
            request,
            local_dataset_path=local_dataset_path,
        )
        for record in evaluation.records:
            self._repository.insert_data_quality_result(record)
        return evaluation

    def _resolve_local_dataset_path(self, silver_result: SilverBuildResult) -> Path:
        local_path = silver_result.transform_summary.output_path
        if local_path.exists():
            return local_path.resolve()

        staging_directory = self._staging_manager.prepare_directory(
            source_system=silver_result.request.primary_input.request.source_file.source_system,
            dataset_name=f"quality_{silver_result.silver_dataset_name}",
        )
        destination_path = staging_directory / Path(silver_result.data_path.key).name
        return self._storage_client.download_file(
            silver_result.data_path,
            destination_path,
        )
