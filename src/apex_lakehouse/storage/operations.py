from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from apex_lakehouse.storage.client import ObjectStorageClient, RemoteObjectMetadata
from apex_lakehouse.storage.files import LocalFileMetadata, collect_local_file_metadata
from apex_lakehouse.storage.models import ArtifactLayout, ObjectStoragePath, RawDatasetLayout
from apex_lakehouse.storage.paths import StoragePathBuilder
from apex_lakehouse.storage.staging import LocalStagingManager, StagingLocation


@dataclass(frozen=True)
class PublishedRawObject:
    local_metadata: LocalFileMetadata
    storage_path: ObjectStoragePath
    remote_metadata: RemoteObjectMetadata | None


@dataclass(frozen=True)
class DownloadedObject:
    storage_path: ObjectStoragePath
    staging_location: StagingLocation
    local_metadata: LocalFileMetadata


class StorageOperations:
    def __init__(
            self,
            *,
            client: ObjectStorageClient,
            path_builder: StoragePathBuilder,
            staging_manager: LocalStagingManager,
    ):
        self._client = client
        self._path_builder = path_builder
        self._staging_manager = staging_manager

    @classmethod
    def from_settings(cls) -> "StorageOperations":
        return cls(
            client=ObjectStorageClient.from_settings(),
            path_builder=StoragePathBuilder.from_settings(),
            staging_manager=LocalStagingManager.from_project_paths(),
        )
    
    def publish_raw_file(
            self,
            source_path: Path,
            *,
            layout: RawDatasetLayout,
            content_type: str | None = None,
    ) -> PublishedRawObject:
        local_metadata = collect_local_file_metadata(source_path)
        storage_path = self._path_builder.raw(layout)

        self._client.upload_file(
            local_metadata.path,
            storage_path,
            content_type=content_type,
        )

        remote_metadata = self._client.head(storage_path)
        return PublishedRawObject(
            local_metadata=local_metadata,
            storage_path=storage_path,
            remote_metadata=remote_metadata,
        )
    
    def stage_download(
            self,
            source: ObjectStoragePath,
            *,
            source_system: str,
            dataset_name: str,
            target_file_name: str | None = None,
    ) -> DownloadedObject:
        staging_directory = self._staging_manager.prepare_directory(
            source_system=source_system,
            dataset_name=dataset_name,
        )

        destination_path = staging_directory / (
            target_file_name or Path(source.key).name
        )

        resolved_path = self._client.download_file(source, destination_path)
        local_metadata = collect_local_file_metadata(resolved_path)

        return DownloadedObject(
            storage_path=source,
            staging_location=StagingLocation(
                directory=staging_directory,
                file_path=resolved_path,
            ),
            local_metadata=local_metadata,
        )
    
    def artifact_exists(self, layout: ArtifactLayout) -> bool:

        path = self._path_builder.artifact(layout)
        return self._client.exists(path)

    def head_raw_object(self, layout: RawDatasetLayout) -> RemoteObjectMetadata | None:
        path = self._path_builder.raw(layout)
        return self._client.head(path)
