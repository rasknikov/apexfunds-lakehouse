from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from apex_lakehouse.storage.client import RemoteObjectMetadata
from apex_lakehouse.storage.files import LocalFileMetadata
from apex_lakehouse.storage.models import ArtifactLayout, ObjectStoragePath, RawDatasetLayout
from apex_lakehouse.storage.operations import DownloadedObject, PublishedRawObject, StorageOperations
from apex_lakehouse.storage.staging import StagingLocation
from apex_lakehouse.time import Competence


def test_publish_raw_file_coordinates_metadata_upload_and_head(tmp_path: Path) -> None:
    source_file = tmp_path / "arquivo.zip"
    source_file.write_text("payload", encoding="utf-8")
    local_metadata = LocalFileMetadata(
        path=source_file.resolve(),
        size_bytes=7,
        content_hash="abc123",
        hash_algorithm="sha256",
        detected_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
    )
    storage_path = ObjectStoragePath(bucket="raw", key="cvm/informe/arquivo.zip")
    remote_metadata = RemoteObjectMetadata(
        bucket="raw",
        key="cvm/informe/arquivo.zip",
        etag="etag123",
        size_bytes=7,
        last_modified_at=datetime(2024, 1, 15, 10, 1, 0, tzinfo=timezone.utc),
        content_type="application/zip",
    )

    client = MagicMock()
    client.head.return_value = remote_metadata
    path_builder = MagicMock()
    path_builder.raw.return_value = storage_path
    staging_manager = MagicMock()
    operations = StorageOperations(
        client=client,
        path_builder=path_builder,
        staging_manager=staging_manager,
    )

    from apex_lakehouse.storage import operations as operations_module

    original_collect = operations_module.collect_local_file_metadata
    operations_module.collect_local_file_metadata = MagicMock(return_value=local_metadata)
    try:
        result = operations.publish_raw_file(
            source_file,
            layout=RawDatasetLayout(
                source_system="cvm",
                dataset_name="informe_diario",
                file_name="arquivo.zip",
                competence=Competence(2024, 1),
            ),
            content_type="application/zip",
        )
    finally:
        operations_module.collect_local_file_metadata = original_collect

    assert isinstance(result, PublishedRawObject)
    assert result.local_metadata == local_metadata
    assert result.storage_path == storage_path
    assert result.remote_metadata == remote_metadata
    client.upload_file.assert_called_once_with(
        local_metadata.path,
        storage_path,
        content_type="application/zip",
    )
    client.head.assert_called_once_with(storage_path)


def test_stage_download_coordinates_download_and_local_metadata(tmp_path: Path) -> None:
    source = ObjectStoragePath(bucket="raw", key="cvm/informe/arquivo.zip")
    staging_directory = (tmp_path / "staging" / "cvm" / "informe_diario").resolve()
    downloaded_path = (staging_directory / "arquivo.zip").resolve()
    local_metadata = LocalFileMetadata(
        path=downloaded_path,
        size_bytes=10,
        content_hash="def456",
        hash_algorithm="sha256",
        detected_at=datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc),
    )

    client = MagicMock()
    client.download_file.return_value = downloaded_path
    path_builder = MagicMock()
    staging_manager = MagicMock()
    staging_manager.prepare_directory.return_value = staging_directory
    operations = StorageOperations(
        client=client,
        path_builder=path_builder,
        staging_manager=staging_manager,
    )

    from apex_lakehouse.storage import operations as operations_module

    original_collect = operations_module.collect_local_file_metadata
    operations_module.collect_local_file_metadata = MagicMock(return_value=local_metadata)
    try:
        result = operations.stage_download(
            source,
            source_system="cvm",
            dataset_name="informe_diario",
        )
    finally:
        operations_module.collect_local_file_metadata = original_collect

    assert isinstance(result, DownloadedObject)
    assert result.storage_path == source
    assert result.local_metadata == local_metadata
    assert result.staging_location == StagingLocation(
        directory=staging_directory,
        file_path=downloaded_path,
    )
    client.download_file.assert_called_once_with(source, downloaded_path)


def test_stage_download_respects_target_file_name_override(tmp_path: Path) -> None:
    source = ObjectStoragePath(bucket="raw", key="cvm/informe/original.zip")
    staging_directory = (tmp_path / "staging" / "cvm" / "informe_diario").resolve()
    downloaded_path = (staging_directory / "renamed.zip").resolve()

    client = MagicMock()
    client.download_file.return_value = downloaded_path
    path_builder = MagicMock()
    staging_manager = MagicMock()
    staging_manager.prepare_directory.return_value = staging_directory
    operations = StorageOperations(
        client=client,
        path_builder=path_builder,
        staging_manager=staging_manager,
    )

    from apex_lakehouse.storage import operations as operations_module

    original_collect = operations_module.collect_local_file_metadata
    operations_module.collect_local_file_metadata = MagicMock(
        return_value=LocalFileMetadata(
            path=downloaded_path,
            size_bytes=1,
            content_hash="x",
            hash_algorithm="sha256",
            detected_at=datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc),
        )
    )
    try:
        operations.stage_download(
            source,
            source_system="cvm",
            dataset_name="informe_diario",
            target_file_name="renamed.zip",
        )
    finally:
        operations_module.collect_local_file_metadata = original_collect

    client.download_file.assert_called_once_with(source, downloaded_path)


def test_artifact_exists_resolves_path_and_delegates_to_client() -> None:
    artifact_layout = ArtifactLayout(
        artifact_type="quality_reports",
        artifact_name="report.json",
        generated_at=datetime(2024, 1, 15, 12, 0, 0),
    )
    artifact_path = ObjectStoragePath(bucket="artifacts", key="quality_reports/report.json")
    client = MagicMock()
    client.exists.return_value = True
    path_builder = MagicMock()
    path_builder.artifact.return_value = artifact_path
    operations = StorageOperations(
        client=client,
        path_builder=path_builder,
        staging_manager=MagicMock(),
    )

    result = operations.artifact_exists(artifact_layout)

    assert result is True
    path_builder.artifact.assert_called_once_with(artifact_layout)
    client.exists.assert_called_once_with(artifact_path)


def test_head_raw_object_resolves_path_and_delegates_to_client() -> None:
    layout = RawDatasetLayout(
        source_system="cvm",
        dataset_name="informe_diario",
        file_name="arquivo.zip",
        competence=Competence(2024, 1),
    )
    storage_path = ObjectStoragePath(bucket="raw", key="cvm/informe/arquivo.zip")
    remote_metadata = RemoteObjectMetadata(
        bucket="raw",
        key="cvm/informe/arquivo.zip",
        etag="etag123",
        size_bytes=7,
        last_modified_at=datetime(2024, 1, 15, 10, 1, 0, tzinfo=timezone.utc),
        content_type="application/zip",
    )
    client = MagicMock()
    client.head.return_value = remote_metadata
    path_builder = MagicMock()
    path_builder.raw.return_value = storage_path
    operations = StorageOperations(
        client=client,
        path_builder=path_builder,
        staging_manager=MagicMock(),
    )

    result = operations.head_raw_object(layout)

    assert result == remote_metadata
    path_builder.raw.assert_called_once_with(layout)
    client.head.assert_called_once_with(storage_path)
