from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from apex_lakehouse.config import PlatformSettings
from apex_lakehouse.storage.client import (
    ObjectStorageClient,
    RemoteObjectMetadata,
    _clean_etag,
    _normalize_last_modified,
)
from apex_lakehouse.storage.models import ObjectStoragePath


def _not_found_error() -> ClientError:
    return ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadObject",
    )


def _access_denied_error() -> ClientError:
    return ClientError(
        {"Error": {"Code": "403", "Message": "Access denied"}},
        "HeadObject",
    )


def test_remote_object_metadata_uri_builds_s3_uri() -> None:
    metadata = RemoteObjectMetadata(
        bucket="raw",
        key="cvm/informe.zip",
        etag="abc",
        size_bytes=10,
        last_modified_at=None,
        content_type="application/zip",
    )

    assert metadata.uri == "s3://raw/cvm/informe.zip"


def test_from_settings_builds_boto3_client() -> None:
    settings = PlatformSettings.from_env()
    fake_client = MagicMock()

    with patch("apex_lakehouse.storage.client.boto3.client", return_value=fake_client) as mock_boto:
        client = ObjectStorageClient.from_settings(settings)

    mock_boto.assert_called_once_with(
        "s3",
        endpoint_url=settings.object_storage.endpoint,
        aws_access_key_id=settings.object_storage.access_key,
        aws_secret_access_key=settings.object_storage.secret_key,
        region_name=settings.object_storage.region,
    )
    assert isinstance(client, ObjectStorageClient)


def test_exists_returns_true_when_head_succeeds() -> None:
    sdk_client = MagicMock()
    client = ObjectStorageClient(sdk_client)
    path = ObjectStoragePath(bucket="raw", key="cvm/informe.zip")

    assert client.exists(path) is True
    sdk_client.head_object.assert_called_once_with(Bucket="raw", Key="cvm/informe.zip")


def test_exists_returns_false_for_not_found_errors() -> None:
    sdk_client = MagicMock()
    sdk_client.head_object.side_effect = _not_found_error()
    client = ObjectStorageClient(sdk_client)

    assert client.exists(ObjectStoragePath(bucket="raw", key="missing.zip")) is False


def test_exists_reraises_non_not_found_errors() -> None:
    sdk_client = MagicMock()
    sdk_client.head_object.side_effect = _access_denied_error()
    client = ObjectStorageClient(sdk_client)

    with pytest.raises(ClientError):
        client.exists(ObjectStoragePath(bucket="raw", key="private.zip"))


def test_head_returns_normalized_metadata() -> None:
    sdk_client = MagicMock()
    sdk_client.head_object.return_value = {
        "ETag": '"abc123"',
        "ContentLength": 42,
        "LastModified": datetime(2024, 1, 15, 10, 0, 0),
        "ContentType": "application/json",
    }
    client = ObjectStorageClient(sdk_client)
    path = ObjectStoragePath(bucket="artifacts", key="report.json")

    metadata = client.head(path)

    assert metadata == RemoteObjectMetadata(
        bucket="artifacts",
        key="report.json",
        etag="abc123",
        size_bytes=42,
        last_modified_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        content_type="application/json",
    )


def test_head_returns_none_for_missing_object() -> None:
    sdk_client = MagicMock()
    sdk_client.head_object.side_effect = _not_found_error()
    client = ObjectStorageClient(sdk_client)

    assert client.head(ObjectStoragePath(bucket="raw", key="missing.zip")) is None


def test_upload_file_uses_resolved_local_file_and_optional_content_type(tmp_path: Path) -> None:
    sdk_client = MagicMock()
    client = ObjectStorageClient(sdk_client)
    source_file = tmp_path / "payload.json"
    source_file.write_text("{}", encoding="utf-8")
    destination = ObjectStoragePath(bucket="artifacts", key="reports/payload.json")

    client.upload_file(source_file, destination, content_type="application/json")

    sdk_client.upload_file.assert_called_once_with(
        Filename=str(source_file.resolve()),
        Bucket="artifacts",
        Key="reports/payload.json",
        ExtraArgs={"ContentType": "application/json"},
    )


def test_upload_bytes_uses_fileobj_upload() -> None:
    sdk_client = MagicMock()
    client = ObjectStorageClient(sdk_client)
    destination = ObjectStoragePath(bucket="artifacts", key="reports/manifest.json")

    client.upload_bytes(b'{"ok":true}', destination, content_type="application/json")

    _, kwargs = sdk_client.upload_fileobj.call_args
    assert kwargs["Bucket"] == "artifacts"
    assert kwargs["Key"] == "reports/manifest.json"
    assert kwargs["ExtraArgs"] == {"ContentType": "application/json"}
    assert kwargs["Fileobj"].read() == b'{"ok":true}'


def test_download_file_creates_parent_directory_and_returns_resolved_path(tmp_path: Path) -> None:
    sdk_client = MagicMock()
    client = ObjectStorageClient(sdk_client)
    source = ObjectStoragePath(bucket="raw", key="cvm/informe.zip")
    destination = tmp_path / "nested" / "informe.zip"

    resolved = client.download_file(source, destination)

    assert resolved == destination.resolve()
    assert destination.parent.exists()
    sdk_client.download_file.assert_called_once_with(
        Bucket="raw",
        Key="cvm/informe.zip",
        Filename=str(destination.resolve()),
    )


def test_download_bytes_returns_body_payload() -> None:
    sdk_client = MagicMock()
    body = MagicMock()
    body.read.return_value = b"payload"
    sdk_client.get_object.return_value = {"Body": body}
    client = ObjectStorageClient(sdk_client)

    payload = client.download_bytes(ObjectStoragePath(bucket="raw", key="file.bin"))

    assert payload == b"payload"
    sdk_client.get_object.assert_called_once_with(Bucket="raw", Key="file.bin")


def test_delete_delegates_to_sdk() -> None:
    sdk_client = MagicMock()
    client = ObjectStorageClient(sdk_client)

    client.delete(ObjectStoragePath(bucket="artifacts", key="old/report.json"))

    sdk_client.delete_object.assert_called_once_with(
        Bucket="artifacts",
        Key="old/report.json",
    )


def test_clean_etag_removes_quotes() -> None:
    assert _clean_etag('"abc123"') == "abc123"
    assert _clean_etag(None) is None


def test_normalize_last_modified_coerces_to_utc() -> None:
    naive = datetime(2024, 1, 15, 10, 0, 0)
    aware = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    assert _normalize_last_modified(naive) == aware
    assert _normalize_last_modified(aware) == aware
    assert _normalize_last_modified(None) is None
