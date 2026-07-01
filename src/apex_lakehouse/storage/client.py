from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import BinaryIO, Protocol

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from apex_lakehouse.config import PlatformSettings, load_settings
from apex_lakehouse.storage.files import ensure_file_exists
from apex_lakehouse.storage.models import ObjectStoragePath


@dataclass(frozen=True)
class RemoteObjectMetadata:
    bucket: str
    key: str
    etag: str | None
    size_bytes: int | None
    last_modified_at: datetime | None
    content_type: str | None

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"
    

class SupportsRead(Protocol):
    def read(self, size: int = -1) -> bytes:
        ...


class ObjectStorageClient:
    def __init__(self, client: BaseClient):
        self._client = client

    @classmethod
    def from_settings(
        cls,
        settings: PlatformSettings | None = None,
    ) -> "ObjectStorageClient":
        resolved_settings = settings or load_settings()
        object_storage = resolved_settings.object_storage

        client = boto3.client(
            "s3",
            endpoint_url=object_storage.endpoint,
            aws_access_key_id=object_storage.access_key,
            aws_secret_access_key=object_storage.secret_key,
            region_name=object_storage.region,
        )
        return cls(client)
    
    def exists(self, path: ObjectStoragePath) -> bool:
        try:
            self._client.head_object(
                Bucket=path.bucket,
                Key=path.key,
            )
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def head(self, path: ObjectStoragePath) -> RemoteObjectMetadata | None:
        """
        Return object metadata if it exists, otherwise None.
        """
        try:
            response = self._client.head_object(
                Bucket=path.bucket,
                Key=path.key,
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

        return RemoteObjectMetadata(
            bucket=path.bucket,
            key=path.key,
            etag=_clean_etag(response.get("ETag")),
            size_bytes=response.get("ContentLength"),
            last_modified_at=_normalize_last_modified(response.get("LastModified")),
            content_type=response.get("ContentType"),
        )

    def upload_file(
        self,
        source_path: Path,
        destination: ObjectStoragePath,
        *,
        content_type: str | None = None,
    ) -> None:

        resolved_source = ensure_file_exists(source_path)

        extra_args: dict[str, str] = {}
        if content_type is not None:
            extra_args["ContentType"] = content_type

        self._client.upload_file(
            Filename=str(resolved_source),
            Bucket=destination.bucket,
            Key=destination.key,
            ExtraArgs=extra_args or None,
        )


    def upload_bytes(
        self,
        payload: bytes,
        destination: ObjectStoragePath,
        *,
        content_type: str | None = None,
    ) -> None:

        stream = BytesIO(payload)
        extra_args: dict[str, str] = {}
        if content_type is not None:
            extra_args["ContentType"] = content_type

        self._client.upload_fileobj(
            Fileobj=stream,
            Bucket=destination.bucket,
            Key=destination.key,
            ExtraArgs=extra_args or None,
        )

    def download_file(
        self,
        source: ObjectStoragePath,
        destination_path: Path,
    ) -> Path:

        resolved_destination = destination_path.resolve()
        resolved_destination.parent.mkdir(parents=True, exist_ok=True)

        self._client.download_file(
            Bucket=source.bucket,
            Key=source.key,
            Filename=str(resolved_destination),
        )
        return resolved_destination
    
    def download_bytes(self, source: ObjectStoragePath) -> bytes:

        response = self._client.get_object(
            Bucket=source.bucket,
            Key=source.key,
        )
        body = response["Body"]
        return body.read()
    
    def delete(self, path: ObjectStoragePath) -> None:
        self._client.delete_object(
            Bucket=path.bucket,
            Key=path.key,
        )


def _clean_etag(value: str | None) -> str | None:
    if value is None:
        return None
    return value.replace('"', "")


def _normalize_last_modified(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    
    return value.astimezone(timezone.utc)