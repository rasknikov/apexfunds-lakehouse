from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping
from uuid import UUID, uuid4

from apex_lakehouse.control_plane.enums import SourceFileStatus
from apex_lakehouse.control_plane.records import IngestionStateRecord, SourceFileRecord
from apex_lakehouse.ingestion.cvm.discovery_models import CvmSourceArtifact
from apex_lakehouse.ingestion.cvm.raw_downloader import (
    CvmRawDownloader,
    DownloadRequest,
    DownloadedFile,
)
from apex_lakehouse.storage.models import RawDatasetLayout
from apex_lakehouse.storage.operations import PublishedRawObject, StorageOperations
from apex_lakehouse.time import utc_now
from apex_lakehouse.types import JsonDict


@dataclass(frozen=True)
class RawIngestionRequest:
    artifact: CvmSourceArtifact
    updated_by: str
    known_source_file_id: UUID | None = None
    pipeline_run_id: UUID | None = None
    metadata: JsonDict | None = None


@dataclass(frozen=True)
class RawIngestionResult:
    artifact: CvmSourceArtifact
    downloaded_file: DownloadedFile
    published_object: PublishedRawObject
    source_file_record: SourceFileRecord
    ingestion_state_record: IngestionStateRecord
    details: JsonDict


class CvmRawIngestionService:
    """Execute download + raw publication for a single CVM artifact."""

    def __init__(
        self,
        *,
        downloader: CvmRawDownloader,
        storage_operations: StorageOperations,
    ):
        self._downloader = downloader
        self._storage_operations = storage_operations

    def ingest(self, request: RawIngestionRequest) -> RawIngestionResult:
        downloaded_file = self._download_artifact(request.artifact)
        published_object = self._publish_to_raw(request.artifact, downloaded_file)
        processed_at = utc_now()

        source_file_record = self._build_source_file_record(
            request=request,
            downloaded_file=downloaded_file,
            published_object=published_object,
            processed_at=processed_at,
        )
        ingestion_state_record = self._build_ingestion_state_record(
            request=request,
            processed_at=processed_at,
        )
        details = self._build_result_details(
            artifact=request.artifact,
            downloaded_file=downloaded_file,
            published_object=published_object,
            extra=request.metadata or {},
        )

        return RawIngestionResult(
            artifact=request.artifact,
            downloaded_file=downloaded_file,
            published_object=published_object,
            source_file_record=source_file_record,
            ingestion_state_record=ingestion_state_record,
            details=details,
        )

    def _download_artifact(self, artifact: CvmSourceArtifact) -> DownloadedFile:
        return self._downloader.download(
            DownloadRequest(
                source_url=artifact.source_url,
                source_system=artifact.source_system,
                dataset_name=artifact.dataset_name.value,
                target_file_name=artifact.file_name,
            )
        )

    def _publish_to_raw(
        self,
        artifact: CvmSourceArtifact,
        downloaded_file: DownloadedFile,
    ) -> PublishedRawObject:
        layout = RawDatasetLayout(
            source_system=artifact.source_system,
            dataset_name=artifact.dataset_name.value,
            file_name=artifact.file_name,
            competence=artifact.competence,
            business_date=artifact.business_date,
        )

        return self._storage_operations.publish_raw_file(
            downloaded_file.staging_location.file_path,
            layout=layout,
            content_type=artifact.content_type,
        )

    def _build_source_file_record(
        self,
        *,
        request: RawIngestionRequest,
        downloaded_file: DownloadedFile,
        published_object: PublishedRawObject,
        processed_at: datetime,
    ) -> SourceFileRecord:
        artifact = request.artifact
        local_metadata = downloaded_file.file_metadata
        storage_path = published_object.storage_path

        return SourceFileRecord(
            source_file_id=self._resolve_source_file_id(request.known_source_file_id),
            source_system=artifact.source_system,
            dataset_name=artifact.dataset_name.value,
            source_url=artifact.source_url,
            file_name=artifact.file_name,
            storage_bucket=storage_path.bucket,
            storage_key=storage_path.key,
            competence=str(artifact.competence) if artifact.competence is not None else None,
            business_date=artifact.business_date,
            content_type=artifact.content_type,
            file_hash=local_metadata.content_hash,
            file_size_bytes=local_metadata.size_bytes,
            source_last_modified_at=artifact.source_last_modified_at,
            first_seen_at=artifact.discovered_at,
            last_seen_at=processed_at,
            first_ingested_at=processed_at,
            latest_ingested_at=processed_at,
            status=SourceFileStatus.INGESTED,
            last_pipeline_run_id=request.pipeline_run_id,
        )

    def _build_ingestion_state_record(
        self,
        *,
        request: RawIngestionRequest,
        processed_at: datetime,
    ) -> IngestionStateRecord:
        artifact = request.artifact

        return IngestionStateRecord(
            source_system=artifact.source_system,
            dataset_name=artifact.dataset_name.value,
            watermark_business_date=artifact.business_date,
            watermark_competence=str(artifact.competence) if artifact.competence is not None else None,
            last_successful_run_id=request.pipeline_run_id,
            last_attempted_run_id=request.pipeline_run_id,
            updated_at=processed_at,
            updated_by=request.updated_by,
        )

    def _build_result_details(
        self,
        *,
        artifact: CvmSourceArtifact,
        downloaded_file: DownloadedFile,
        published_object: PublishedRawObject,
        extra: Mapping[str, object],
    ) -> JsonDict:
        remote_metadata = published_object.remote_metadata

        details: JsonDict = {
            "source_url": artifact.source_url,
            "source_file_name": artifact.file_name,
            "staging_file_path": str(downloaded_file.staging_location.file_path),
            "storage_bucket": published_object.storage_path.bucket,
            "storage_key": published_object.storage_path.key,
            "storage_uri": published_object.storage_path.uri,
            "content_hash": downloaded_file.file_metadata.content_hash,
            "hash_algorithm": downloaded_file.file_metadata.hash_algorithm,
            "size_bytes": downloaded_file.file_metadata.size_bytes,
        }

        if artifact.competence is not None:
            details["competence"] = str(artifact.competence)

        if artifact.business_date is not None:
            details["business_date"] = artifact.business_date.isoformat()

        if artifact.content_type is not None:
            details["content_type"] = artifact.content_type

        if remote_metadata is not None:
            details["remote_etag"] = remote_metadata.etag
            details["remote_last_modified_at"] = (
                remote_metadata.last_modified_at.isoformat()
                if remote_metadata.last_modified_at is not None
                else None
            )

        details.update(extra)
        return details

    def _resolve_source_file_id(self, known_source_file_id: UUID | None) -> UUID:
        return known_source_file_id or uuid4()
