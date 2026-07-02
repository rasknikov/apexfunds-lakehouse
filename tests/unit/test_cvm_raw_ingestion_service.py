from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

from apex_lakehouse.control_plane.enums import SourceFileStatus
from apex_lakehouse.ingestion.cvm.discovery_models import CvmDataset, CvmSourceArtifact
from apex_lakehouse.ingestion.cvm.raw_downloader import DownloadedFile
from apex_lakehouse.ingestion.cvm.raw_ingestion_service import (
    CvmRawIngestionService,
    RawIngestionRequest,
)
from apex_lakehouse.storage.client import RemoteObjectMetadata
from apex_lakehouse.storage.files import LocalFileMetadata
from apex_lakehouse.storage.models import ObjectStoragePath
from apex_lakehouse.storage.operations import PublishedRawObject
from apex_lakehouse.storage.staging import StagingLocation
from apex_lakehouse.time import Competence


def test_ingest_builds_records_and_details(tmp_path: Path, monkeypatch) -> None:
    artifact = CvmSourceArtifact(
        dataset_name=CvmDataset.INFORME_DIARIO,
        source_url="https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202401.zip",
        file_name="inf_diario_fi_202401.zip",
        discovered_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        competence=Competence(2024, 1),
        business_date=date(2024, 1, 15),
        content_type="application/zip",
    )
    pipeline_run_id = uuid4()
    known_source_file_id = uuid4()
    processed_at = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    file_path = (tmp_path / "staging" / "inf_diario_fi_202401.zip").resolve()
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"payload")

    downloaded_file = DownloadedFile(
        request=type("Request", (), {"source_url": artifact.source_url})(),  # type: ignore[arg-type]
        staging_location=StagingLocation(
            directory=file_path.parent,
            file_path=file_path,
        ),
        file_metadata=LocalFileMetadata(
            path=file_path,
            size_bytes=7,
            content_hash="abc123",
            hash_algorithm="sha256",
            detected_at=processed_at,
        ),
    )
    published_object = PublishedRawObject(
        local_metadata=downloaded_file.file_metadata,
        storage_path=ObjectStoragePath(
            bucket="raw",
            key="raw/cvm/informe_diario/ano=2024/mes=01/inf_diario_fi_202401.zip",
        ),
        remote_metadata=RemoteObjectMetadata(
            bucket="raw",
            key="raw/cvm/informe_diario/ano=2024/mes=01/inf_diario_fi_202401.zip",
            etag="etag123",
            size_bytes=7,
            last_modified_at=processed_at,
            content_type="application/zip",
        ),
    )

    downloader = type("DownloaderStub", (), {"download": lambda self, request: downloaded_file})()
    storage_operations = type(
        "StorageOperationsStub",
        (),
        {"publish_raw_file": lambda self, source_path, layout, content_type=None: published_object},
    )()
    service = CvmRawIngestionService(
        downloader=downloader,  # type: ignore[arg-type]
        storage_operations=storage_operations,  # type: ignore[arg-type]
    )
    monkeypatch.setattr("apex_lakehouse.ingestion.cvm.raw_ingestion_service.utc_now", lambda: processed_at)

    result = service.ingest(
        RawIngestionRequest(
            artifact=artifact,
            updated_by="test-suite",
            known_source_file_id=known_source_file_id,
            pipeline_run_id=pipeline_run_id,
            metadata={"batch": "2024-01"},
        )
    )

    assert result.source_file_record.source_file_id == known_source_file_id
    assert result.source_file_record.status is SourceFileStatus.INGESTED
    assert result.source_file_record.last_pipeline_run_id == pipeline_run_id
    assert result.source_file_record.storage_bucket == "raw"
    assert result.source_file_record.file_hash == "abc123"
    assert result.ingestion_state_record.last_successful_run_id == pipeline_run_id
    assert result.ingestion_state_record.last_attempted_run_id == pipeline_run_id
    assert result.ingestion_state_record.watermark_competence == "2024-01"
    assert result.details["storage_uri"] == published_object.storage_path.uri
    assert result.details["remote_etag"] == "etag123"
    assert result.details["batch"] == "2024-01"


def test_ingest_generates_source_file_id_when_artifact_is_new(tmp_path: Path, monkeypatch) -> None:
    artifact = CvmSourceArtifact(
        dataset_name=CvmDataset.CADASTRO_FUNDOS,
        source_url="https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv",
        file_name="cad_fi.csv",
        discovered_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
    )
    processed_at = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    file_path = (tmp_path / "staging" / "cad_fi.csv").resolve()
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text("payload", encoding="utf-8")

    downloaded_file = DownloadedFile(
        request=type("Request", (), {"source_url": artifact.source_url})(),  # type: ignore[arg-type]
        staging_location=StagingLocation(directory=file_path.parent, file_path=file_path),
        file_metadata=LocalFileMetadata(
            path=file_path,
            size_bytes=7,
            content_hash="abc123",
            hash_algorithm="sha256",
            detected_at=processed_at,
        ),
    )
    published_object = PublishedRawObject(
        local_metadata=downloaded_file.file_metadata,
        storage_path=ObjectStoragePath(bucket="raw", key="raw/cvm/cadastro_fundos/cad_fi.csv"),
        remote_metadata=None,
    )

    downloader = type("DownloaderStub", (), {"download": lambda self, request: downloaded_file})()
    storage_operations = type(
        "StorageOperationsStub",
        (),
        {"publish_raw_file": lambda self, source_path, layout, content_type=None: published_object},
    )()
    service = CvmRawIngestionService(
        downloader=downloader,  # type: ignore[arg-type]
        storage_operations=storage_operations,  # type: ignore[arg-type]
    )
    monkeypatch.setattr("apex_lakehouse.ingestion.cvm.raw_ingestion_service.utc_now", lambda: processed_at)

    result = service.ingest(
        RawIngestionRequest(
            artifact=artifact,
            updated_by="test-suite",
        )
    )

    assert result.source_file_record.source_file_id is not None
    assert result.ingestion_state_record.last_successful_run_id is None
    assert result.details["storage_key"] == "raw/cvm/cadastro_fundos/cad_fi.csv"
