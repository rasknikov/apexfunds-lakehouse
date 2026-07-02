from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError

import pytest

from apex_lakehouse.exceptions import ConfigurationError, ExternalServiceError
from apex_lakehouse.ingestion.cvm.raw_downloader import CvmRawDownloader, DownloadRequest
from apex_lakehouse.storage.files import LocalFileMetadata
from apex_lakehouse.storage.staging import LocalStagingManager


class _ResponseStub:
    def __init__(self, payload: bytes):
        self._payload = payload
        self._consumed = False

    def __enter__(self) -> "_ResponseStub":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        if self._consumed:
            return b""
        self._consumed = True
        return self._payload


def test_download_stages_file_and_collects_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    staging_manager = LocalStagingManager(tmp_path / "staging")
    downloader = CvmRawDownloader(staging_manager, backoff_seconds=0)
    expected_metadata = LocalFileMetadata(
        path=(tmp_path / "staging" / "cvm" / "informe_diario" / "arquivo.zip").resolve(),
        size_bytes=12,
        content_hash="abc123",
        hash_algorithm="sha256",
        detected_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
    )

    monkeypatch.setattr(
        "apex_lakehouse.ingestion.cvm.raw_downloader.urlopen",
        lambda request, timeout: _ResponseStub(b"zip payload"),
    )
    monkeypatch.setattr(
        "apex_lakehouse.ingestion.cvm.raw_downloader.collect_local_file_metadata",
        lambda path: expected_metadata,
    )

    result = downloader.download(
        DownloadRequest(
            source_url="https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/arquivo.zip",
            source_system="cvm",
            dataset_name="informe_diario",
            target_file_name="arquivo.zip",
        )
    )

    assert result.file_metadata == expected_metadata
    assert result.staging_location.file_path.exists()
    assert result.staging_location.file_path.read_bytes() == b"zip payload"


def test_download_retries_and_raises_external_service_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    staging_manager = LocalStagingManager(tmp_path / "staging")
    downloader = CvmRawDownloader(
        staging_manager,
        max_attempts=2,
        backoff_seconds=0,
    )

    def _raise_url_error(request, timeout):
        raise URLError("temporary unavailable")

    monkeypatch.setattr(
        "apex_lakehouse.ingestion.cvm.raw_downloader.urlopen",
        _raise_url_error,
    )

    with pytest.raises(ExternalServiceError):
        downloader.download(
            DownloadRequest(
                source_url="https://dados.cvm.gov.br/file.zip",
                source_system="cvm",
                dataset_name="informe_diario",
                target_file_name="file.zip",
            )
        )


def test_downloader_rejects_invalid_configuration(tmp_path: Path) -> None:
    staging_manager = LocalStagingManager(tmp_path / "staging")

    with pytest.raises(ConfigurationError):
        CvmRawDownloader(staging_manager, timeout_seconds=0)

    with pytest.raises(ConfigurationError):
        CvmRawDownloader(staging_manager, max_attempts=0)

    with pytest.raises(ConfigurationError):
        CvmRawDownloader(staging_manager, backoff_seconds=-1)
