from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from apex_lakehouse.exceptions import ConfigurationError, ExternalServiceError
from apex_lakehouse.storage.files import LocalFileMetadata, collect_local_file_metadata
from apex_lakehouse.storage.staging import LocalStagingManager, StagingLocation



DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_SECONDS = 2.0
DEFAULT_HEADERS = {
    "User-Agent": "apex-lakehouse/1.0",
}



@dataclass(frozen=True)
class DownloadRequest:
    source_url: str
    source_system: str
    dataset_name: str
    target_file_name: str
    headers: Mapping[str, str] | None = None


@dataclass(frozen=True)
class DownloadedFile:
    request: DownloadRequest
    staging_location: StagingLocation
    file_metadata: LocalFileMetadata


class CvmRawDownloader:
    def __init__(
        self,
        staging_manager: LocalStagingManager,
        *,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    ):
        if timeout_seconds <= 0:
            raise ConfigurationError("timeout_seconds must be greater than zero")
        if max_attempts <= 0:
            raise ConfigurationError("max_attempts must be greater than zero")
        if backoff_seconds < 0:
            raise ConfigurationError("backoff_seconds must be zero or greater")

        self._staging_manager = staging_manager
        self._timeout_seconds = timeout_seconds
        self._max_attempts = max_attempts
        self._backoff_seconds = backoff_seconds

    def download(self, request: DownloadRequest) -> DownloadedFile:
        staging_directory = self._staging_manager.prepare_directory(
            source_system=request.source_system,
            dataset_name=request.dataset_name,
        )
        destination_path = staging_directory / request.target_file_name

        self._download_with_retry(
            source_url=request.source_url,
            destination_path=destination_path,
            headers=request.headers or {},
        )

        file_metadata = collect_local_file_metadata(destination_path)
        return DownloadedFile(
            request=request,
            staging_location=StagingLocation(
                directory=staging_directory,
                file_path=destination_path
            ),
            file_metadata=file_metadata,
        )
    
    def _download_with_retry(
        self,
        *,
        source_url: str,
        destination_path: Path,
        headers: Mapping[str, str],
    ) -> None:
        last_error: Exception | None = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                self._download_once(
                    source_url=source_url,
                    destination_path=destination_path,
                    headers=headers,
                )
                return
            except (HTTPError, URLError, TimeoutError, OSError) as exc:
                last_error = exc
                if destination_path.exists():
                    destination_path.unlink(missing_ok=True)

                if attempt == self._max_attempts:
                    break

                sleep(self._backoff_seconds * attempt)

        raise ExternalServiceError(
            f"Failed to download CVM artifact after {self._max_attempts} attempts: {source_url}"
        ) from last_error
    
    def _download_once(
        self,
        *,
        source_url: str,
        destination_path: Path,
        headers: Mapping[str, str],
    ) -> None:
        request = Request(
            source_url,
            headers={**DEFAULT_HEADERS, **dict(headers)},
            method="GET",
        )

        destination_path.parent.mkdir(parents=True, exist_ok=True)

        with urlopen(request, timeout=self._timeout_seconds) as response:  # nosec: B310
            with destination_path.open("wb") as file_obj:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    file_obj.write(chunk)
