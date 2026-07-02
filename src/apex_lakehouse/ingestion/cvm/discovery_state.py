"""Adapters between CVM discovery and control-plane known artifact state."""

from __future__ import annotations

from typing import Sequence

from apex_lakehouse.control_plane.repository import ControlPlaneRepository
from apex_lakehouse.ingestion.cvm.discovery_service import KnownArtifactState


class CvmDiscoveryStateRepository:
    """Read known CVM artifact state from the control plane."""

    def __init__(self, repository: ControlPlaneRepository):
        self._repository = repository

    def get_known_artifacts(self, source_urls: Sequence[str]) -> list[KnownArtifactState]:
        rows = self._repository.get_source_files_by_urls(
            source_system="cvm",
            source_urls=source_urls,
        )

        return [
            KnownArtifactState(
                source_url=str(row["source_url"]),
                file_name=str(row["file_name"]),
                status=str(row["status"]) if row["status"] is not None else None,
                source_file_id=row["source_file_id"],
                file_hash=str(row["file_hash"]) if row["file_hash"] is not None else None,
                source_last_modified_at=row["source_last_modified_at"],
            )
            for row in rows
        ]
