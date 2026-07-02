"""High-level CVM discovery workflow orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from apex_lakehouse.control_plane.repository import ControlPlaneRepository
from apex_lakehouse.ingestion.cvm.discovery_models import (
    CvmDataset,
    CvmDiscoveryResult,
)
from apex_lakehouse.ingestion.cvm.discovery_service import CvmDiscoveryService
from apex_lakehouse.ingestion.cvm.discovery_source import CvmDiscoverySource, CvmSourceListing
from apex_lakehouse.ingestion.cvm.discovery_state import CvmDiscoveryStateRepository


@dataclass(frozen=True)
class CvmDiscoveryBatch:
    dataset_name: CvmDataset
    listings: list[CvmSourceListing]
    results: list[CvmDiscoveryResult]
    discovered_at: datetime


class CvmDiscoveryWorkflow:
    """Coordinate source listing, known-state lookup and discovery decisions."""

    def __init__(
        self,
        *,
        source: CvmDiscoverySource,
        state_repository: CvmDiscoveryStateRepository,
        service: CvmDiscoveryService,
    ):
        self._source = source
        self._state_repository = state_repository
        self._service = service

    @classmethod
    def from_repository(
        cls,
        repository: ControlPlaneRepository,
    ) -> "CvmDiscoveryWorkflow":
        return cls(
            source=CvmDiscoverySource(),
            state_repository=CvmDiscoveryStateRepository(repository),
            service=CvmDiscoveryService(),
        )

    def discover_dataset(
        self,
        dataset_name: CvmDataset,
        *,
        discovered_at: datetime | None = None,
    ) -> CvmDiscoveryBatch:
        resolved_discovered_at = discovered_at or datetime.now(timezone.utc)
        listings = self._source.list_dataset(dataset_name, listed_at=resolved_discovered_at)
        source_urls = [listing.source_url for listing in listings]
        known_artifacts = self._state_repository.get_known_artifacts(source_urls)
        candidates = self._service.build_candidates(
            source_urls,
            discovered_at=resolved_discovered_at,
            known_artifacts=known_artifacts,
        )
        results = self._service.decide_candidates(candidates)

        return CvmDiscoveryBatch(
            dataset_name=dataset_name,
            listings=listings,
            results=results,
            discovered_at=resolved_discovered_at,
        )

    def discover_many(
        self,
        dataset_names: Sequence[CvmDataset],
        *,
        discovered_at: datetime | None = None,
    ) -> list[CvmDiscoveryBatch]:
        resolved_discovered_at = discovered_at or datetime.now(timezone.utc)
        return [
            self.discover_dataset(dataset_name, discovered_at=resolved_discovered_at)
            for dataset_name in dataset_names
        ]
