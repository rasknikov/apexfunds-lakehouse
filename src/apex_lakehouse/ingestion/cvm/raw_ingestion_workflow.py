"""Workflow that turns discovery decisions into persisted raw ingestions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence
from uuid import UUID

from apex_lakehouse.control_plane.repository import ControlPlaneRepository
from apex_lakehouse.ingestion.cvm.discovery_models import CvmDiscoveryResult, DiscoveryDecision
from apex_lakehouse.ingestion.cvm.raw_downloader import CvmRawDownloader
from apex_lakehouse.ingestion.cvm.raw_ingestion_service import (
    CvmRawIngestionService,
    RawIngestionRequest,
    RawIngestionResult,
)
from apex_lakehouse.storage.operations import StorageOperations
from apex_lakehouse.storage.staging import LocalStagingManager
from apex_lakehouse.types import JsonDict


@dataclass(frozen=True)
class CvmRawWorkflowResult:
    discovery_result: CvmDiscoveryResult
    persisted: bool
    reason: str
    ingestion_result: RawIngestionResult | None = None


class CvmRawIngestionWorkflow:
    """Coordinate raw ingestion and control-plane persistence."""

    def __init__(
        self,
        *,
        repository: ControlPlaneRepository,
        service: CvmRawIngestionService,
    ):
        self._repository = repository
        self._service = service

    @classmethod
    def from_repository(
        cls,
        repository: ControlPlaneRepository,
    ) -> "CvmRawIngestionWorkflow":
        staging_manager = LocalStagingManager.from_project_paths()
        downloader = CvmRawDownloader(staging_manager)
        storage_operations = StorageOperations.from_settings()

        return cls(
            repository=repository,
            service=CvmRawIngestionService(
                downloader=downloader,
                storage_operations=storage_operations,
            ),
        )

    def ingest_discovery_result(
        self,
        discovery_result: CvmDiscoveryResult,
        *,
        updated_by: str,
        pipeline_run_id: UUID | None = None,
    ) -> CvmRawWorkflowResult:
        if not self._should_ingest(discovery_result):
            return CvmRawWorkflowResult(
                discovery_result=discovery_result,
                persisted=False,
                reason=f"Skipped discovery result with decision={discovery_result.decision.value}",
            )

        request = RawIngestionRequest(
            artifact=discovery_result.artifact,
            updated_by=updated_by,
            known_source_file_id=discovery_result.known_source_file_id,
            pipeline_run_id=pipeline_run_id,
            metadata=self._build_request_metadata(discovery_result),
        )
        ingestion_result = self._service.ingest(request)

        self._repository.upsert_source_file(ingestion_result.source_file_record)
        self._repository.upsert_ingestion_state(ingestion_result.ingestion_state_record)

        return CvmRawWorkflowResult(
            discovery_result=discovery_result,
            persisted=True,
            reason="Artifact ingested and control-plane state updated",
            ingestion_result=ingestion_result,
        )

    def ingest_many(
        self,
        discovery_results: Sequence[CvmDiscoveryResult],
        *,
        updated_by: str,
        pipeline_run_id: UUID | None = None,
    ) -> list[CvmRawWorkflowResult]:
        return [
            self.ingest_discovery_result(
                discovery_result,
                updated_by=updated_by,
                pipeline_run_id=pipeline_run_id,
            )
            for discovery_result in discovery_results
        ]

    def _should_ingest(self, discovery_result: CvmDiscoveryResult) -> bool:
        return discovery_result.decision in {
            DiscoveryDecision.NEW,
            DiscoveryDecision.CHANGED,
        }

    def _build_request_metadata(
        self,
        discovery_result: CvmDiscoveryResult,
    ) -> JsonDict:
        metadata: JsonDict = {
            "discovery_decision": discovery_result.decision.value,
            "discovery_reason": discovery_result.reason,
            "discovery_run_id": str(discovery_result.discovery_run_id),
        }
        metadata.update(discovery_result.details)
        return metadata
