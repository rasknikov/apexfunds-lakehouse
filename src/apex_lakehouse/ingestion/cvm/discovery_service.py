"""Service-layer logic for CVM source discovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Sequence
from uuid import UUID

from apex_lakehouse.ingestion.cvm.discovery_models import (
    CvmDiscoveryCandidate,
    CvmDiscoveryResult,
    CvmSourceArtifact,
    DiscoveryDecision,
    build_discovery_details,
)
from apex_lakehouse.ingestion.cvm.discovery_parser import classify_cvm_artifact


@dataclass(frozen=True)
class KnownArtifactState:
    """
    Minimal control-plane view of one known source artifact.

    This is intentionally small so discovery does not depend yet on a concrete
    repository implementation.
    """

    source_url: str
    file_name: str
    status: str | None = None
    source_file_id: UUID | None = None
    file_hash: str | None = None
    source_last_modified_at: datetime | None = None


class CvmDiscoveryService:
    """
    Turn raw source URLs plus known registry state into discovery decisions.
    """

    def build_artifact(
        self,
        source_url: str,
        *,
        discovered_at: datetime,
        source_last_modified_at: datetime | None = None,
        content_type: str | None = None,
    ) -> CvmSourceArtifact:
        """
        Parse one CVM source URL into a structured discovery artifact.
        """
        dataset_name, file_name, competence, business_date = classify_cvm_artifact(source_url)

        return CvmSourceArtifact(
            dataset_name=dataset_name,
            source_url=source_url,
            file_name=file_name,
            discovered_at=discovered_at,
            competence=competence,
            business_date=business_date,
            source_last_modified_at=source_last_modified_at,
            content_type=content_type,
        )

    def build_candidates(
        self,
        source_urls: Iterable[str],
        *,
        discovered_at: datetime,
        known_artifacts: Sequence[KnownArtifactState] | None = None,
    ) -> list[CvmDiscoveryCandidate]:
        """
        Build discovery candidates by combining parsed artifacts with known state.
        """
        known_by_url = {
            item.source_url: item
            for item in (known_artifacts or [])
        }

        candidates: list[CvmDiscoveryCandidate] = []

        for source_url in source_urls:
            artifact = self.build_artifact(
                source_url,
                discovered_at=discovered_at,
            )
            known = known_by_url.get(source_url)

            candidates.append(
                CvmDiscoveryCandidate(
                    artifact=artifact,
                    known_source_file_id=known.source_file_id if known else None,
                    known_file_hash=known.file_hash if known else None,
                    known_status=known.status if known else None,
                )
            )

        return candidates

    def decide_candidate(self, candidate: CvmDiscoveryCandidate) -> CvmDiscoveryResult:
        """
        Decide whether one discovered artifact should be treated as new, changed
        or already known.

        Current first-cut rule:
        - if we never saw the URL, it is new;
        - if source status was failed, we retry and classify it as changed;
        - if the source was already known, we keep it as already_known.

        This is intentionally conservative until we connect richer metadata from
        CVM responses and the control plane.
        """
        artifact = candidate.artifact

        if candidate.known_source_file_id is None:
            return CvmDiscoveryResult(
                artifact=artifact,
                decision=DiscoveryDecision.NEW,
                reason="Artifact URL was not found in control-plane state.",
                details=build_discovery_details(
                    dataset_name=artifact.dataset_name.value,
                    file_name=artifact.file_name,
                    period_label=artifact.period_label,
                ),
            )

        if candidate.known_status == "failed":
            return CvmDiscoveryResult(
                artifact=artifact,
                decision=DiscoveryDecision.CHANGED,
                reason="Known artifact previously failed and should be retried.",
                known_source_file_id=candidate.known_source_file_id,
                details=build_discovery_details(
                    dataset_name=artifact.dataset_name.value,
                    file_name=artifact.file_name,
                    previous_status=candidate.known_status,
                ),
            )

        return CvmDiscoveryResult(
            artifact=artifact,
            decision=DiscoveryDecision.ALREADY_KNOWN,
            reason="Artifact is already present in control-plane state.",
            known_source_file_id=candidate.known_source_file_id,
            details=build_discovery_details(
                dataset_name=artifact.dataset_name.value,
                file_name=artifact.file_name,
                previous_status=candidate.known_status,
            ),
        )

    def decide_candidates(
        self,
        candidates: Sequence[CvmDiscoveryCandidate],
    ) -> list[CvmDiscoveryResult]:
        """
        Apply discovery decision rules to a batch of candidates.
        """
        return [self.decide_candidate(candidate) for candidate in candidates]
