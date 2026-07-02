from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from apex_lakehouse.ingestion.cvm.discovery_models import CvmDataset, DiscoveryDecision
from apex_lakehouse.ingestion.cvm.discovery_service import CvmDiscoveryService, KnownArtifactState


def test_build_artifact_parses_source_url_into_artifact() -> None:
    service = CvmDiscoveryService()
    discovered_at = datetime(2026, 7, 2, 10, 0, 0)

    artifact = service.build_artifact(
        "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202401.zip",
        discovered_at=discovered_at,
    )

    assert artifact.dataset_name is CvmDataset.INFORME_DIARIO
    assert artifact.file_name == "inf_diario_fi_202401.zip"
    assert artifact.discovered_at == discovered_at
    assert str(artifact.competence) == "2024-01"


def test_build_candidates_attaches_known_state_by_source_url() -> None:
    service = CvmDiscoveryService()
    known_id = uuid4()
    candidates = service.build_candidates(
        ["https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"],
        discovered_at=datetime(2026, 7, 2, 10, 0, 0),
        known_artifacts=[
            KnownArtifactState(
                source_url="https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv",
                file_name="cad_fi.csv",
                status="failed",
                source_file_id=known_id,
            )
        ],
    )

    assert len(candidates) == 1
    assert candidates[0].known_source_file_id == known_id
    assert candidates[0].known_status == "failed"


def test_decide_candidate_marks_unknown_artifact_as_new() -> None:
    service = CvmDiscoveryService()
    candidate = service.build_candidates(
        ["https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"],
        discovered_at=datetime(2026, 7, 2, 10, 0, 0),
    )[0]

    result = service.decide_candidate(candidate)

    assert result.decision is DiscoveryDecision.NEW


def test_decide_candidate_marks_failed_artifact_as_changed() -> None:
    service = CvmDiscoveryService()
    known_id = uuid4()
    candidate = service.build_candidates(
        ["https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"],
        discovered_at=datetime(2026, 7, 2, 10, 0, 0),
        known_artifacts=[
            KnownArtifactState(
                source_url="https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv",
                file_name="cad_fi.csv",
                status="failed",
                source_file_id=known_id,
            )
        ],
    )[0]

    result = service.decide_candidate(candidate)

    assert result.decision is DiscoveryDecision.CHANGED
    assert result.known_source_file_id == known_id


def test_decide_candidates_keeps_known_successful_artifact_as_already_known() -> None:
    service = CvmDiscoveryService()
    candidate = service.build_candidates(
        ["https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"],
        discovered_at=datetime(2026, 7, 2, 10, 0, 0),
        known_artifacts=[
            KnownArtifactState(
                source_url="https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv",
                file_name="cad_fi.csv",
                status="ingested",
                source_file_id=uuid4(),
            )
        ],
    )[0]

    result = service.decide_candidates([candidate])[0]

    assert result.decision is DiscoveryDecision.ALREADY_KNOWN
