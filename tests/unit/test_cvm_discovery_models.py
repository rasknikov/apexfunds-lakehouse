from __future__ import annotations

from datetime import date, datetime

from apex_lakehouse.ingestion.cvm.discovery_models import (
    CvmDataset,
    CvmDiscoveryResult,
    CvmSourceArtifact,
    DiscoveryDecision,
    build_discovery_details,
)
from apex_lakehouse.time import Competence


def test_cvm_source_artifact_source_path_extracts_url_path() -> None:
    artifact = CvmSourceArtifact(
        dataset_name=CvmDataset.INFORME_DIARIO,
        source_url="https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202401.zip?download=1",
        file_name="inf_diario_fi_202401.zip",
        discovered_at=datetime(2026, 7, 2, 10, 0, 0),
    )

    assert artifact.source_path == "/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202401.zip"


def test_cvm_source_artifact_period_label_prefers_competence() -> None:
    artifact = CvmSourceArtifact(
        dataset_name=CvmDataset.PERFIL_MENSAL,
        source_url="https://dados.cvm.gov.br/perfil_mensal_fi_202401.csv",
        file_name="perfil_mensal_fi_202401.csv",
        discovered_at=datetime(2026, 7, 2, 10, 0, 0),
        competence=Competence(2024, 1),
        business_date=date(2024, 1, 31),
    )

    assert artifact.period_label == "2024-01"


def test_cvm_source_artifact_period_label_falls_back_to_business_date() -> None:
    artifact = CvmSourceArtifact(
        dataset_name=CvmDataset.CADASTRO_FUNDOS,
        source_url="https://dados.cvm.gov.br/cad_fi_2024-01-31.csv",
        file_name="cad_fi_2024-01-31.csv",
        discovered_at=datetime(2026, 7, 2, 10, 0, 0),
        business_date=date(2024, 1, 31),
    )

    assert artifact.period_label == "2024-01-31"


def test_build_discovery_details_returns_new_payload() -> None:
    base = {"dataset_name": "informe_diario"}

    payload = build_discovery_details(base, decision="new")

    assert payload == {"dataset_name": "informe_diario", "decision": "new"}
    assert payload is not base
    assert base == {"dataset_name": "informe_diario"}


def test_cvm_discovery_result_defaults_discovery_run_id() -> None:
    artifact = CvmSourceArtifact(
        dataset_name=CvmDataset.INFORME_DIARIO,
        source_url="https://dados.cvm.gov.br/inf_diario_fi_202401.zip",
        file_name="inf_diario_fi_202401.zip",
        discovered_at=datetime(2026, 7, 2, 10, 0, 0),
    )

    result = CvmDiscoveryResult(
        artifact=artifact,
        decision=DiscoveryDecision.NEW,
        reason="new file",
    )

    assert result.discovery_run_id is not None
