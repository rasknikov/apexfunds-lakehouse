from __future__ import annotations

from datetime import date

import pytest

from apex_lakehouse.exceptions import ValidationError
from apex_lakehouse.ingestion.cvm.discovery_models import CvmDataset
from apex_lakehouse.ingestion.cvm.discovery_parser import (
    classify_cvm_artifact,
    detect_cvm_dataset,
    extract_business_date,
    extract_competence,
    extract_file_name_from_url,
)
from apex_lakehouse.time import Competence


def test_extract_file_name_from_url_returns_terminal_name() -> None:
    assert (
        extract_file_name_from_url(
            "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202401.zip"
        )
        == "inf_diario_fi_202401.zip"
    )


def test_detect_cvm_dataset_recognizes_supported_files() -> None:
    assert detect_cvm_dataset("inf_diario_fi_202401.zip") is CvmDataset.INFORME_DIARIO
    assert detect_cvm_dataset("perfil_mensal_fi_202401.csv") is CvmDataset.PERFIL_MENSAL
    assert detect_cvm_dataset("cad_fi.csv") is CvmDataset.CADASTRO_FUNDOS


def test_detect_cvm_dataset_raises_for_unknown_name() -> None:
    with pytest.raises(ValidationError, match="Unsupported CVM dataset"):
        detect_cvm_dataset("unknown_dataset.csv")


def test_extract_competence_returns_month_for_supported_datasets() -> None:
    assert extract_competence("inf_diario_fi_202401.zip", CvmDataset.INFORME_DIARIO) == Competence(2024, 1)
    assert extract_competence("perfil_mensal_fi_202402.csv", CvmDataset.PERFIL_MENSAL) == Competence(2024, 2)


def test_extract_business_date_reads_iso_date_from_source_or_file_name() -> None:
    assert (
        extract_business_date(
            "https://dados.cvm.gov.br/archive/2024-01-31/cad_fi.csv",
            "cad_fi.csv",
        )
        == date(2024, 1, 31)
    )


def test_classify_cvm_artifact_returns_dataset_file_name_and_periods() -> None:
    dataset_name, file_name, competence, business_date = classify_cvm_artifact(
        "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202401.zip"
    )

    assert dataset_name is CvmDataset.INFORME_DIARIO
    assert file_name == "inf_diario_fi_202401.zip"
    assert competence == Competence(2024, 1)
    assert business_date is None
