"""Parsing helpers for CVM discovery artifacts."""

from __future__ import annotations

from datetime import date

import re
from pathlib import PurePosixPath
from urllib.parse import urlparse

from apex_lakehouse.exceptions import ValidationError
from apex_lakehouse.ingestion.cvm.discovery_models import CvmDataset
from apex_lakehouse.time import Competence, parse_iso_date


INFORME_DIARIO_PATTERN = re.compile(
    r"inf_diario_fi_(?P<year>\d{4})(?P<month>\d{2})",
    re.IGNORECASE,
)

CADASTRO_FUNDOS_PATTERN = re.compile(
    r"cad_fi",
    re.IGNORECASE,
)

PERFIL_MENSAL_PATTERN = re.compile(
    r"perfil_mensal_fi_(?P<year>\d{4})(?P<month>\d{2})",
    re.IGNORECASE,
)

ISO_DATE_PATTERN = re.compile(
    r"(?P<date>\d{4}-\d{2}-\d{2})"
)


def extract_file_name_from_url(source_url: str) -> str:
    """
    Extract the file name portion from a source URL.
    """
    parsed = urlparse(source_url)
    file_name = PurePosixPath(parsed.path).name

    if not file_name:
        raise ValidationError(f"Could not extract file name from URL: {source_url}")

    return file_name


def detect_cvm_dataset(file_name: str) -> CvmDataset:
    """
    Infer the logical CVM dataset from a published file name.
    """
    normalized = file_name.lower()

    if INFORME_DIARIO_PATTERN.search(normalized):
        return CvmDataset.INFORME_DIARIO

    if PERFIL_MENSAL_PATTERN.search(normalized):
        return CvmDataset.PERFIL_MENSAL

    if CADASTRO_FUNDOS_PATTERN.search(normalized):
        return CvmDataset.CADASTRO_FUNDOS

    raise ValidationError(f"Unsupported CVM dataset for file: {file_name}")


def extract_competence(file_name: str, dataset_name: CvmDataset) -> Competence | None:
    """
    Extract the business competence from file names that encode year and month.
    """
    normalized = file_name.lower()

    if dataset_name is CvmDataset.INFORME_DIARIO:
        match = INFORME_DIARIO_PATTERN.search(normalized)
        if match:
            return Competence(
                year=int(match.group("year")),
                month=int(match.group("month")),
            )

    if dataset_name is CvmDataset.PERFIL_MENSAL:
        match = PERFIL_MENSAL_PATTERN.search(normalized)
        if match:
            return Competence(
                year=int(match.group("year")),
                month=int(match.group("month")),
            )

    return None


def extract_business_date(source_url: str, file_name: str) -> date | None:
    """
    Extract a specific business date when the source encodes one.

    For the first discovery cut we only support explicit ISO dates present in
    the URL or file name. If not present, discovery can still work with
    competence-only artifacts.
    """
    for candidate in (source_url, file_name):
        match = ISO_DATE_PATTERN.search(candidate)
        if match:
            return parse_iso_date(match.group("date"))

    return None


def classify_cvm_artifact(source_url: str) -> tuple[CvmDataset, str, Competence | None, date | None]:
    """
    Parse the minimum discovery identity from a CVM source URL.

    Returns:
    - dataset name
    - file name
    - competence, when encoded in the artifact name
    - business date, when explicitly available
    """
    file_name = extract_file_name_from_url(source_url)
    dataset_name = detect_cvm_dataset(file_name)
    competence = extract_competence(file_name, dataset_name)
    business_date = extract_business_date(source_url, file_name)

    return dataset_name, file_name, competence, business_date
