"""Dataset-specific quality rules for silver CVM outputs."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Iterable

from apex_lakehouse.control_plane.enums import QualitySeverity
from apex_lakehouse.exceptions import DatasetNotFoundError
from apex_lakehouse.quality.models import QualityRuleDefinition, QualityRuleOutcome

MAX_SAMPLE_FAILURES = 5


def get_silver_quality_rules(dataset_name: str) -> list[QualityRuleDefinition]:
    if dataset_name == "fundos":
        return [
            _required_field_rule(
                rule_code="cnpj_not_null",
                rule_name="CNPJ must be present",
                field_name="cnpj_fundo",
                severity=QualitySeverity.CRITICAL,
                blocking=True,
            ),
            _required_field_rule(
                rule_code="fund_name_not_null",
                rule_name="Fund name must be present",
                field_name="nome_fundo",
                severity=QualitySeverity.ERROR,
                blocking=True,
            ),
            _unique_fields_rule(
                rule_code="cnpj_unique",
                rule_name="Fund registry must be unique by CNPJ",
                field_names=("cnpj_fundo",),
                severity=QualitySeverity.CRITICAL,
                blocking=True,
            ),
        ]

    if dataset_name == "fundos_informe_diario":
        return [
            _required_field_rule(
                rule_code="cnpj_not_null",
                rule_name="CNPJ must be present",
                field_name="cnpj_fundo",
                severity=QualitySeverity.CRITICAL,
                blocking=True,
            ),
            _required_field_rule(
                rule_code="competence_date_not_null",
                rule_name="Competence date must be present",
                field_name="data_competencia",
                severity=QualitySeverity.CRITICAL,
                blocking=True,
            ),
            _decimal_greater_than_rule(
                rule_code="quota_value_positive",
                rule_name="Quota value must be greater than zero",
                field_name="valor_cota",
                minimum="0",
                severity=QualitySeverity.CRITICAL,
                blocking=True,
            ),
            _decimal_greater_or_equal_rule(
                rule_code="patrimonio_non_negative",
                rule_name="Patrimonio liquido must be non-negative",
                field_name="patrimonio_liquido",
                minimum="0",
                severity=QualitySeverity.ERROR,
                blocking=True,
            ),
            _required_field_rule(
                rule_code="fund_registry_match",
                rule_name="Fund must have registry enrichment",
                field_name="nome_fundo",
                severity=QualitySeverity.WARN,
                blocking=False,
            ),
            _unique_fields_rule(
                rule_code="fund_date_unique",
                rule_name="Daily fund rows must be unique by CNPJ and date",
                field_names=("cnpj_fundo", "data_competencia"),
                severity=QualitySeverity.ERROR,
                blocking=True,
            ),
        ]

    if dataset_name == "fundos_perfil_mensal":
        return [
            _required_field_rule(
                rule_code="cnpj_not_null",
                rule_name="CNPJ must be present",
                field_name="cnpj_fundo",
                severity=QualitySeverity.CRITICAL,
                blocking=True,
            ),
            _required_field_rule(
                rule_code="competence_not_null",
                rule_name="Competence must be present",
                field_name="competencia",
                severity=QualitySeverity.CRITICAL,
                blocking=True,
            ),
            _unique_fields_rule(
                rule_code="fund_competence_unique",
                rule_name="Monthly fund rows must be unique by CNPJ and competence",
                field_names=("cnpj_fundo", "competencia"),
                severity=QualitySeverity.ERROR,
                blocking=True,
            ),
        ]

    raise DatasetNotFoundError(f"No silver quality rules defined for dataset: {dataset_name}")


def _required_field_rule(
    *,
    rule_code: str,
    rule_name: str,
    field_name: str,
    severity: QualitySeverity,
    blocking: bool,
) -> QualityRuleDefinition:
    def evaluator(rows: list[dict[str, str]]) -> QualityRuleOutcome:
        failures = [
            _sample_payload(row, field_name)
            for row in rows
            if row.get(field_name, "").strip() == ""
        ]
        return QualityRuleOutcome(
            row_count_evaluated=len(rows),
            row_count_failed=len(failures),
            failed_payloads=failures,
            sample_failures=failures[:MAX_SAMPLE_FAILURES],
            details={"field_name": field_name},
        )

    return QualityRuleDefinition(
        rule_code=rule_code,
        rule_name=rule_name,
        severity=severity,
        blocking=blocking,
        evaluator=evaluator,
    )


def _decimal_greater_than_rule(
    *,
    rule_code: str,
    rule_name: str,
    field_name: str,
    minimum: str,
    severity: QualitySeverity,
    blocking: bool,
) -> QualityRuleDefinition:
    threshold = Decimal(minimum)

    def evaluator(rows: list[dict[str, str]]) -> QualityRuleOutcome:
        failures = []
        for row in rows:
            value = row.get(field_name, "").strip()
            if value == "":
                failures.append(_sample_payload(row, field_name))
                continue
            try:
                if Decimal(value) <= threshold:
                    failures.append(_sample_payload(row, field_name))
            except InvalidOperation:
                failures.append(_sample_payload(row, field_name))

        return QualityRuleOutcome(
            row_count_evaluated=len(rows),
            row_count_failed=len(failures),
            failed_payloads=failures,
            sample_failures=failures[:MAX_SAMPLE_FAILURES],
            details={"field_name": field_name, "minimum": minimum},
        )

    return QualityRuleDefinition(
        rule_code=rule_code,
        rule_name=rule_name,
        severity=severity,
        blocking=blocking,
        evaluator=evaluator,
    )


def _decimal_greater_or_equal_rule(
    *,
    rule_code: str,
    rule_name: str,
    field_name: str,
    minimum: str,
    severity: QualitySeverity,
    blocking: bool,
) -> QualityRuleDefinition:
    threshold = Decimal(minimum)

    def evaluator(rows: list[dict[str, str]]) -> QualityRuleOutcome:
        failures = []
        for row in rows:
            value = row.get(field_name, "").strip()
            if value == "":
                failures.append(_sample_payload(row, field_name))
                continue
            try:
                if Decimal(value) < threshold:
                    failures.append(_sample_payload(row, field_name))
            except InvalidOperation:
                failures.append(_sample_payload(row, field_name))

        return QualityRuleOutcome(
            row_count_evaluated=len(rows),
            row_count_failed=len(failures),
            failed_payloads=failures,
            sample_failures=failures[:MAX_SAMPLE_FAILURES],
            details={"field_name": field_name, "minimum": minimum},
        )

    return QualityRuleDefinition(
        rule_code=rule_code,
        rule_name=rule_name,
        severity=severity,
        blocking=blocking,
        evaluator=evaluator,
    )


def _unique_fields_rule(
    *,
    rule_code: str,
    rule_name: str,
    field_names: tuple[str, ...],
    severity: QualitySeverity,
    blocking: bool,
) -> QualityRuleDefinition:
    def evaluator(rows: list[dict[str, str]]) -> QualityRuleOutcome:
        seen: dict[tuple[str, ...], int] = {}
        failures: list[dict[str, str]] = []

        for row in rows:
            key = tuple(row.get(field_name, "").strip() for field_name in field_names)
            seen[key] = seen.get(key, 0) + 1
            if seen[key] > 1:
                failures.append(_sample_payload(row, *field_names))

        return QualityRuleOutcome(
            row_count_evaluated=len(rows),
            row_count_failed=len(failures),
            failed_payloads=failures,
            sample_failures=failures[:MAX_SAMPLE_FAILURES],
            details={"field_names": list(field_names)},
        )

    return QualityRuleDefinition(
        rule_code=rule_code,
        rule_name=rule_name,
        severity=severity,
        blocking=blocking,
        evaluator=evaluator,
    )


def _sample_payload(row: dict[str, str], *focus_fields: str) -> dict[str, str]:
    sample = {field_name: row.get(field_name, "") for field_name in focus_fields}
    for field_name in ("cnpj_fundo", "data_competencia", "competencia", "nome_fundo"):
        if field_name in row and field_name not in sample:
            sample[field_name] = row[field_name]
    return sample
