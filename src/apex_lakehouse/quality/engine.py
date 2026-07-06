"""CSV-backed data-quality engine for silver datasets."""

from __future__ import annotations

import csv
from pathlib import Path

from apex_lakehouse.control_plane.enums import QualityCheckStatus
from apex_lakehouse.control_plane.records import DataQualityResultRecord
from apex_lakehouse.quality.models import (
    DatasetQualityEvaluation,
    DatasetQualityRequest,
    PromotionGateDecision,
)
from apex_lakehouse.quality.rules import get_silver_quality_rules
from apex_lakehouse.types import JsonDict


class DatasetQualityEngine:
    """Evaluate dataset rules and build control-plane quality records."""

    def evaluate(
        self,
        request: DatasetQualityRequest,
        *,
        local_dataset_path: Path,
    ) -> DatasetQualityEvaluation:
        rows = _read_csv_rows(local_dataset_path)
        rules = get_silver_quality_rules(request.dataset_name)
        records: list[DataQualityResultRecord] = []
        rule_outcomes = {}

        for rule in rules:
            outcome = rule.evaluator(rows)
            rule_outcomes[rule.rule_code] = outcome
            status = (
                QualityCheckStatus.FAILED
                if outcome.row_count_failed > 0
                else QualityCheckStatus.PASSED
            )
            details: JsonDict = dict(outcome.details)
            if outcome.sample_failures:
                details["sample_failures"] = outcome.sample_failures

            records.append(
                DataQualityResultRecord(
                    pipeline_run_id=request.pipeline_run_id,
                    dataset_name=request.dataset_name,
                    layer_name=request.layer_name,
                    rule_code=rule.rule_code,
                    rule_name=rule.rule_name,
                    severity=rule.severity,
                    status=status,
                    blocking=rule.blocking,
                    row_count_evaluated=outcome.row_count_evaluated,
                    row_count_failed=outcome.row_count_failed,
                    evaluated_at=request.silver_result.request.primary_input.request.source_file.last_seen_at,
                    partition_key=request.silver_result.partition_key,
                    failure_ratio=outcome.failure_ratio,
                    details_json=details,
                )
            )

        gate = _build_promotion_gate(records)
        details = {
            "rule_count": len(records),
            "blocking_failures": gate.blocking_failures,
            "failed_rules": gate.failed_rules,
        }
        return DatasetQualityEvaluation(
            request=request,
            local_dataset_path=local_dataset_path.resolve(),
            records=records,
            gate=gate,
            rule_outcomes=rule_outcomes,
            details=details,
        )


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        return list(csv.DictReader(file_obj))


def _build_promotion_gate(records: list[DataQualityResultRecord]) -> PromotionGateDecision:
    failed_records = [record for record in records if record.status is QualityCheckStatus.FAILED]
    blocking_failures = sum(
        1
        for record in failed_records
        if record.blocking
    )
    if blocking_failures > 0:
        return PromotionGateDecision(
            allowed=False,
            blocking_failures=blocking_failures,
            failed_rules=len(failed_records),
            reason="Blocking quality rules failed",
        )

    if failed_records:
        return PromotionGateDecision(
            allowed=True,
            blocking_failures=0,
            failed_rules=len(failed_records),
            reason="Only non-blocking quality rules failed",
        )

    return PromotionGateDecision(
        allowed=True,
        blocking_failures=0,
        failed_rules=0,
        reason="All quality rules passed",
    )
