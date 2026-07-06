"""Contracts for data-quality evaluation and promotion gating."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable
from uuid import UUID

from apex_lakehouse.control_plane.records import DataQualityResultRecord
from apex_lakehouse.control_plane.enums import QualitySeverity
from apex_lakehouse.ingestion.cvm.silver_models import SilverBuildResult
from apex_lakehouse.types import JsonDict

RuleEvaluator = Callable[[list[dict[str, str]]], "QualityRuleOutcome"]


@dataclass(frozen=True)
class QualityRuleDefinition:
    rule_code: str
    rule_name: str
    severity: QualitySeverity
    blocking: bool
    evaluator: RuleEvaluator


@dataclass(frozen=True)
class QualityRuleOutcome:
    row_count_evaluated: int
    row_count_failed: int
    failed_payloads: list[dict[str, str]] = field(default_factory=list)
    sample_failures: list[dict[str, str]] = field(default_factory=list)
    details: JsonDict = field(default_factory=dict)

    @property
    def failure_ratio(self) -> float | None:
        if self.row_count_evaluated == 0:
            return None
        return self.row_count_failed / self.row_count_evaluated


@dataclass(frozen=True)
class DatasetQualityRequest:
    silver_result: SilverBuildResult
    pipeline_run_id: UUID
    dataset_name: str
    layer_name: str = "silver"


@dataclass(frozen=True)
class PromotionGateDecision:
    allowed: bool
    blocking_failures: int
    failed_rules: int
    reason: str


@dataclass(frozen=True)
class DatasetQualityEvaluation:
    request: DatasetQualityRequest
    local_dataset_path: Path
    records: list[DataQualityResultRecord]
    gate: PromotionGateDecision
    rule_outcomes: dict[str, QualityRuleOutcome] = field(default_factory=dict)
    details: JsonDict = field(default_factory=dict)
