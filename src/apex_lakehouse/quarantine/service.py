"""Service that converts failed quality rows into quarantine records."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from apex_lakehouse.control_plane.enums import QualityCheckStatus, QuarantineStatus
from apex_lakehouse.control_plane.records import QuarantineRecord
from apex_lakehouse.control_plane.repository import ControlPlaneRepository
from apex_lakehouse.quality.models import DatasetQualityEvaluation
from apex_lakehouse.quality.rules import get_silver_quality_rules
from apex_lakehouse.quarantine.models import (
    QuarantineBuildRequest,
    QuarantineBuildResult,
    QuarantineReplayRequest,
)
from apex_lakehouse.time import utc_now
from apex_lakehouse.types import JsonDict


class DatasetQuarantineService:
    """Persist quarantine records for failed quality rows and support replay state."""

    def __init__(self, *, repository: ControlPlaneRepository):
        self._repository = repository

    @classmethod
    def from_repository(
        cls,
        repository: ControlPlaneRepository,
    ) -> "DatasetQuarantineService":
        return cls(repository=repository)

    def build(self, request: QuarantineBuildRequest) -> QuarantineBuildResult:
        created_at = utc_now()
        records = self._build_records(
            request.evaluation,
            source_system=request.source_system,
            dataset_name=request.dataset_name,
            layer_name=request.layer_name,
            created_at=created_at,
            include_non_blocking=request.include_non_blocking,
        )
        for record in records:
            self._repository.insert_quarantine_record(record)

        details: JsonDict = {
            "record_count": len(records),
            "blocking_only": not request.include_non_blocking,
        }
        return QuarantineBuildResult(
            request=request,
            records=records,
            created_at=created_at,
            details=details,
        )

    def mark_for_replay(self, request: QuarantineReplayRequest) -> None:
        self._repository.update_quarantine_status(
            request.quarantine_id,
            quarantine_status=QuarantineStatus.REPLAY_PENDING.value,
            resolved_at=request.requested_at,
            resolution_note=request.resolution_note,
        )

    def _build_records(
        self,
        evaluation: DatasetQualityEvaluation,
        *,
        source_system: str,
        dataset_name: str,
        layer_name: str,
        created_at: datetime,
        include_non_blocking: bool,
    ) -> list[QuarantineRecord]:
        rule_definitions = {
            rule.rule_code: rule
            for rule in get_silver_quality_rules(evaluation.request.dataset_name)
        }
        records: list[QuarantineRecord] = []

        for quality_record in evaluation.records:
            if quality_record.status is not QualityCheckStatus.FAILED:
                continue
            if not include_non_blocking and not quality_record.blocking:
                continue

            failed_payloads = evaluation.rule_outcomes.get(
                quality_record.rule_code,
            )
            if failed_payloads is None:
                continue

            rule_definition = rule_definitions[quality_record.rule_code]
            for payload in failed_payloads.failed_payloads:
                records.append(
                    QuarantineRecord(
                        pipeline_run_id=quality_record.pipeline_run_id,
                        source_system=source_system,
                        dataset_name=dataset_name,
                        layer_name=layer_name,
                        rule_code=quality_record.rule_code,
                        reason=rule_definition.rule_name,
                        payload_json=payload,
                        record_locator=_build_record_locator(payload),
                        quarantined_at=created_at,
                    )
                )

        return records


def _build_record_locator(payload: dict[str, str]) -> str | None:
    if payload.get("cnpj_fundo") and payload.get("data_competencia"):
        return (
            f"cnpj_fundo={payload['cnpj_fundo']}/"
            f"data_competencia={payload['data_competencia']}"
        )
    if payload.get("cnpj_fundo") and payload.get("competencia"):
        return (
            f"cnpj_fundo={payload['cnpj_fundo']}/"
            f"competencia={payload['competencia']}"
        )
    if payload.get("cnpj_fundo"):
        return f"cnpj_fundo={payload['cnpj_fundo']}"
    return None
