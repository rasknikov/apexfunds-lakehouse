from __future__ import annotations

from datetime import datetime

from apex_lakehouse.control_plane.enums import (
    PipelineRunStatus,
    QuarantineStatus,
    TriggerMode,
)
from apex_lakehouse.control_plane.records import PipelineRunRecord, QuarantineRecord, merge_details


def test_pipeline_run_record_defaults() -> None:
    record = PipelineRunRecord(
        pipeline_name="cvm_informe_diario_ingestion",
        source_system="cvm",
        dataset_name="informe_diario",
        trigger_mode=TriggerMode.SCHEDULED,
        status=PipelineRunStatus.RUNNING,
        started_at=datetime(2024, 1, 15, 10, 0, 0),
    )

    assert record.rows_read == 0
    assert record.rows_written == 0
    assert record.rows_quarantined == 0
    assert record.files_discovered == 0
    assert record.files_processed == 0
    assert record.files_skipped == 0
    assert record.details_json == {}


def test_quarantine_record_defaults() -> None:
    pipeline_run = PipelineRunRecord(
        pipeline_name="quality_gate",
        source_system="cvm",
        dataset_name="fundos",
        trigger_mode=TriggerMode.REPLAY,
        status=PipelineRunStatus.FAILED,
        started_at=datetime(2024, 1, 15, 10, 0, 0),
    )

    record = QuarantineRecord(
        pipeline_run_id=pipeline_run.pipeline_run_id,
        source_system="cvm",
        dataset_name="fundos",
        layer_name="silver",
        rule_code="fund_missing_registry",
        reason="Fund without registry match",
        payload_json={"cnpj_fundo": "123"},
        quarantined_at=datetime(2024, 1, 15, 10, 5, 0),
    )

    assert record.pipeline_run_id == pipeline_run.pipeline_run_id
    assert record.quarantine_status is QuarantineStatus.OPEN
    assert record.record_locator is None
    assert record.resolved_at is None


def test_merge_details_returns_new_dict_without_mutating_inputs() -> None:
    base = {"dataset": "informe_diario"}
    extra = {"status": "running"}

    merged = merge_details(base, extra)

    assert merged == {"dataset": "informe_diario", "status": "running"}
    assert merged is not base
    assert base == {"dataset": "informe_diario"}
