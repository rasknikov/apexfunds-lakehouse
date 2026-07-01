from __future__ import annotations

from datetime import date, datetime
from datetime import timezone
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from apex_lakehouse.control_plane.enums import (
    PipelineRunStatus,
    QualityCheckStatus,
    QualitySeverity,
    QuarantineStatus,
    SourceFileStatus,
    TriggerMode,
)
from apex_lakehouse.control_plane.records import (
    DataQualityResultRecord,
    IngestionStateRecord,
    PipelineRunRecord,
    QuarantineRecord,
    SourceFileRecord,
)
from apex_lakehouse.control_plane.service import (
    get_control_plane_repository,
    upgrade_control_plane,
)


def _assert_same_utc_timestamp(actual: datetime, expected: datetime) -> None:
    assert actual.tzinfo is not None
    assert actual == expected.replace(tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def integration_repository():
    try:
        upgrade_control_plane()
        repository = get_control_plane_repository()
        with repository.session() as session:
            session.execute(text("select 1"))
    except OperationalError as exc:
        pytest.skip(f"PostgreSQL integration environment unavailable: {exc}")

    return repository


@pytest.fixture(autouse=True)
def clean_control_plane_tables(integration_repository) -> None:
    with integration_repository.session() as session:
        session.execute(
            text(
                """
                truncate table
                    quarantine.base_records,
                    ops.data_quality_results,
                    ops.source_file_registry,
                    ops.ingestion_state,
                    ops.pipeline_run_log
                """
            )
        )


def test_pipeline_run_roundtrip_and_finish_update(integration_repository) -> None:
    pipeline_name = f"cvm_informe_diario_ingestion_{uuid4().hex}"
    dataset_name = f"informe_diario_{uuid4().hex}"
    record = PipelineRunRecord(
        pipeline_name=pipeline_name,
        source_system="cvm",
        dataset_name=dataset_name,
        trigger_mode=TriggerMode.SCHEDULED,
        status=PipelineRunStatus.RUNNING,
        started_at=datetime(2024, 1, 15, 10, 0, 0),
        details_json={"phase": "discover"},
    )

    integration_repository.insert_pipeline_run(record)
    latest_before_finish = integration_repository.get_latest_pipeline_run(record.pipeline_name)

    assert latest_before_finish is not None
    assert latest_before_finish["pipeline_run_id"] == record.pipeline_run_id
    assert latest_before_finish["status"] == "running"
    assert latest_before_finish["details_json"] == {"phase": "discover"}

    finished_at = datetime(2024, 1, 15, 10, 30, 0)
    integration_repository.mark_pipeline_run_finished(
        record.pipeline_run_id,
        status="succeeded",
        finished_at=finished_at,
        rows_read=10,
        rows_written=8,
        rows_quarantined=2,
        files_discovered=1,
        files_processed=1,
        files_skipped=0,
    )

    latest_after_finish = integration_repository.get_latest_pipeline_run(record.pipeline_name)

    assert latest_after_finish is not None
    assert latest_after_finish["status"] == "succeeded"
    _assert_same_utc_timestamp(latest_after_finish["finished_at"], finished_at)
    assert latest_after_finish["rows_written"] == 8
    assert latest_after_finish["rows_quarantined"] == 2


def test_source_file_upsert_updates_existing_row(integration_repository) -> None:
    dataset_name = f"informe_diario_{uuid4().hex}"
    source_url = f"https://dados.cvm.gov.br/{uuid4().hex}/informe.zip"
    first_record = SourceFileRecord(
        source_system="cvm",
        dataset_name=dataset_name,
        source_url=source_url,
        file_name="informe_v1.zip",
        file_hash="abc123",
        first_seen_at=datetime(2024, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2024, 1, 15, 10, 0, 0),
        status=SourceFileStatus.DISCOVERED,
    )
    second_record = SourceFileRecord(
        source_system="cvm",
        dataset_name=dataset_name,
        source_url=source_url,
        file_name="informe_v2.zip",
        file_hash="abc123",
        first_seen_at=datetime(2024, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2024, 1, 15, 11, 0, 0),
        latest_ingested_at=datetime(2024, 1, 15, 11, 5, 0),
        status=SourceFileStatus.INGESTED,
    )

    integration_repository.upsert_source_file(first_record)
    integration_repository.upsert_source_file(second_record)

    with integration_repository.session() as session:
        row = (
            session.execute(
                text(
                    """
                    select file_name, status, last_seen_at, latest_ingested_at
                    from ops.source_file_registry
                    where dataset_name = :dataset_name
                    """
                ),
                {"dataset_name": dataset_name},
            )
            .mappings()
            .one()
        )
        count = session.execute(text("select count(*) from ops.source_file_registry")).scalar_one()

    assert count == 1
    assert row["file_name"] == "informe_v2.zip"
    assert row["status"] == "ingested"
    _assert_same_utc_timestamp(row["last_seen_at"], datetime(2024, 1, 15, 11, 0, 0))
    _assert_same_utc_timestamp(
        row["latest_ingested_at"],
        datetime(2024, 1, 15, 11, 5, 0),
    )


def test_ingestion_state_upsert_increments_lock_version(integration_repository) -> None:
    dataset_name = f"informe_diario_{uuid4().hex}"
    first_record = IngestionStateRecord(
        source_system="cvm",
        dataset_name=dataset_name,
        watermark_business_date=date(2024, 1, 15),
        updated_at=datetime(2024, 1, 15, 10, 0, 0),
        updated_by="airflow",
    )
    second_record = IngestionStateRecord(
        source_system="cvm",
        dataset_name=dataset_name,
        watermark_business_date=date(2024, 1, 16),
        updated_at=datetime(2024, 1, 16, 10, 0, 0),
        updated_by="airflow",
    )

    integration_repository.upsert_ingestion_state(first_record)
    integration_repository.upsert_ingestion_state(second_record)

    state = integration_repository.get_ingestion_state("cvm", dataset_name)

    assert state is not None
    assert state["watermark_business_date"] == date(2024, 1, 16)
    assert state["lock_version"] == 1
    assert state["updated_by"] == "airflow"


def test_quality_and_quarantine_records_persist_with_foreign_key(integration_repository) -> None:
    dataset_name = f"silver.fundos_{uuid4().hex}"
    pipeline_name = f"silver_quality_gate_{uuid4().hex}"
    pipeline_run = PipelineRunRecord(
        pipeline_name=pipeline_name,
        source_system="cvm",
        dataset_name=dataset_name,
        trigger_mode=TriggerMode.REPLAY,
        status=PipelineRunStatus.FAILED,
        started_at=datetime(2024, 1, 15, 12, 0, 0),
    )
    quality_result = DataQualityResultRecord(
        pipeline_run_id=pipeline_run.pipeline_run_id,
        dataset_name=dataset_name,
        layer_name="silver",
        rule_code="cnpj_not_null",
        rule_name="CNPJ must not be null",
        severity=QualitySeverity.CRITICAL,
        status=QualityCheckStatus.FAILED,
        blocking=True,
        row_count_evaluated=100,
        row_count_failed=2,
        failure_ratio=0.02,
        evaluated_at=datetime(2024, 1, 15, 12, 5, 0),
        details_json={"sample_failures": 2},
    )
    quarantine_record = QuarantineRecord(
        pipeline_run_id=pipeline_run.pipeline_run_id,
        source_system="cvm",
        dataset_name=dataset_name,
        layer_name="silver",
        rule_code="cnpj_not_null",
        reason="Null CNPJ in promoted row",
        payload_json={"row_id": "abc"},
        quarantined_at=datetime(2024, 1, 15, 12, 6, 0),
        quarantine_status=QuarantineStatus.REPLAY_PENDING,
    )

    integration_repository.insert_pipeline_run(pipeline_run)
    integration_repository.insert_data_quality_result(quality_result)
    integration_repository.insert_quarantine_record(quarantine_record)

    with integration_repository.session() as session:
        quality_row = (
            session.execute(
                text(
                    """
                    select severity, status, details_json
                    from ops.data_quality_results
                    where pipeline_run_id = :pipeline_run_id
                    """
                ),
                {"pipeline_run_id": str(pipeline_run.pipeline_run_id)},
            )
            .mappings()
            .one()
        )
        quarantine_row = (
            session.execute(
                text(
                    """
                    select quarantine_status, payload_json
                    from quarantine.base_records
                    where pipeline_run_id = :pipeline_run_id
                    """
                ),
                {"pipeline_run_id": str(pipeline_run.pipeline_run_id)},
            )
            .mappings()
            .one()
        )

    assert quality_row["severity"] == "critical"
    assert quality_row["status"] == "failed"
    assert quality_row["details_json"] == {"sample_failures": 2}
    assert quarantine_row["quarantine_status"] == "replay_pending"
    assert quarantine_row["payload_json"] == {"row_id": "abc"}
