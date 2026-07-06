from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from apex_lakehouse.config import PlatformSettings
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
from apex_lakehouse.control_plane.repository import ControlPlaneRepository


def _build_repository_with_session() -> tuple[ControlPlaneRepository, MagicMock]:
    repository = ControlPlaneRepository(MagicMock())
    session = MagicMock()
    repository._session_factory = MagicMock(return_value=session)
    return repository, session


def test_from_settings_creates_sync_engine() -> None:
    settings = PlatformSettings.from_env()

    with patch("apex_lakehouse.control_plane.repository.create_engine") as mock_create_engine:
        repository = ControlPlaneRepository.from_settings(settings=settings, echo=True)

    mock_create_engine.assert_called_once_with(
        settings.postgres.sqlalchemy_url,
        future=True,
        pool_pre_ping=True,
        echo=True,
    )
    assert isinstance(repository, ControlPlaneRepository)


def test_session_commits_and_closes_on_success() -> None:
    repository, session = _build_repository_with_session()

    with repository.session() as active_session:
        assert active_session is session

    session.commit.assert_called_once()
    session.rollback.assert_not_called()
    session.close.assert_called_once()


def test_session_rolls_back_and_closes_on_failure() -> None:
    repository, session = _build_repository_with_session()

    with pytest.raises(RuntimeError, match="boom"):
        with repository.session():
            raise RuntimeError("boom")

    session.commit.assert_not_called()
    session.rollback.assert_called_once()
    session.close.assert_called_once()


def test_insert_pipeline_run_serializes_enums_and_json() -> None:
    repository, session = _build_repository_with_session()
    record = PipelineRunRecord(
        pipeline_run_id=uuid4(),
        pipeline_name="cvm_informe_diario_ingestion",
        source_system="cvm",
        dataset_name="informe_diario",
        trigger_mode=TriggerMode.SCHEDULED,
        status=PipelineRunStatus.RUNNING,
        started_at=datetime(2024, 1, 15, 10, 0, 0),
        details_json={"step": "discover"},
    )

    repository.insert_pipeline_run(record)

    assert session.execute.call_count == 1
    _, payload = session.execute.call_args[0]
    assert payload["pipeline_run_id"] == str(record.pipeline_run_id)
    assert payload["trigger_mode"] == "scheduled"
    assert payload["status"] == "running"
    assert payload["details_json"] == '{"step": "discover"}'
    session.commit.assert_called_once()
    session.close.assert_called_once()


def test_upsert_source_file_serializes_status_and_optional_run_id() -> None:
    repository, session = _build_repository_with_session()
    run_id = uuid4()
    record = SourceFileRecord(
        source_system="cvm",
        dataset_name="informe_diario",
        source_url="https://example.com/file.zip",
        file_name="file.zip",
        file_hash="abc123",
        first_seen_at=datetime(2024, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2024, 1, 15, 10, 1, 0),
        status=SourceFileStatus.INGESTED,
        last_pipeline_run_id=run_id,
    )

    repository.upsert_source_file(record)

    _, payload = session.execute.call_args[0]
    assert payload["source_file_id"] == str(record.source_file_id)
    assert payload["status"] == "ingested"
    assert payload["last_pipeline_run_id"] == str(run_id)


def test_upsert_ingestion_state_serializes_uuid_fields() -> None:
    repository, session = _build_repository_with_session()
    success_run_id = uuid4()
    attempted_run_id = uuid4()
    record = IngestionStateRecord(
        source_system="cvm",
        dataset_name="informe_diario",
        updated_at=datetime(2024, 1, 15, 10, 0, 0),
        updated_by="airflow",
        last_successful_run_id=success_run_id,
        last_attempted_run_id=attempted_run_id,
    )

    repository.upsert_ingestion_state(record)

    _, payload = session.execute.call_args[0]
    assert payload["last_successful_run_id"] == str(success_run_id)
    assert payload["last_attempted_run_id"] == str(attempted_run_id)


def test_insert_data_quality_result_serializes_enum_and_json() -> None:
    repository, session = _build_repository_with_session()
    record = DataQualityResultRecord(
        pipeline_run_id=uuid4(),
        dataset_name="silver.fundos",
        layer_name="silver",
        rule_code="cnpj_not_null",
        rule_name="CNPJ must not be null",
        severity=QualitySeverity.CRITICAL,
        status=QualityCheckStatus.FAILED,
        blocking=True,
        row_count_evaluated=100,
        row_count_failed=3,
        evaluated_at=datetime(2024, 1, 15, 11, 0, 0),
        details_json={"sample_failures": 3},
    )

    repository.insert_data_quality_result(record)

    _, payload = session.execute.call_args[0]
    assert payload["pipeline_run_id"] == str(record.pipeline_run_id)
    assert payload["severity"] == "critical"
    assert payload["status"] == "failed"
    assert payload["details_json"] == '{"sample_failures": 3}'


def test_insert_quarantine_record_serializes_status_and_payload() -> None:
    repository, session = _build_repository_with_session()
    record = QuarantineRecord(
        pipeline_run_id=uuid4(),
        source_system="cvm",
        dataset_name="silver.fundos",
        layer_name="silver",
        rule_code="missing_fund",
        reason="Missing fund registry entry",
        payload_json={"cnpj_fundo": "123"},
        quarantined_at=datetime(2024, 1, 15, 12, 0, 0),
        quarantine_status=QuarantineStatus.REPLAY_PENDING,
    )

    repository.insert_quarantine_record(record)

    _, payload = session.execute.call_args[0]
    assert payload["pipeline_run_id"] == str(record.pipeline_run_id)
    assert payload["quarantine_status"] == "replay_pending"
    assert payload["payload_json"] == '{"cnpj_fundo": "123"}'


def test_update_quarantine_status_serializes_status_and_resolution_fields() -> None:
    repository, session = _build_repository_with_session()
    quarantine_id = uuid4()
    resolved_at = datetime(2024, 1, 15, 13, 0, 0)

    repository.update_quarantine_status(
        quarantine_id,
        quarantine_status=QuarantineStatus.RESOLVED.value,
        resolved_at=resolved_at,
        resolution_note="Manually replayed",
    )

    _, payload = session.execute.call_args[0]
    assert payload["quarantine_id"] == str(quarantine_id)
    assert payload["quarantine_status"] == "resolved"
    assert payload["resolved_at"] == resolved_at
    assert payload["resolution_note"] == "Manually replayed"


def test_get_latest_pipeline_run_returns_mapping_as_dict() -> None:
    repository, session = _build_repository_with_session()
    row = {
        "pipeline_name": "cvm_informe_diario_ingestion",
        "status": "succeeded",
    }
    result_proxy = MagicMock()
    result_proxy.mappings.return_value.first.return_value = row
    session.execute.return_value = result_proxy

    result = repository.get_latest_pipeline_run("cvm_informe_diario_ingestion")

    assert result == row


def test_list_latest_pipeline_runs_returns_rows_as_dicts() -> None:
    repository, session = _build_repository_with_session()
    rows = [{"pipeline_name": "cvm_gold_build", "status": "succeeded"}]
    result_proxy = MagicMock()
    result_proxy.mappings.return_value.all.return_value = rows
    session.execute.return_value = result_proxy

    result = repository.list_latest_pipeline_runs(limit=5, pipeline_name="cvm_gold_build")

    assert result == rows
    _, payload = session.execute.call_args[0]
    assert payload["limit"] == 5
    assert payload["pipeline_name"] == "cvm_gold_build"


def test_get_ingestion_state_returns_none_when_missing() -> None:
    repository, session = _build_repository_with_session()
    result_proxy = MagicMock()
    result_proxy.mappings.return_value.first.return_value = None
    session.execute.return_value = result_proxy

    result = repository.get_ingestion_state("cvm", "informe_diario")

    assert result is None


def test_get_source_files_by_urls_returns_mapped_rows() -> None:
    repository, session = _build_repository_with_session()
    row = {
        "source_url": "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv",
        "file_name": "cad_fi.csv",
        "status": "ingested",
    }
    result_proxy = MagicMock()
    result_proxy.mappings.return_value.all.return_value = [row]
    session.execute.return_value = result_proxy

    result = repository.get_source_files_by_urls(
        source_system="cvm",
        source_urls=["https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"],
    )

    assert result == [row]


def test_list_latest_quality_results_returns_rows_as_dicts() -> None:
    repository, session = _build_repository_with_session()
    rows = [{"dataset_name": "fundos_informe_diario", "status": "passed"}]
    result_proxy = MagicMock()
    result_proxy.mappings.return_value.all.return_value = rows
    session.execute.return_value = result_proxy

    result = repository.list_latest_quality_results(limit=10, dataset_name="fundos_informe_diario")

    assert result == rows
    _, payload = session.execute.call_args[0]
    assert payload["limit"] == 10
    assert payload["dataset_name"] == "fundos_informe_diario"


def test_check_health_executes_select_one() -> None:
    repository, session = _build_repository_with_session()
    result_proxy = MagicMock()
    result_proxy.scalar_one.return_value = 1
    session.execute.return_value = result_proxy

    result = repository.check_health()

    assert result is True


def test_mark_pipeline_run_finished_updates_terminal_fields() -> None:
    repository, session = _build_repository_with_session()
    run_id = uuid4()
    finished_at = datetime(2024, 1, 15, 12, 30, 0)

    repository.mark_pipeline_run_finished(
        run_id,
        status="succeeded",
        finished_at=finished_at,
        rows_read=10,
        rows_written=8,
        rows_quarantined=2,
        files_discovered=1,
        files_processed=1,
        files_skipped=0,
    )

    _, payload = session.execute.call_args[0]
    assert payload["pipeline_run_id"] == str(run_id)
    assert payload["status"] == "succeeded"
    assert payload["finished_at"] == finished_at
    assert payload["rows_read"] == 10
