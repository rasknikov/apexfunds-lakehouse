from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from apex_lakehouse.control_plane.enums import QualityCheckStatus, QualitySeverity, QuarantineStatus
from apex_lakehouse.control_plane.records import DataQualityResultRecord, SourceFileRecord
from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildRequest, BronzeBuildResult, BronzeParseSummary
from apex_lakehouse.ingestion.cvm.silver_models import SilverBuildRequest, SilverBuildResult, SilverTransformSummary
from apex_lakehouse.quality.models import (
    DatasetQualityEvaluation,
    DatasetQualityRequest,
    PromotionGateDecision,
    QualityRuleOutcome,
)
from apex_lakehouse.quarantine.models import QuarantineBuildRequest, QuarantineReplayRequest
from apex_lakehouse.quarantine.service import DatasetQuarantineService
from apex_lakehouse.storage.models import ObjectStoragePath


def test_build_persists_blocking_failures_as_quarantine_records() -> None:
    inserted_records = []
    repository = type(
        "RepositoryStub",
        (),
        {
            "insert_quarantine_record": lambda self, record: inserted_records.append(record),
            "update_quarantine_status": lambda self, *args, **kwargs: None,
        },
    )()
    evaluation = _build_quality_evaluation()
    service = DatasetQuarantineService(repository=repository)  # type: ignore[arg-type]

    result = service.build(
        QuarantineBuildRequest(
            evaluation=evaluation,
            source_system="cvm",
            dataset_name="fundos_informe_diario",
            layer_name="silver",
        )
    )

    assert len(result.records) == 1
    assert result.records[0].rule_code == "quota_value_positive"
    assert result.records[0].payload_json["cnpj_fundo"] == "12345678000190"
    assert result.records[0].record_locator == "cnpj_fundo=12345678000190/data_competencia=2024-01-15"
    assert result.records[0].quarantine_status is QuarantineStatus.OPEN
    assert inserted_records == result.records


def test_mark_for_replay_updates_quarantine_status() -> None:
    calls = []
    repository = type(
        "RepositoryStub",
        (),
        {
            "insert_quarantine_record": lambda self, record: None,
            "update_quarantine_status": lambda self, quarantine_id, **kwargs: calls.append(
                (quarantine_id, kwargs)
            ),
        },
    )()
    service = DatasetQuarantineService(repository=repository)  # type: ignore[arg-type]
    quarantine_id = uuid4()
    requested_at = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    service.mark_for_replay(
        QuarantineReplayRequest(
            quarantine_id=quarantine_id,
            requested_at=requested_at,
            resolution_note="Ready for replay",
        )
    )

    assert calls == [
        (
            quarantine_id,
            {
                "quarantine_status": "replay_pending",
                "resolved_at": requested_at,
                "resolution_note": "Ready for replay",
            },
        )
    ]


def _build_quality_evaluation() -> DatasetQualityEvaluation:
    pipeline_run_id = uuid4()
    quality_request = DatasetQualityRequest(
        silver_result=_build_silver_result(),
        pipeline_run_id=pipeline_run_id,
        dataset_name="fundos_informe_diario",
    )
    return DatasetQualityEvaluation(
        request=quality_request,
        local_dataset_path=Path("ignored.csv"),
        records=[
            DataQualityResultRecord(
                pipeline_run_id=pipeline_run_id,
                dataset_name="fundos_informe_diario",
                layer_name="silver",
                rule_code="quota_value_positive",
                rule_name="Quota value must be greater than zero",
                severity=QualitySeverity.CRITICAL,
                status=QualityCheckStatus.FAILED,
                blocking=True,
                row_count_evaluated=1,
                row_count_failed=1,
                evaluated_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
            ),
            DataQualityResultRecord(
                pipeline_run_id=pipeline_run_id,
                dataset_name="fundos_informe_diario",
                layer_name="silver",
                rule_code="fund_registry_match",
                rule_name="Fund must have registry enrichment",
                severity=QualitySeverity.WARN,
                status=QualityCheckStatus.FAILED,
                blocking=False,
                row_count_evaluated=1,
                row_count_failed=1,
                evaluated_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
            ),
        ],
        gate=PromotionGateDecision(
            allowed=False,
            blocking_failures=1,
            failed_rules=2,
            reason="Blocking quality rules failed",
        ),
        rule_outcomes={
            "quota_value_positive": QualityRuleOutcome(
                row_count_evaluated=1,
                row_count_failed=1,
                failed_payloads=[
                    {
                        "cnpj_fundo": "12345678000190",
                        "data_competencia": "2024-01-15",
                        "valor_cota": "0",
                    }
                ],
                sample_failures=[
                    {
                        "cnpj_fundo": "12345678000190",
                        "data_competencia": "2024-01-15",
                        "valor_cota": "0",
                    }
                ],
            ),
            "fund_registry_match": QualityRuleOutcome(
                row_count_evaluated=1,
                row_count_failed=1,
                failed_payloads=[
                    {
                        "cnpj_fundo": "12345678000190",
                        "data_competencia": "2024-01-15",
                        "nome_fundo": "",
                    }
                ],
                sample_failures=[
                    {
                        "cnpj_fundo": "12345678000190",
                        "data_competencia": "2024-01-15",
                        "nome_fundo": "",
                    }
                ],
            ),
        },
    )


def _build_silver_result() -> SilverBuildResult:
    source_file = SourceFileRecord(
        source_system="cvm",
        dataset_name="informe_diario",
        source_url="https://dados.cvm.gov.br/file.csv",
        file_name="file.csv",
        file_hash="hash123",
        first_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
    )
    bronze_result = BronzeBuildResult(
        request=BronzeBuildRequest(source_file=source_file, updated_by="test-suite"),
        bronze_dataset_name="cvm_informe_diario",
        partition_key="ano=2024/mes=01",
        parse_summary=BronzeParseSummary(
            output_path=Path("ignored.csv"),
            schema_path=Path("ignored.json"),
            row_count=1,
            columns=tuple(),
        ),
        data_path=ObjectStoragePath(bucket="lakehouse", key="bronze/cvm_informe_diario/part.csv"),
        schema_path=ObjectStoragePath(bucket="lakehouse", key="bronze/cvm_informe_diario/schema.json"),
    )
    return SilverBuildResult(
        request=SilverBuildRequest(primary_input=bronze_result, updated_by="test-suite"),
        silver_dataset_name="fundos_informe_diario",
        partition_key="ano=2024/mes=01",
        transform_summary=SilverTransformSummary(
            output_path=Path("ignored.csv"),
            schema_path=Path("ignored.json"),
            row_count=1,
            deduplicated_rows=0,
            columns=tuple(),
            input_dataset_name="informe_diario",
        ),
        data_path=ObjectStoragePath(bucket="lakehouse", key="silver/fundos_informe_diario/part.csv"),
        schema_path=ObjectStoragePath(bucket="lakehouse", key="silver/fundos_informe_diario/schema.json"),
    )
