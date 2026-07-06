from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from apex_lakehouse.control_plane.enums import QualityCheckStatus
from apex_lakehouse.control_plane.records import SourceFileRecord
from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildRequest, BronzeBuildResult, BronzeParseSummary
from apex_lakehouse.ingestion.cvm.silver_models import (
    SilverBuildRequest,
    SilverBuildResult,
    SilverTransformSummary,
)
from apex_lakehouse.quality.engine import DatasetQualityEngine
from apex_lakehouse.quality.models import DatasetQualityRequest
from apex_lakehouse.storage.models import ObjectStoragePath


def test_engine_blocks_promotion_when_blocking_rules_fail(tmp_path: Path) -> None:
    dataset_path = tmp_path / "fundos_informe_diario.csv"
    dataset_path.write_text(
        "cnpj_fundo,data_competencia,valor_cota,patrimonio_liquido,nome_fundo\n"
        "12345678000190,2024-01-15,0,-1,\n",
        encoding="utf-8",
    )
    silver_result = _build_silver_result(
        dataset_name="fundos_informe_diario",
        output_path=dataset_path,
    )
    request = DatasetQualityRequest(
        silver_result=silver_result,
        pipeline_run_id=uuid4(),
        dataset_name="fundos_informe_diario",
    )

    evaluation = DatasetQualityEngine().evaluate(request, local_dataset_path=dataset_path)

    assert evaluation.gate.allowed is False
    assert evaluation.gate.blocking_failures == 2
    assert any(record.rule_code == "quota_value_positive" for record in evaluation.records)
    assert any(record.status is QualityCheckStatus.FAILED for record in evaluation.records)


def test_engine_allows_promotion_when_only_warning_fails(tmp_path: Path) -> None:
    dataset_path = tmp_path / "fundos_informe_diario.csv"
    dataset_path.write_text(
        "cnpj_fundo,data_competencia,valor_cota,patrimonio_liquido,nome_fundo\n"
        "12345678000190,2024-01-15,1.23,1000,\n",
        encoding="utf-8",
    )
    silver_result = _build_silver_result(
        dataset_name="fundos_informe_diario",
        output_path=dataset_path,
    )
    request = DatasetQualityRequest(
        silver_result=silver_result,
        pipeline_run_id=uuid4(),
        dataset_name="fundos_informe_diario",
    )

    evaluation = DatasetQualityEngine().evaluate(request, local_dataset_path=dataset_path)

    assert evaluation.gate.allowed is True
    assert evaluation.gate.failed_rules == 1
    warning_record = next(record for record in evaluation.records if record.rule_code == "fund_registry_match")
    assert warning_record.blocking is False
    assert warning_record.status is QualityCheckStatus.FAILED


def _build_silver_result(*, dataset_name: str, output_path: Path) -> SilverBuildResult:
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
        silver_dataset_name=dataset_name,
        partition_key="ano=2024/mes=01",
        transform_summary=SilverTransformSummary(
            output_path=output_path,
            schema_path=output_path.with_suffix(".json"),
            row_count=1,
            deduplicated_rows=0,
            columns=tuple(),
            input_dataset_name="informe_diario",
        ),
        data_path=ObjectStoragePath(bucket="lakehouse", key=f"silver/{dataset_name}/part.csv"),
        schema_path=ObjectStoragePath(bucket="lakehouse", key=f"silver/{dataset_name}/schema.json"),
    )
