from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from apex_lakehouse.control_plane.records import SourceFileRecord
from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildRequest, BronzeBuildResult, BronzeParseSummary
from apex_lakehouse.ingestion.cvm.silver_models import SilverBuildRequest, SilverBuildResult, SilverTransformSummary
from apex_lakehouse.quality.models import DatasetQualityRequest
from apex_lakehouse.quality.service import DatasetQualityService
from apex_lakehouse.storage.models import ObjectStoragePath
from apex_lakehouse.storage.staging import LocalStagingManager


def test_service_persists_quality_results_and_uses_local_file(tmp_path: Path) -> None:
    dataset_path = tmp_path / "fundos.csv"
    dataset_path.write_text(
        "cnpj_fundo,nome_fundo,data_registro\n12345678000190,Fundo XPTO,2024-01-15\n",
        encoding="utf-8",
    )
    silver_result = _build_silver_result(output_path=dataset_path)
    inserted_records = []
    repository = type(
        "RepositoryStub",
        (),
        {"insert_data_quality_result": lambda self, record: inserted_records.append(record)},
    )()
    service = DatasetQualityService(
        repository=repository,  # type: ignore[arg-type]
        engine=__import__("apex_lakehouse.quality.engine", fromlist=["DatasetQualityEngine"]).DatasetQualityEngine(),
        storage_client=type("StorageStub", (), {"download_file": lambda self, source, destination: destination})(),  # type: ignore[arg-type]
        staging_manager=LocalStagingManager(tmp_path / "staging"),
    )

    evaluation = service.evaluate(
        DatasetQualityRequest(
            silver_result=silver_result,
            pipeline_run_id=uuid4(),
            dataset_name="fundos",
        )
    )

    assert evaluation.gate.allowed is True
    assert len(inserted_records) == 3


def test_service_downloads_remote_file_when_local_output_is_missing(tmp_path: Path) -> None:
    remote_source = tmp_path / "remote.csv"
    remote_source.write_text(
        "cnpj_fundo,nome_fundo,data_registro\n12345678000190,Fundo XPTO,2024-01-15\n",
        encoding="utf-8",
    )
    missing_local = tmp_path / "missing.csv"
    silver_result = _build_silver_result(output_path=missing_local)
    downloads = []

    class StorageStub:
        def download_file(self, source: ObjectStoragePath, destination_path: Path) -> Path:
            downloads.append((source, destination_path))
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(remote_source, destination_path)
            return destination_path

    repository = type(
        "RepositoryStub",
        (),
        {"insert_data_quality_result": lambda self, record: None},
    )()
    service = DatasetQualityService(
        repository=repository,  # type: ignore[arg-type]
        engine=__import__("apex_lakehouse.quality.engine", fromlist=["DatasetQualityEngine"]).DatasetQualityEngine(),
        storage_client=StorageStub(),  # type: ignore[arg-type]
        staging_manager=LocalStagingManager(tmp_path / "staging"),
    )

    evaluation = service.evaluate(
        DatasetQualityRequest(
            silver_result=silver_result,
            pipeline_run_id=uuid4(),
            dataset_name="fundos",
        )
    )

    assert downloads
    assert evaluation.local_dataset_path.exists()
    assert evaluation.gate.allowed is True


def _build_silver_result(*, output_path: Path) -> SilverBuildResult:
    source_file = SourceFileRecord(
        source_system="cvm",
        dataset_name="cadastro_fundos",
        source_url="https://dados.cvm.gov.br/file.csv",
        file_name="file.csv",
        file_hash="hash123",
        first_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
    )
    bronze_result = BronzeBuildResult(
        request=BronzeBuildRequest(source_file=source_file, updated_by="test-suite"),
        bronze_dataset_name="cvm_cadastro_fundos",
        partition_key="ano=2024/mes=01",
        parse_summary=BronzeParseSummary(
            output_path=Path("ignored.csv"),
            schema_path=Path("ignored.json"),
            row_count=1,
            columns=tuple(),
        ),
        data_path=ObjectStoragePath(bucket="lakehouse", key="bronze/cvm_cadastro_fundos/part.csv"),
        schema_path=ObjectStoragePath(bucket="lakehouse", key="bronze/cvm_cadastro_fundos/schema.json"),
    )
    return SilverBuildResult(
        request=SilverBuildRequest(primary_input=bronze_result, updated_by="test-suite"),
        silver_dataset_name="fundos",
        partition_key="ano=2024/mes=01",
        transform_summary=SilverTransformSummary(
            output_path=output_path,
            schema_path=output_path.with_suffix(".json"),
            row_count=1,
            deduplicated_rows=0,
            columns=tuple(),
            input_dataset_name="cadastro_fundos",
        ),
        data_path=ObjectStoragePath(bucket="lakehouse", key="silver/fundos/part.csv"),
        schema_path=ObjectStoragePath(bucket="lakehouse", key="silver/fundos/schema.json"),
    )
