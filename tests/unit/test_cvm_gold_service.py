from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from apex_lakehouse.control_plane.records import SourceFileRecord
from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildRequest, BronzeBuildResult, BronzeParseSummary
from apex_lakehouse.ingestion.cvm.gold_models import (
    GoldColumnSchema,
    GoldDatasetSummary,
    GoldMartBuildRequest,
)
from apex_lakehouse.ingestion.cvm.gold_service import CvmGoldService
from apex_lakehouse.ingestion.cvm.silver_models import (
    SilverBuildRequest,
    SilverBuildResult,
    SilverTransformSummary,
)
from apex_lakehouse.storage.models import ObjectStoragePath
from apex_lakehouse.storage.staging import LocalStagingManager


def test_build_uploads_three_gold_datasets(tmp_path: Path, monkeypatch) -> None:
    funds_source = tmp_path / "funds.csv"
    funds_source.write_text("cnpj_fundo\n123\n", encoding="utf-8")
    informe_source = tmp_path / "informe.csv"
    informe_source.write_text("cnpj_fundo\n123\n", encoding="utf-8")
    output_directory = tmp_path / "gold-output"
    output_directory.mkdir(parents=True, exist_ok=True)

    summaries = [
        _summary(output_directory, "dim_fundo", 1, None),
        _summary(output_directory, "dim_tempo", 2, "ano=2024/mes=01"),
        _summary(output_directory, "fato_fundo_diario", 2, "ano=2024/mes=01"),
    ]

    class StorageStub:
        def __init__(self) -> None:
            self.download_calls = []
            self.upload_calls = []

        def download_file(self, source: ObjectStoragePath, destination_path: Path) -> Path:
            self.download_calls.append((source, destination_path))
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            source_path = funds_source if "fundos" in source.key else informe_source
            shutil.copy2(source_path, destination_path)
            return destination_path

        def upload_file(self, source_path: Path, destination: ObjectStoragePath, *, content_type=None) -> None:
            self.upload_calls.append((source_path, destination, content_type))

    transformer = type(
        "TransformerStub",
        (),
        {"transform": lambda self, request: summaries},
    )()
    monkeypatch.setattr("apex_lakehouse.ingestion.cvm.gold_service.utc_now", lambda: datetime(2024, 1, 16, 10, 0, 0, tzinfo=timezone.utc))

    service = CvmGoldService(
        storage_client=StorageStub(),  # type: ignore[arg-type]
        path_builder=type(
            "PathBuilderStub",
            (),
            {
                "gold": lambda self, dataset_name, partition_key=None: ObjectStoragePath(
                    bucket="lakehouse",
                    key=f"gold/{dataset_name}" if partition_key is None else f"gold/{dataset_name}/{partition_key}",
                )
            },
        )(),  # type: ignore[arg-type]
        staging_manager=LocalStagingManager(tmp_path / "staging"),
        transformer=transformer,  # type: ignore[arg-type]
    )

    result = service.build(
        GoldMartBuildRequest(
            funds_input=_build_silver_result("fundos"),
            informe_input=_build_silver_result("fundos_informe_diario"),
            updated_by="test-suite",
            pipeline_run_id=uuid4(),
        )
    )

    assert len(result.outputs) == 3
    assert result.details["dataset_count"] == 3
    assert result.details["total_rows"] == 5


def _summary(output_directory: Path, dataset_name: str, row_count: int, partition_key: str | None) -> GoldDatasetSummary:
    output_path = output_directory / f"{dataset_name}.csv"
    output_path.write_text("col\nx\n", encoding="utf-8")
    schema_path = output_directory / f"{dataset_name}.schema.json"
    schema_path.write_text("{}", encoding="utf-8")
    return GoldDatasetSummary(
        dataset_name=dataset_name,
        output_path=output_path,
        schema_path=schema_path,
        row_count=row_count,
        columns=(GoldColumnSchema(name="col", data_type="string", nullable=False),),
        partition_key=partition_key,
    )


def _build_silver_result(dataset_name: str) -> SilverBuildResult:
    bronze_dataset_name = "cvm_cadastro_fundos" if dataset_name == "fundos" else "cvm_informe_diario"
    source_dataset_name = "cadastro_fundos" if dataset_name == "fundos" else "informe_diario"
    source_file = SourceFileRecord(
        source_system="cvm",
        dataset_name=source_dataset_name,
        source_url="https://dados.cvm.gov.br/file.csv",
        file_name="file.csv",
        file_hash="hash123",
        first_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
    )
    bronze_result = BronzeBuildResult(
        request=BronzeBuildRequest(source_file=source_file, updated_by="test-suite"),
        bronze_dataset_name=bronze_dataset_name,
        partition_key="ano=2024/mes=01",
        parse_summary=BronzeParseSummary(
            output_path=Path("ignored.csv"),
            schema_path=Path("ignored.json"),
            row_count=1,
            columns=tuple(),
        ),
        data_path=ObjectStoragePath(bucket="lakehouse", key=f"bronze/{bronze_dataset_name}/part.csv"),
        schema_path=ObjectStoragePath(bucket="lakehouse", key=f"bronze/{bronze_dataset_name}/schema.json"),
    )
    return SilverBuildResult(
        request=SilverBuildRequest(primary_input=bronze_result, updated_by="test-suite"),
        silver_dataset_name=dataset_name,
        partition_key="ano=2024/mes=01",
        transform_summary=SilverTransformSummary(
            output_path=Path("missing.csv"),
            schema_path=Path("missing.json"),
            row_count=1,
            deduplicated_rows=0,
            columns=tuple(),
            input_dataset_name=source_dataset_name,
        ),
        data_path=ObjectStoragePath(bucket="lakehouse", key=f"silver/{dataset_name}/part.csv"),
        schema_path=ObjectStoragePath(bucket="lakehouse", key=f"silver/{dataset_name}/schema.json"),
    )
