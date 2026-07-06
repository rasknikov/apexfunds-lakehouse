from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from apex_lakehouse.control_plane.records import SourceFileRecord
from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildRequest, BronzeBuildResult, BronzeParseSummary
from apex_lakehouse.ingestion.cvm.silver_models import SilverBuildRequest, SilverTransformSummary
from apex_lakehouse.ingestion.cvm.silver_service import CvmSilverService
from apex_lakehouse.storage.models import ObjectStoragePath
from apex_lakehouse.storage.staging import LocalStagingManager


def test_build_downloads_bronze_transforms_and_uploads_silver_assets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bronze_source = tmp_path / "bronze.csv"
    bronze_source.write_text("CNPJ_FUNDO\n12345678000190\n", encoding="utf-8")
    transformed_output = tmp_path / "silver.csv"
    transformed_output.write_text("cnpj_fundo\n12345678000190\n", encoding="utf-8")
    transformed_schema = tmp_path / "schema.json"
    transformed_schema.write_text('{"columns":[]}', encoding="utf-8")

    bronze_result = _build_bronze_result(
        dataset_name="cadastro_fundos",
        source_file=_build_source_file_record(dataset_name="cadastro_fundos"),
        data_path=ObjectStoragePath(bucket="lakehouse", key="bronze/cvm_cadastro_fundos/part.csv"),
    )

    class StorageClientStub:
        def __init__(self) -> None:
            self.download_calls = []
            self.upload_calls = []

        def download_file(self, source: ObjectStoragePath, destination_path: Path) -> Path:
            self.download_calls.append((source, destination_path))
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(bronze_source, destination_path)
            return destination_path

        def upload_file(self, source_path: Path, destination: ObjectStoragePath, *, content_type=None) -> None:
            self.upload_calls.append((source_path, destination, content_type))

    transformer = type(
        "TransformerStub",
        (),
        {
            "transform": lambda self, request: SilverTransformSummary(
                output_path=transformed_output,
                schema_path=transformed_schema,
                row_count=1,
                deduplicated_rows=0,
                columns=tuple(),
                schema_version="v1",
                input_dataset_name="cadastro_fundos",
            )
        },
    )()
    monkeypatch.setattr("apex_lakehouse.ingestion.cvm.silver_service.utc_now", lambda: datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc))

    client = StorageClientStub()
    service = CvmSilverService(
        storage_client=client,  # type: ignore[arg-type]
        path_builder=type(
            "PathBuilderStub",
            (),
            {
                "silver": lambda self, dataset_name, partition_key=None: ObjectStoragePath(
                    bucket="lakehouse",
                    key=f"silver/{dataset_name}" if partition_key is None else f"silver/{dataset_name}/{partition_key}",
                )
            },
        )(),  # type: ignore[arg-type]
        staging_manager=LocalStagingManager(tmp_path / "staging"),
        transformer=transformer,  # type: ignore[arg-type]
    )

    result = service.build(
        SilverBuildRequest(
            primary_input=bronze_result,
            updated_by="test-suite",
        )
    )

    assert result.silver_dataset_name == "fundos"
    assert result.details["row_count"] == 1
    assert result.data_path.key.endswith(
        f"source_file_id={bronze_result.request.source_file.source_file_id}/part-00000.csv"
    )
    assert len(client.download_calls) == 1
    assert len(client.upload_calls) == 2


def _build_bronze_result(
    *,
    dataset_name: str,
    source_file: SourceFileRecord,
    data_path: ObjectStoragePath,
) -> BronzeBuildResult:
    return BronzeBuildResult(
        request=BronzeBuildRequest(
            source_file=source_file,
            updated_by="test-suite",
        ),
        bronze_dataset_name=f"cvm_{dataset_name}",
        partition_key="ano=2024/mes=01",
        parse_summary=BronzeParseSummary(
            output_path=Path("ignored.csv"),
            schema_path=Path("ignored.schema.json"),
            row_count=1,
            columns=tuple(),
        ),
        data_path=data_path,
        schema_path=ObjectStoragePath(bucket="lakehouse", key="ignored/schema.json"),
    )


def _build_source_file_record(*, dataset_name: str) -> SourceFileRecord:
    return SourceFileRecord(
        source_system="cvm",
        dataset_name=dataset_name,
        source_url="https://dados.cvm.gov.br/file.csv",
        file_name="file.csv",
        file_hash="hash123",
        first_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
    )
