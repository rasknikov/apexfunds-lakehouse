from __future__ import annotations

import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from apex_lakehouse.control_plane.records import SourceFileRecord
from apex_lakehouse.exceptions import IngestionStateError
from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildRequest, BronzeParseSummary
from apex_lakehouse.ingestion.cvm.bronze_service import CvmBronzeService
from apex_lakehouse.storage.models import ObjectStoragePath
from apex_lakehouse.storage.staging import LocalStagingManager


def test_build_downloads_raw_parses_and_uploads_bronze_assets(tmp_path: Path, monkeypatch) -> None:
    raw_source = tmp_path / "raw-source.csv"
    raw_source.write_text("COLUNA\nvalor\n", encoding="utf-8")
    parse_output = tmp_path / "parsed.csv"
    parse_output.write_text("COLUNA\nvalor\n", encoding="utf-8")
    parse_schema = tmp_path / "parsed.schema.json"
    parse_schema.write_text('{"columns":[]}', encoding="utf-8")
    source_file = _build_source_file_record(
        storage_bucket="raw",
        storage_key="cvm/informe_diario/ano=2024/mes=01/file.csv",
    )

    class StorageClientStub:
        def __init__(self) -> None:
            self.upload_calls = []
            self.download_calls = []

        def download_file(self, source: ObjectStoragePath, destination_path: Path) -> Path:
            self.download_calls.append((source, destination_path))
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(raw_source, destination_path)
            return destination_path

        def upload_file(self, source_path: Path, destination: ObjectStoragePath, *, content_type=None) -> None:
            self.upload_calls.append((source_path, destination, content_type))

    class PathBuilderStub:
        def bronze(self, dataset_name: str, *, partition_key: str | None = None) -> ObjectStoragePath:
            key = f"bronze/{dataset_name}"
            if partition_key is not None:
                key = f"{key}/{partition_key}"
            return ObjectStoragePath(bucket="lakehouse", key=key)

    parser = type(
        "ParserStub",
        (),
        {
            "parse": lambda self, **kwargs: BronzeParseSummary(
                output_path=parse_output,
                schema_path=parse_schema,
                row_count=1,
                columns=tuple(),
                schema_version="v1",
                delimiter=";",
                source_format="csv",
            )
        },
    )()

    processed_at = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("apex_lakehouse.ingestion.cvm.bronze_service.utc_now", lambda: processed_at)

    client = StorageClientStub()
    service = CvmBronzeService(
        storage_client=client,  # type: ignore[arg-type]
        path_builder=PathBuilderStub(),  # type: ignore[arg-type]
        staging_manager=LocalStagingManager(tmp_path / "staging"),
        parser=parser,  # type: ignore[arg-type]
    )

    result = service.build(BronzeBuildRequest(source_file=source_file, updated_by="test-suite"))

    assert result.bronze_dataset_name == "cvm_informe_diario"
    assert result.partition_key == "ano=2024/mes=01"
    assert result.data_path.key.endswith(f"source_file_id={source_file.source_file_id}/part-00000.csv")
    assert result.schema_path.key.endswith(f"source_file_id={source_file.source_file_id}/schema.json")
    assert result.details["row_count"] == 1
    assert client.download_calls[0][0] == ObjectStoragePath(
        bucket="raw",
        key="cvm/informe_diario/ano=2024/mes=01/file.csv",
    )
    assert len(client.upload_calls) == 2
    assert client.upload_calls[0][2] == "text/csv"
    assert client.upload_calls[1][2] == "application/json"


def test_build_requires_raw_storage_location(tmp_path: Path) -> None:
    service = CvmBronzeService(
        storage_client=type("ClientStub", (), {})(),  # type: ignore[arg-type]
        path_builder=type("PathBuilderStub", (), {})(),  # type: ignore[arg-type]
        staging_manager=LocalStagingManager(tmp_path / "staging"),
        parser=type("ParserStub", (), {})(),  # type: ignore[arg-type]
    )
    source_file = _build_source_file_record(storage_bucket=None, storage_key=None)

    with pytest.raises(IngestionStateError):
        service.build(BronzeBuildRequest(source_file=source_file, updated_by="test-suite"))


def _build_source_file_record(
    *,
    storage_bucket: str | None,
    storage_key: str | None,
) -> SourceFileRecord:
    return SourceFileRecord(
        source_system="cvm",
        dataset_name="informe_diario",
        source_url="https://dados.cvm.gov.br/file.csv",
        file_name="file.csv",
        file_hash="hash123",
        first_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        storage_bucket=storage_bucket,
        storage_key=storage_key,
        competence="2024-01",
        business_date=date(2024, 1, 15),
    )
