from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
import zipfile

from apex_lakehouse.control_plane.records import SourceFileRecord
from apex_lakehouse.ingestion.cvm.bronze_parser import CvmBronzeParser


def test_parse_csv_normalizes_business_and_technical_columns(tmp_path: Path) -> None:
    source_path = tmp_path / "inf_diario.csv"
    source_path.write_text(
        "CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;QT_COTAS;DENOM_SOCIAL\n"
        "12.345.678/0001-90;15/01/2024;1.234,56;10;Fundo XPTO\n",
        encoding="latin-1",
    )
    output_path = tmp_path / "bronze.csv"
    schema_path = tmp_path / "schema.json"
    source_file = _build_source_file_record(file_name="inf_diario.csv")
    processed_at = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    summary = CvmBronzeParser().parse(
        source_path=source_path,
        output_path=output_path,
        schema_path=schema_path,
        source_file=source_file,
        pipeline_run_id=uuid4(),
        processed_at=processed_at,
    )

    with output_path.open("r", encoding="utf-8", newline="") as file_obj:
        rows = list(csv.DictReader(file_obj))

    assert summary.row_count == 1
    assert rows[0]["CNPJ_FUNDO"] == "12.345.678/0001-90"
    assert rows[0]["DT_COMPTC"] == "2024-01-15"
    assert rows[0]["VL_TOTAL"] == "1234.56"
    assert rows[0]["QT_COTAS"] == "10"
    assert rows[0]["source_system"] == "cvm"
    assert rows[0]["file_hash"] == "hash123"
    assert rows[0]["schema_version"] == "v1"
    assert rows[0]["ingestion_timestamp"] == processed_at.isoformat()

    schema_payload = json.loads(schema_path.read_text(encoding="utf-8"))
    column_types = {column["name"]: column["data_type"] for column in schema_payload["columns"]}
    assert column_types["CNPJ_FUNDO"] == "string"
    assert column_types["DT_COMPTC"] == "date"
    assert column_types["VL_TOTAL"] == "decimal"
    assert column_types["QT_COTAS"] == "integer"
    assert column_types["source_file_id"] == "string"
    assert column_types["ingestion_timestamp"] == "timestamp"


def test_parse_zip_reads_first_csv_member(tmp_path: Path) -> None:
    source_path = tmp_path / "cadastro.zip"
    with zipfile.ZipFile(source_path, "w") as archive:
        archive.writestr(
            "cad_fi.csv",
            "CNPJ_FUNDO;DT_REG;DENOM_SOCIAL\n12.345.678/0001-90;20240115;Fundo XPTO\n",
        )

    output_path = tmp_path / "bronze.csv"
    schema_path = tmp_path / "schema.json"
    source_file = _build_source_file_record(file_name="cadastro.zip", dataset_name="cadastro_fundos")

    summary = CvmBronzeParser().parse(
        source_path=source_path,
        output_path=output_path,
        schema_path=schema_path,
        source_file=source_file,
        pipeline_run_id=None,
        processed_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
    )

    assert summary.source_format == "zip_csv"
    with output_path.open("r", encoding="utf-8", newline="") as file_obj:
        rows = list(csv.DictReader(file_obj))
    assert rows[0]["DT_REG"] == "2024-01-15"
    assert rows[0]["pipeline_run_id"] == ""


def _build_source_file_record(
    *,
    file_name: str,
    dataset_name: str = "informe_diario",
) -> SourceFileRecord:
    return SourceFileRecord(
        source_system="cvm",
        dataset_name=dataset_name,
        source_url=f"https://dados.cvm.gov.br/{file_name}",
        file_name=file_name,
        file_hash="hash123",
        first_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
    )
