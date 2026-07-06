from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from apex_lakehouse.ingestion.cvm.silver_transformer import (
    CvmSilverTransformer,
    SilverTransformRequest,
)


def test_transform_cadastro_normalizes_cnpj_and_deduplicates(tmp_path: Path) -> None:
    input_path = tmp_path / "bronze-cadastro.csv"
    input_path.write_text(
        "CNPJ_FUNDO,DENOM_SOCIAL,CLASSE,SIT,DT_REG,DT_INI_ATIV,CNPJ_ADMIN,ADMIN,CNPJ_GESTOR,GESTOR,ingestion_timestamp\n"
        "12.345.678/0001-90,Fundo XPTO,Renda Fixa,EM FUNCIONAMENTO NORMAL,15/01/2024,20240110,11.111.111/0001-11,Admin 1,22.222.222/0001-22,Gestor 1,2024-01-15T09:00:00+00:00\n"
        "12.345.678/0001-90,Fundo XPTO Atualizado,Renda Fixa,EM FUNCIONAMENTO NORMAL,15/01/2024,20240110,11.111.111/0001-11,Admin 1,22.222.222/0001-22,Gestor 1,2024-01-15T10:00:00+00:00\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "silver.csv"
    schema_path = tmp_path / "schema.json"

    summary = CvmSilverTransformer().transform(
        SilverTransformRequest(
            dataset_name="cadastro_fundos",
            input_path=input_path,
            output_path=output_path,
            schema_path=schema_path,
            source_file_id=uuid4(),
            source_file_name="cad_fi.csv",
            source_url="https://dados.cvm.gov.br/cad_fi.csv",
            processed_at=datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
        )
    )

    with output_path.open("r", encoding="utf-8", newline="") as file_obj:
        rows = list(csv.DictReader(file_obj))

    assert summary.row_count == 1
    assert summary.deduplicated_rows == 1
    assert rows[0]["cnpj_fundo"] == "12345678000190"
    assert rows[0]["nome_fundo"] == "Fundo XPTO Atualizado"
    assert rows[0]["data_registro"] == "2024-01-15"
    assert rows[0]["data_inicio_atividade"] == "2024-01-10"
    assert rows[0]["cnpj_administrador"] == "11111111000111"


def test_transform_informe_enriches_with_cadastro(tmp_path: Path) -> None:
    informe_path = tmp_path / "bronze-informe.csv"
    informe_path.write_text(
        "CNPJ_FUNDO,DT_COMPTC,VL_QUOTA,VL_PATRIM_LIQ,VL_TOTAL,CAPTC_DIA,RESG_DIA,NR_COTST\n"
        "12.345.678/0001-90,15/01/2024,1.2345,1000.00,1200.00,10.00,5.00,20\n",
        encoding="utf-8",
    )
    cadastro_path = tmp_path / "bronze-cadastro.csv"
    cadastro_path.write_text(
        "CNPJ_FUNDO,DENOM_SOCIAL,CLASSE,SIT,CNPJ_ADMIN,ADMIN\n"
        "12345678000190,Fundo XPTO,Renda Fixa,ATIVO,11111111000111,Admin XPTO\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "silver.csv"
    schema_path = tmp_path / "schema.json"

    summary = CvmSilverTransformer().transform(
        SilverTransformRequest(
            dataset_name="informe_diario",
            input_path=informe_path,
            output_path=output_path,
            schema_path=schema_path,
            source_file_id=uuid4(),
            source_file_name="inf_diario.csv",
            source_url="https://dados.cvm.gov.br/inf_diario.csv",
            processed_at=datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            cadastro_input_path=cadastro_path,
        )
    )

    with output_path.open("r", encoding="utf-8", newline="") as file_obj:
        rows = list(csv.DictReader(file_obj))

    assert summary.row_count == 1
    assert rows[0]["cnpj_fundo"] == "12345678000190"
    assert rows[0]["data_competencia"] == "2024-01-15"
    assert rows[0]["valor_cota"] == "1.2345"
    assert rows[0]["nome_fundo"] == "Fundo XPTO"
    assert rows[0]["nome_administrador"] == "Admin XPTO"

    schema_payload = json.loads(schema_path.read_text(encoding="utf-8"))
    assert schema_payload["output_dataset_name"] == "fundos_informe_diario"

