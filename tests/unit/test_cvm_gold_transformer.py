from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from apex_lakehouse.ingestion.cvm.gold_transformer import (
    CvmGoldTransformer,
    GoldMartTransformRequest,
)


def test_transform_builds_dim_fundo_dim_tempo_and_fact_metrics(tmp_path: Path) -> None:
    funds_path = tmp_path / "fundos.csv"
    funds_path.write_text(
        "cnpj_fundo,nome_fundo,classe_fundo,situacao,data_registro,data_inicio_atividade,cnpj_administrador,nome_administrador,cnpj_gestor,nome_gestor\n"
        "12345678000190,Fundo XPTO,Renda Fixa,ATIVO,2024-01-01,2024-01-02,11111111000111,Admin XPTO,22222222000122,Gestor XPTO\n",
        encoding="utf-8",
    )
    informe_path = tmp_path / "informe.csv"
    informe_path.write_text(
        "cnpj_fundo,data_competencia,nome_fundo,classe_fundo,valor_cota,patrimonio_liquido,valor_carteira,captacao_dia,resgate_dia,numero_cotistas\n"
        "12345678000190,2024-01-15,Fundo XPTO,Renda Fixa,1.00,100,120,10,3,20\n"
        "12345678000190,2024-01-16,Fundo XPTO,Renda Fixa,1.10,120,140,8,2,22\n",
        encoding="utf-8",
    )
    output_directory = tmp_path / "gold"

    summaries = CvmGoldTransformer().transform(
        GoldMartTransformRequest(
            funds_input_path=funds_path,
            informe_input_path=informe_path,
            output_directory=output_directory,
            generated_at=datetime(2024, 1, 16, 10, 0, 0, tzinfo=timezone.utc),
            pipeline_run_id=uuid4(),
            partition_key="ano=2024/mes=01",
        )
    )

    summary_by_name = {summary.dataset_name: summary for summary in summaries}
    assert summary_by_name["dim_fundo"].row_count == 1
    assert summary_by_name["dim_tempo"].row_count == 2
    assert summary_by_name["fato_fundo_diario"].row_count == 2

    with summary_by_name["fato_fundo_diario"].output_path.open("r", encoding="utf-8", newline="") as file_obj:
        fact_rows = list(csv.DictReader(file_obj))

    assert fact_rows[0]["captacao_liquida"] == "7"
    assert fact_rows[0]["rentabilidade_diaria"] == ""
    assert fact_rows[1]["captacao_liquida"] == "6"
    assert fact_rows[1]["rentabilidade_diaria"] == "0.1"
    assert fact_rows[1]["variacao_patrimonio_liquido"] == "20"
    assert fact_rows[1]["variacao_numero_cotistas"] == "2"

    schema_payload = json.loads(summary_by_name["dim_tempo"].schema_path.read_text(encoding="utf-8"))
    assert schema_payload["dataset_name"] == "dim_tempo"
    assert schema_payload["partition_key"] == "ano=2024/mes=01"
