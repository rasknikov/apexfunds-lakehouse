"""Analytical gold transformations for CVM funds datasets."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable
from uuid import UUID

from apex_lakehouse.ingestion.cvm.gold_models import (
    DEFAULT_GOLD_SCHEMA_VERSION,
    GoldColumnSchema,
    GoldColumnType,
    GoldDatasetSummary,
)

TECHNICAL_COLUMN_TYPES: dict[str, GoldColumnType] = {
    "pipeline_run_id": "string",
    "gold_generated_at": "timestamp",
    "schema_version": "string",
}


@dataclass(frozen=True)
class GoldMartTransformRequest:
    funds_input_path: Path
    informe_input_path: Path
    output_directory: Path
    generated_at: datetime
    pipeline_run_id: UUID | None = None
    schema_version: str = DEFAULT_GOLD_SCHEMA_VERSION
    partition_key: str | None = None


class CvmGoldTransformer:
    """Build dimensional and fact gold datasets from silver CVM outputs."""

    def transform(self, request: GoldMartTransformRequest) -> list[GoldDatasetSummary]:
        funds_rows = _read_csv_rows(request.funds_input_path)
        informe_rows = _read_csv_rows(request.informe_input_path)
        request.output_directory.mkdir(parents=True, exist_ok=True)

        dim_fundo_rows = self._build_dim_fundo_rows(
            funds_rows,
            generated_at=request.generated_at,
            pipeline_run_id=request.pipeline_run_id,
            schema_version=request.schema_version,
        )
        dim_tempo_rows = self._build_dim_tempo_rows(
            informe_rows,
            generated_at=request.generated_at,
            pipeline_run_id=request.pipeline_run_id,
            schema_version=request.schema_version,
            partition_key=request.partition_key,
        )
        fato_rows = self._build_fato_fundo_diario_rows(
            informe_rows,
            generated_at=request.generated_at,
            pipeline_run_id=request.pipeline_run_id,
            schema_version=request.schema_version,
            partition_key=request.partition_key,
        )

        return [
            _write_dataset(
                dataset_name="dim_fundo",
                rows=dim_fundo_rows,
                fieldnames=[
                    "cnpj_fundo",
                    "nome_fundo",
                    "classe_fundo",
                    "situacao",
                    "data_registro",
                    "data_inicio_atividade",
                    "cnpj_administrador",
                    "nome_administrador",
                    "cnpj_gestor",
                    "nome_gestor",
                    "pipeline_run_id",
                    "gold_generated_at",
                    "schema_version",
                ],
                output_directory=request.output_directory,
                schema_version=request.schema_version,
                partition_key=None,
            ),
            _write_dataset(
                dataset_name="dim_tempo",
                rows=dim_tempo_rows,
                fieldnames=[
                    "data_referencia",
                    "ano",
                    "mes",
                    "dia",
                    "trimestre",
                    "competencia",
                    "dia_semana",
                    "pipeline_run_id",
                    "gold_generated_at",
                    "schema_version",
                ],
                output_directory=request.output_directory,
                schema_version=request.schema_version,
                partition_key=request.partition_key,
            ),
            _write_dataset(
                dataset_name="fato_fundo_diario",
                rows=fato_rows,
                fieldnames=[
                    "cnpj_fundo",
                    "data_referencia",
                    "nome_fundo",
                    "classe_fundo",
                    "valor_cota",
                    "patrimonio_liquido",
                    "valor_carteira",
                    "captacao_dia",
                    "resgate_dia",
                    "captacao_liquida",
                    "numero_cotistas",
                    "rentabilidade_diaria",
                    "variacao_patrimonio_liquido",
                    "variacao_numero_cotistas",
                    "pipeline_run_id",
                    "gold_generated_at",
                    "schema_version",
                ],
                output_directory=request.output_directory,
                schema_version=request.schema_version,
                partition_key=request.partition_key,
            ),
        ]

    def _build_dim_fundo_rows(
        self,
        funds_rows: list[dict[str, str]],
        *,
        generated_at: datetime,
        pipeline_run_id: UUID | None,
        schema_version: str,
    ) -> list[dict[str, str]]:
        unique_rows: dict[str, dict[str, str]] = {}
        for row in funds_rows:
            cnpj_fundo = row.get("cnpj_fundo", "").strip()
            if cnpj_fundo == "":
                continue
            unique_rows[cnpj_fundo] = {
                "cnpj_fundo": cnpj_fundo,
                "nome_fundo": row.get("nome_fundo", ""),
                "classe_fundo": row.get("classe_fundo", ""),
                "situacao": row.get("situacao", ""),
                "data_registro": row.get("data_registro", ""),
                "data_inicio_atividade": row.get("data_inicio_atividade", ""),
                "cnpj_administrador": row.get("cnpj_administrador", ""),
                "nome_administrador": row.get("nome_administrador", ""),
                "cnpj_gestor": row.get("cnpj_gestor", ""),
                "nome_gestor": row.get("nome_gestor", ""),
                **_technical_values(
                    generated_at=generated_at,
                    pipeline_run_id=pipeline_run_id,
                    schema_version=schema_version,
                ),
            }
        return list(unique_rows.values())

    def _build_dim_tempo_rows(
        self,
        informe_rows: list[dict[str, str]],
        *,
        generated_at: datetime,
        pipeline_run_id: UUID | None,
        schema_version: str,
        partition_key: str | None,
    ) -> list[dict[str, str]]:
        unique_dates: dict[str, dict[str, str]] = {}
        for row in informe_rows:
            data_referencia = row.get("data_competencia", "").strip()
            if data_referencia == "":
                continue
            parsed = date.fromisoformat(data_referencia)
            unique_dates[data_referencia] = {
                "data_referencia": data_referencia,
                "ano": str(parsed.year),
                "mes": str(parsed.month),
                "dia": str(parsed.day),
                "trimestre": str(((parsed.month - 1) // 3) + 1),
                "competencia": data_referencia[:7],
                "dia_semana": str(parsed.isoweekday()),
                **_technical_values(
                    generated_at=generated_at,
                    pipeline_run_id=pipeline_run_id,
                    schema_version=schema_version,
                ),
            }
        return list(unique_dates.values())

    def _build_fato_fundo_diario_rows(
        self,
        informe_rows: list[dict[str, str]],
        *,
        generated_at: datetime,
        pipeline_run_id: UUID | None,
        schema_version: str,
        partition_key: str | None,
    ) -> list[dict[str, str]]:
        sorted_rows = sorted(
            informe_rows,
            key=lambda row: (
                row.get("cnpj_fundo", ""),
                row.get("data_competencia", ""),
            ),
        )
        previous_by_fund: dict[str, dict[str, str]] = {}
        fact_rows: list[dict[str, str]] = []

        for row in sorted_rows:
            cnpj_fundo = row.get("cnpj_fundo", "").strip()
            if cnpj_fundo == "":
                continue

            previous_row = previous_by_fund.get(cnpj_fundo)
            valor_cota = _to_decimal(row.get("valor_cota"))
            patrimonio_liquido = _to_decimal(row.get("patrimonio_liquido"))
            captacao_dia = _to_decimal(row.get("captacao_dia"))
            resgate_dia = _to_decimal(row.get("resgate_dia"))
            numero_cotistas = _to_decimal(row.get("numero_cotistas"))

            previous_valor_cota = _to_decimal(previous_row.get("valor_cota")) if previous_row else None
            previous_patrimonio = _to_decimal(previous_row.get("patrimonio_liquido")) if previous_row else None
            previous_cotistas = _to_decimal(previous_row.get("numero_cotistas")) if previous_row else None

            fact_rows.append(
                {
                    "cnpj_fundo": cnpj_fundo,
                    "data_referencia": row.get("data_competencia", ""),
                    "nome_fundo": row.get("nome_fundo", ""),
                    "classe_fundo": row.get("classe_fundo", ""),
                    "valor_cota": _serialize_decimal(valor_cota),
                    "patrimonio_liquido": _serialize_decimal(patrimonio_liquido),
                    "valor_carteira": row.get("valor_carteira", ""),
                    "captacao_dia": _serialize_decimal(captacao_dia),
                    "resgate_dia": _serialize_decimal(resgate_dia),
                    "captacao_liquida": _serialize_decimal(
                        _subtract(captacao_dia, resgate_dia)
                    ),
                    "numero_cotistas": _serialize_decimal(numero_cotistas),
                    "rentabilidade_diaria": _serialize_decimal(
                        _daily_return(valor_cota, previous_valor_cota)
                    ),
                    "variacao_patrimonio_liquido": _serialize_decimal(
                        _subtract(patrimonio_liquido, previous_patrimonio)
                    ),
                    "variacao_numero_cotistas": _serialize_decimal(
                        _subtract(numero_cotistas, previous_cotistas)
                    ),
                    **_technical_values(
                        generated_at=generated_at,
                        pipeline_run_id=pipeline_run_id,
                        schema_version=schema_version,
                    ),
                }
            )
            previous_by_fund[cnpj_fundo] = row

        return fact_rows


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        return list(csv.DictReader(file_obj))


def _write_dataset(
    *,
    dataset_name: str,
    rows: list[dict[str, str]],
    fieldnames: list[str],
    output_directory: Path,
    schema_version: str,
    partition_key: str | None,
) -> GoldDatasetSummary:
    output_path = output_directory / f"{dataset_name}.csv"
    schema_path = output_directory / f"{dataset_name}.schema.json"
    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({fieldname: row.get(fieldname, "") for fieldname in fieldnames})

    columns = tuple(
        GoldColumnSchema(
            name=fieldname,
            data_type=_infer_type(fieldname),
            nullable=True,
            technical=fieldname in TECHNICAL_COLUMN_TYPES,
        )
        for fieldname in fieldnames
    )
    schema_path.write_text(
        json.dumps(
            {
                "dataset_name": dataset_name,
                "schema_version": schema_version,
                "partition_key": partition_key,
                "row_count": len(rows),
                "columns": [
                    {
                        "name": column.name,
                        "data_type": column.data_type,
                        "nullable": column.nullable,
                        "technical": column.technical,
                    }
                    for column in columns
                ],
            },
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )
    return GoldDatasetSummary(
        dataset_name=dataset_name,
        output_path=output_path.resolve(),
        schema_path=schema_path.resolve(),
        row_count=len(rows),
        columns=columns,
        partition_key=partition_key,
        schema_version=schema_version,
    )


def _technical_values(
    *,
    generated_at: datetime,
    pipeline_run_id: UUID | None,
    schema_version: str,
) -> dict[str, str]:
    return {
        "pipeline_run_id": "" if pipeline_run_id is None else str(pipeline_run_id),
        "gold_generated_at": generated_at.isoformat(),
        "schema_version": schema_version,
    }


def _infer_type(fieldname: str) -> GoldColumnType:
    if fieldname in TECHNICAL_COLUMN_TYPES:
        return TECHNICAL_COLUMN_TYPES[fieldname]
    if fieldname in {"data_referencia", "data_registro", "data_inicio_atividade"}:
        return "date"
    if fieldname in {"ano", "mes", "dia", "trimestre", "dia_semana"}:
        return "integer"
    if fieldname in {
        "valor_cota",
        "patrimonio_liquido",
        "valor_carteira",
        "captacao_dia",
        "resgate_dia",
        "captacao_liquida",
        "rentabilidade_diaria",
        "variacao_patrimonio_liquido",
        "variacao_numero_cotistas",
        "numero_cotistas",
    }:
        return "decimal"
    return "string"


def _to_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    candidate = value.strip()
    if candidate == "":
        return None
    try:
        return Decimal(candidate)
    except (InvalidOperation, ValueError):
        return None


def _serialize_decimal(value: Decimal | None) -> str:
    if value is None:
        return ""
    normalized = format(value, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".") or "0"
    return normalized


def _subtract(left: Decimal | None, right: Decimal | None) -> Decimal | None:
    if left is None or right is None:
        return None
    return left - right


def _daily_return(current: Decimal | None, previous: Decimal | None) -> Decimal | None:
    if current is None or previous is None or previous == 0:
        return None
    return (current / previous) - Decimal("1")
