"""Pure transformation logic for CVM bronze-to-silver datasets."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Sequence
from uuid import UUID

from apex_lakehouse.exceptions import DatasetNotFoundError
from apex_lakehouse.ingestion.cvm.silver_models import (
    DEFAULT_SILVER_SCHEMA_VERSION,
    SilverColumnSchema,
    SilverColumnType,
    SilverTransformSummary,
)

TECHNICAL_COLUMNS: dict[str, SilverColumnType] = {
    "source_system": "string",
    "source_file_id": "string",
    "source_file": "string",
    "source_url": "string",
    "silver_generated_at": "timestamp",
    "schema_version": "string",
    "pipeline_run_id": "string",
}


@dataclass(frozen=True)
class SilverTransformRequest:
    dataset_name: str
    input_path: Path
    output_path: Path
    schema_path: Path
    source_file_id: UUID
    source_file_name: str
    source_url: str
    processed_at: datetime
    pipeline_run_id: UUID | None = None
    cadastro_input_path: Path | None = None
    schema_version: str = DEFAULT_SILVER_SCHEMA_VERSION


@dataclass
class _TrackedColumn:
    data_type: SilverColumnType
    nullable: bool
    technical: bool


class CvmSilverTransformer:
    """Transform bronze CSV datasets into conformed silver outputs."""

    def transform(self, request: SilverTransformRequest) -> SilverTransformSummary:
        rows = _read_csv_rows(request.input_path)
        if request.dataset_name == "cadastro_fundos":
            output_dataset_name = "fundos"
            transformed_rows = _deduplicate_rows(
                [self._transform_cadastro_row(row, request) for row in rows],
                key_fields=("cnpj_fundo",),
                ordering_fields=("data_registro", "silver_generated_at"),
            )
        elif request.dataset_name == "informe_diario":
            output_dataset_name = "fundos_informe_diario"
            cadastro_index = self._build_cadastro_index(request.cadastro_input_path)
            transformed_rows = _deduplicate_rows(
                [
                    self._transform_informe_row(row, request, cadastro_index)
                    for row in rows
                ],
                key_fields=("cnpj_fundo", "data_competencia"),
                ordering_fields=("data_competencia", "silver_generated_at"),
            )
        elif request.dataset_name == "perfil_mensal":
            output_dataset_name = "fundos_perfil_mensal"
            cadastro_index = self._build_cadastro_index(request.cadastro_input_path)
            transformed_rows = _deduplicate_rows(
                [
                    self._transform_perfil_row(row, request, cadastro_index)
                    for row in rows
                ],
                key_fields=("cnpj_fundo", "competencia"),
                ordering_fields=("competencia", "silver_generated_at"),
            )
        else:
            raise DatasetNotFoundError(f"Unsupported CVM silver dataset: {request.dataset_name}")

        request.output_path.parent.mkdir(parents=True, exist_ok=True)
        request.schema_path.parent.mkdir(parents=True, exist_ok=True)

        tracked_columns: dict[str, _TrackedColumn] = {}
        fieldnames = self._resolve_fieldnames(output_dataset_name)
        with request.output_path.open("w", encoding="utf-8", newline="") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            writer.writeheader()

            for row in transformed_rows:
                for fieldname in fieldnames:
                    _track_column(
                        tracked_columns,
                        column_name=fieldname,
                        data_type=_infer_type(fieldname),
                        nullable=row.get(fieldname) in {None, ""},
                        technical=fieldname in TECHNICAL_COLUMNS,
                    )
                writer.writerow(
                    {
                        fieldname: "" if row.get(fieldname) is None else str(row[fieldname])
                        for fieldname in fieldnames
                    }
                )

        columns = tuple(
            SilverColumnSchema(
                name=fieldname,
                data_type=tracked_columns.get(
                    fieldname,
                    _TrackedColumn(
                        data_type=_infer_type(fieldname),
                        nullable=True,
                        technical=fieldname in TECHNICAL_COLUMNS,
                    ),
                ).data_type,
                nullable=tracked_columns.get(
                    fieldname,
                    _TrackedColumn(
                        data_type=_infer_type(fieldname),
                        nullable=True,
                        technical=fieldname in TECHNICAL_COLUMNS,
                    ),
                ).nullable,
                technical=fieldname in TECHNICAL_COLUMNS,
            )
            for fieldname in fieldnames
        )

        request.schema_path.write_text(
            json.dumps(
                {
                    "schema_version": request.schema_version,
                    "input_dataset_name": request.dataset_name,
                    "output_dataset_name": output_dataset_name,
                    "row_count": len(transformed_rows),
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

        return SilverTransformSummary(
            output_path=request.output_path.resolve(),
            schema_path=request.schema_path.resolve(),
            row_count=len(transformed_rows),
            deduplicated_rows=len(rows) - len(transformed_rows),
            columns=columns,
            schema_version=request.schema_version,
            input_dataset_name=request.dataset_name,
        )

    def _build_cadastro_index(
        self,
        cadastro_input_path: Path | None,
    ) -> dict[str, dict[str, str | None]]:
        if cadastro_input_path is None or not cadastro_input_path.exists():
            return {}

        index: dict[str, dict[str, str | None]] = {}
        for row in _read_csv_rows(cadastro_input_path):
            cnpj = _normalize_cnpj(_pick(row, "cnpj_fundo", "CNPJ_FUNDO"))
            if cnpj is None:
                continue
            index[cnpj] = {
                "nome_fundo": _pick(row, "nome_fundo", "DENOM_SOCIAL", "denom_social"),
                "classe_fundo": _pick(row, "classe_fundo", "CLASSE", "classe"),
                "situacao": _pick(row, "situacao", "SIT", "situacao"),
                "cnpj_administrador": _normalize_cnpj(
                    _pick(row, "cnpj_administrador", "CNPJ_ADMIN", "cnpj_admin")
                ),
                "nome_administrador": _pick(
                    row,
                    "nome_administrador",
                    "ADMIN",
                    "ADMINISTRADOR",
                    "administrador",
                ),
                "cnpj_gestor": _normalize_cnpj(
                    _pick(row, "cnpj_gestor", "CNPJ_GESTOR", "cnpj_gestor")
                ),
                "nome_gestor": _pick(
                    row,
                    "nome_gestor",
                    "GESTOR",
                    "gestor",
                ),
            }
        return index

    def _transform_cadastro_row(
        self,
        row: dict[str, str],
        request: SilverTransformRequest,
    ) -> dict[str, str | None]:
        return {
            "cnpj_fundo": _normalize_cnpj(_pick(row, "CNPJ_FUNDO", "cnpj_fundo")),
            "nome_fundo": _pick(row, "DENOM_SOCIAL", "denom_social", "NOME_FUNDO"),
            "classe_fundo": _pick(row, "CLASSE", "classe", "TP_FUNDO"),
            "situacao": _pick(row, "SIT", "situacao"),
            "data_registro": _normalize_date(
                _pick(row, "DT_REG", "data_registro", "DT_CONST")
            ),
            "data_inicio_atividade": _normalize_date(
                _pick(row, "DT_INI_ATIV", "DT_INIEXERC", "data_inicio_atividade")
            ),
            "cnpj_administrador": _normalize_cnpj(
                _pick(row, "CNPJ_ADMIN", "cnpj_admin", "cnpj_administrador")
            ),
            "nome_administrador": _pick(
                row,
                "ADMIN",
                "ADMINISTRADOR",
                "administrador",
            ),
            "cnpj_gestor": _normalize_cnpj(
                _pick(row, "CNPJ_GESTOR", "cnpj_gestor")
            ),
            "nome_gestor": _pick(row, "GESTOR", "gestor"),
            **_technical_values(request),
        }

    def _transform_informe_row(
        self,
        row: dict[str, str],
        request: SilverTransformRequest,
        cadastro_index: dict[str, dict[str, str | None]],
    ) -> dict[str, str | None]:
        cnpj_fundo = _normalize_cnpj(_pick(row, "CNPJ_FUNDO", "cnpj_fundo"))
        cadastro = cadastro_index.get(cnpj_fundo or "", {})
        return {
            "cnpj_fundo": cnpj_fundo,
            "data_competencia": _normalize_date(
                _pick(row, "DT_COMPTC", "data_competencia", "DT_COMPT")
            ),
            "valor_cota": _normalize_decimal(_pick(row, "VL_QUOTA", "valor_cota")),
            "patrimonio_liquido": _normalize_decimal(
                _pick(row, "VL_PATRIM_LIQ", "patrimonio_liquido")
            ),
            "valor_carteira": _normalize_decimal(
                _pick(row, "VL_TOTAL", "valor_carteira")
            ),
            "captacao_dia": _normalize_decimal(_pick(row, "CAPTC_DIA", "captacao_dia")),
            "resgate_dia": _normalize_decimal(_pick(row, "RESG_DIA", "resgate_dia")),
            "numero_cotistas": _normalize_integer(
                _pick(row, "NR_COTST", "numero_cotistas")
            ),
            "nome_fundo": cadastro.get("nome_fundo"),
            "classe_fundo": cadastro.get("classe_fundo"),
            "situacao": cadastro.get("situacao"),
            "cnpj_administrador": cadastro.get("cnpj_administrador"),
            "nome_administrador": cadastro.get("nome_administrador"),
            **_technical_values(request),
        }

    def _transform_perfil_row(
        self,
        row: dict[str, str],
        request: SilverTransformRequest,
        cadastro_index: dict[str, dict[str, str | None]],
    ) -> dict[str, str | None]:
        cnpj_fundo = _normalize_cnpj(_pick(row, "CNPJ_FUNDO", "cnpj_fundo"))
        cadastro = cadastro_index.get(cnpj_fundo or "", {})
        competencia = _normalize_competence(
            _pick(row, "competencia", "COMPETENCIA", "DT_COMPTC", "dt_comptc")
        )
        return {
            "cnpj_fundo": cnpj_fundo,
            "competencia": competencia,
            "nome_fundo": cadastro.get("nome_fundo"),
            "classe_fundo": cadastro.get("classe_fundo"),
            "situacao": cadastro.get("situacao"),
            "patrimonio_liquido": _normalize_decimal(
                _pick(row, "VL_PATRIM_LIQ", "patrimonio_liquido")
            ),
            "cotistas": _normalize_integer(_pick(row, "NR_COTST", "cotistas")),
            **_technical_values(request),
        }

    def _resolve_fieldnames(self, output_dataset_name: str) -> list[str]:
        if output_dataset_name == "fundos":
            return [
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
                *TECHNICAL_COLUMNS.keys(),
            ]
        if output_dataset_name == "fundos_informe_diario":
            return [
                "cnpj_fundo",
                "data_competencia",
                "valor_cota",
                "patrimonio_liquido",
                "valor_carteira",
                "captacao_dia",
                "resgate_dia",
                "numero_cotistas",
                "nome_fundo",
                "classe_fundo",
                "situacao",
                "cnpj_administrador",
                "nome_administrador",
                *TECHNICAL_COLUMNS.keys(),
            ]
        if output_dataset_name == "fundos_perfil_mensal":
            return [
                "cnpj_fundo",
                "competencia",
                "nome_fundo",
                "classe_fundo",
                "situacao",
                "patrimonio_liquido",
                "cotistas",
                *TECHNICAL_COLUMNS.keys(),
            ]
        raise DatasetNotFoundError(f"Unsupported silver output dataset: {output_dataset_name}")


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        return list(csv.DictReader(file_obj))


def _pick(row: dict[str, str], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        stripped = value.strip()
        if stripped != "":
            return stripped
    return None


def _normalize_cnpj(value: str | None) -> str | None:
    if value is None:
        return None
    digits = "".join(character for character in value if character.isdigit())
    if len(digits) == 0:
        return None
    return digits.zfill(14)


def _normalize_date(value: str | None) -> str | None:
    if value is None:
        return None
    for pattern in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value, pattern).date().isoformat()
        except ValueError:
            continue
    return None


def _normalize_competence(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if len(normalized) == 7 and normalized[4] == "-":
        return normalized
    if len(normalized) == 8 and normalized.isdigit():
        return f"{normalized[:4]}-{normalized[4:6]}"
    if len(normalized) == 10:
        date_value = _normalize_date(normalized)
        if date_value is None:
            return None
        return date_value[:7]
    return None


def _normalize_decimal(value: str | None) -> str | None:
    if value is None:
        return None
    candidate = value.replace(" ", "")
    if "," in candidate and "." in candidate:
        candidate = candidate.replace(".", "").replace(",", ".")
    elif "," in candidate:
        candidate = candidate.replace(",", ".")
    try:
        normalized = format(Decimal(candidate), "f")
    except (InvalidOperation, ValueError):
        return None
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".") or "0"
    return normalized


def _normalize_integer(value: str | None) -> str | None:
    decimal_value = _normalize_decimal(value)
    if decimal_value is None:
        return None
    try:
        return str(int(Decimal(decimal_value)))
    except (InvalidOperation, ValueError):
        return None


def _technical_values(request: SilverTransformRequest) -> dict[str, str]:
    return {
        "source_system": "cvm",
        "source_file_id": str(request.source_file_id),
        "source_file": request.source_file_name,
        "source_url": request.source_url,
        "silver_generated_at": request.processed_at.isoformat(),
        "schema_version": request.schema_version,
        "pipeline_run_id": "" if request.pipeline_run_id is None else str(request.pipeline_run_id),
    }


def _deduplicate_rows(
    rows: Sequence[dict[str, str | None]],
    *,
    key_fields: tuple[str, ...],
    ordering_fields: tuple[str, ...],
) -> list[dict[str, str | None]]:
    indexed: dict[tuple[str | None, ...], dict[str, str | None]] = {}
    for row in rows:
        key = tuple(row.get(field) for field in key_fields)
        current = indexed.get(key)
        if current is None or _ordering_value(row, ordering_fields) >= _ordering_value(
            current,
            ordering_fields,
        ):
            indexed[key] = row
    return list(indexed.values())


def _ordering_value(
    row: dict[str, str | None],
    ordering_fields: Iterable[str],
) -> tuple[str, ...]:
    return tuple("" if row.get(field) is None else str(row[field]) for field in ordering_fields)


def _track_column(
    tracked_columns: dict[str, _TrackedColumn],
    *,
    column_name: str,
    data_type: SilverColumnType,
    nullable: bool,
    technical: bool,
) -> None:
    existing = tracked_columns.get(column_name)
    if existing is None:
        tracked_columns[column_name] = _TrackedColumn(
            data_type=data_type,
            nullable=nullable,
            technical=technical,
        )
        return
    existing.nullable = existing.nullable or nullable


def _infer_type(fieldname: str) -> SilverColumnType:
    if fieldname in TECHNICAL_COLUMNS:
        return TECHNICAL_COLUMNS[fieldname]
    if fieldname in {"data_registro", "data_inicio_atividade", "data_competencia"}:
        return "date"
    if fieldname in {"competencia"}:
        return "string"
    if fieldname in {"numero_cotistas", "cotistas"}:
        return "integer"
    if fieldname in {
        "valor_cota",
        "patrimonio_liquido",
        "valor_carteira",
        "captacao_dia",
        "resgate_dia",
    }:
        return "decimal"
    return "string"
