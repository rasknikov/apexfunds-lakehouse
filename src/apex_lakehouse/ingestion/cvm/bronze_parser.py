"""Streaming bronze parser for raw CVM CSV and ZIP artifacts."""

from __future__ import annotations

import csv
import json
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import TextIOWrapper
from pathlib import Path
from typing import Generator, Iterable
from uuid import UUID
import zipfile

from apex_lakehouse.control_plane.records import SourceFileRecord
from apex_lakehouse.ingestion.cvm.bronze_models import (
    DEFAULT_BRONZE_SCHEMA_VERSION,
    BronzeColumnSchema,
    BronzeColumnType,
    BronzeParseSummary,
)

TECHNICAL_COLUMN_TYPES: dict[str, BronzeColumnType] = {
    "source_system": "string",
    "source_file": "string",
    "source_url": "string",
    "source_file_id": "string",
    "ingestion_timestamp": "timestamp",
    "file_hash": "string",
    "schema_version": "string",
    "pipeline_run_id": "string",
}

DATE_COLUMN_PATTERN = re.compile(r"(^dt_|_dt_|_data$|^data_)", re.IGNORECASE)
STRING_ONLY_COLUMN_PATTERN = re.compile(
    r"(cnpj|cpf|denom|nome|social|fundo|classe|tipo|tp_|cnpj_|id_|^id$|cvm|isin)",
    re.IGNORECASE,
)
INTEGER_HINT_PATTERN = re.compile(r"(^qt_|^nr_)", re.IGNORECASE)
DECIMAL_HINT_PATTERN = re.compile(r"(^vl_|_vl_|valor|captc|resg)", re.IGNORECASE)


@dataclass
class _TrackedColumn:
    data_type: BronzeColumnType
    nullable: bool
    technical: bool = False


@dataclass(frozen=True)
class _RawCsvSource:
    source_path: Path
    member_name: str | None = None
    source_format: str = "csv"

    def read_sample_bytes(self, size: int = 8192) -> bytes:
        with self.open_binary() as stream:
            return stream.read(size)

    @contextmanager
    def open_binary(self) -> Generator[object, None, None]:
        if self.member_name is None:
            with self.source_path.open("rb") as stream:
                yield stream
            return

        with zipfile.ZipFile(self.source_path) as archive:
            with archive.open(self.member_name, "r") as member_stream:
                yield member_stream

    @contextmanager
    def open_text(self, encoding: str) -> Generator[TextIOWrapper, None, None]:
        with self.open_binary() as stream:
            text_stream = TextIOWrapper(stream, encoding=encoding, newline="")
            try:
                yield text_stream
            finally:
                text_stream.detach()


class CvmBronzeParser:
    """Normalize raw CVM files into bronze-ready CSV plus schema metadata."""

    def parse(
        self,
        *,
        source_path: Path,
        output_path: Path,
        schema_path: Path,
        source_file: SourceFileRecord,
        pipeline_run_id: UUID | None,
        processed_at: datetime,
        schema_version: str = DEFAULT_BRONZE_SCHEMA_VERSION,
    ) -> BronzeParseSummary:
        raw_source = _resolve_raw_csv_source(source_path)
        sample_bytes = raw_source.read_sample_bytes()
        encoding = _detect_encoding(sample_bytes)
        delimiter = _detect_delimiter(sample_bytes, encoding=encoding)
        tracked_columns: dict[str, _TrackedColumn] = {}

        output_path.parent.mkdir(parents=True, exist_ok=True)
        schema_path.parent.mkdir(parents=True, exist_ok=True)

        with raw_source.open_text(encoding) as text_stream:
            reader = csv.DictReader(text_stream, delimiter=delimiter)
            if reader.fieldnames is None:
                raise ValueError(f"No header detected in source file: {source_path}")

            business_columns = [column.strip() for column in reader.fieldnames]
            technical_columns = list(TECHNICAL_COLUMN_TYPES.keys())
            output_columns = business_columns + technical_columns

            with output_path.open("w", encoding="utf-8", newline="") as output_file:
                writer = csv.DictWriter(output_file, fieldnames=output_columns)
                writer.writeheader()

                row_count = 0
                for raw_row in reader:
                    normalized_business_row = _normalize_business_row(
                        raw_row,
                        tracked_columns=tracked_columns,
                    )
                    normalized_business_row.update(
                        _build_technical_columns(
                            source_file=source_file,
                            pipeline_run_id=pipeline_run_id,
                            processed_at=processed_at,
                            schema_version=schema_version,
                        )
                    )
                    writer.writerow(_serialize_row(normalized_business_row, output_columns))
                    row_count += 1

        _merge_technical_columns(tracked_columns)
        columns = tuple(
            BronzeColumnSchema(
                name=column_name,
                data_type=tracked_columns[column_name].data_type,
                nullable=tracked_columns[column_name].nullable,
                technical=tracked_columns[column_name].technical,
            )
            for column_name in output_columns
        )
        schema_path.write_text(
            json.dumps(
                {
                    "schema_version": schema_version,
                    "source_format": raw_source.source_format,
                    "row_count": row_count,
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

        return BronzeParseSummary(
            output_path=output_path.resolve(),
            schema_path=schema_path.resolve(),
            row_count=row_count,
            columns=columns,
            schema_version=schema_version,
            delimiter=delimiter,
            source_format=raw_source.source_format,
        )


def _resolve_raw_csv_source(source_path: Path) -> _RawCsvSource:
    suffix = source_path.suffix.lower()
    if suffix == ".csv":
        return _RawCsvSource(source_path=source_path.resolve(), source_format="csv")

    if suffix == ".zip":
        with zipfile.ZipFile(source_path) as archive:
            csv_members = [
                name for name in archive.namelist()
                if not name.endswith("/") and name.lower().endswith(".csv")
            ]
            if not csv_members:
                raise ValueError(f"ZIP source does not contain any CSV member: {source_path}")
            return _RawCsvSource(
                source_path=source_path.resolve(),
                member_name=csv_members[0],
                source_format="zip_csv",
            )

    raise ValueError(f"Unsupported raw source format: {source_path}")


def _detect_encoding(sample_bytes: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            sample_bytes.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _detect_delimiter(sample_bytes: bytes, *, encoding: str) -> str:
    sample_text = sample_bytes.decode(encoding, errors="replace")
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=";,\t")
        return dialect.delimiter
    except csv.Error:
        if sample_text.count(";") >= sample_text.count(","):
            return ";"
        return ","


def _normalize_business_row(
    raw_row: dict[str, str | None],
    *,
    tracked_columns: dict[str, _TrackedColumn],
) -> dict[str, str | None]:
    normalized: dict[str, str | None] = {}

    for column_name, raw_value in raw_row.items():
        sanitized_name = column_name.strip()
        value, data_type = _normalize_value(sanitized_name, raw_value)
        _track_column(
            tracked_columns,
            column_name=sanitized_name,
            data_type=data_type,
            nullable=value is None,
            technical=False,
        )
        normalized[sanitized_name] = value

    return normalized


def _normalize_value(
    column_name: str,
    raw_value: str | None,
) -> tuple[str | None, BronzeColumnType]:
    if raw_value is None:
        return None, "string"

    trimmed = raw_value.strip()
    if trimmed == "":
        return None, "string"

    if _looks_like_date_column(column_name):
        normalized_date = _normalize_date_value(trimmed)
        if normalized_date is not None:
            return normalized_date, "date"

    if _looks_like_string_only_column(column_name):
        return trimmed, "string"

    normalized_number = _normalize_numeric_value(trimmed)
    if normalized_number is not None:
        if "." in normalized_number:
            return normalized_number, "decimal"
        if _looks_like_integer_column(column_name):
            return normalized_number, "integer"
        if _looks_like_decimal_column(column_name):
            return normalized_number, "decimal"

    return trimmed, "string"


def _looks_like_date_column(column_name: str) -> bool:
    return DATE_COLUMN_PATTERN.search(column_name) is not None


def _looks_like_string_only_column(column_name: str) -> bool:
    return STRING_ONLY_COLUMN_PATTERN.search(column_name) is not None


def _looks_like_integer_column(column_name: str) -> bool:
    return INTEGER_HINT_PATTERN.search(column_name) is not None


def _looks_like_decimal_column(column_name: str) -> bool:
    return DECIMAL_HINT_PATTERN.search(column_name) is not None


def _normalize_date_value(value: str) -> str | None:
    for pattern in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value, pattern).date().isoformat()
        except ValueError:
            continue
    return None


def _normalize_numeric_value(value: str) -> str | None:
    candidate = value.replace(" ", "")
    if "," in candidate and "." in candidate:
        candidate = candidate.replace(".", "").replace(",", ".")
    elif "," in candidate:
        candidate = candidate.replace(",", ".")

    if not re.fullmatch(r"[-+]?\d+(\.\d+)?", candidate):
        return None

    try:
        decimal_value = Decimal(candidate)
    except InvalidOperation:
        return None

    normalized = format(decimal_value, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".") or "0"
    return normalized


def _build_technical_columns(
    *,
    source_file: SourceFileRecord,
    pipeline_run_id: UUID | None,
    processed_at: datetime,
    schema_version: str,
) -> dict[str, str]:
    return {
        "source_system": source_file.source_system,
        "source_file": source_file.file_name,
        "source_url": source_file.source_url,
        "source_file_id": str(source_file.source_file_id),
        "ingestion_timestamp": processed_at.isoformat(),
        "file_hash": source_file.file_hash,
        "schema_version": schema_version,
        "pipeline_run_id": str(pipeline_run_id) if pipeline_run_id is not None else "",
    }


def _serialize_row(
    row: dict[str, str | None],
    columns: Iterable[str],
) -> dict[str, str]:
    return {
        column_name: "" if row.get(column_name) is None else str(row[column_name])
        for column_name in columns
    }


def _track_column(
    tracked_columns: dict[str, _TrackedColumn],
    *,
    column_name: str,
    data_type: BronzeColumnType,
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

    existing.data_type = _merge_types(existing.data_type, data_type)
    existing.nullable = existing.nullable or nullable
    existing.technical = existing.technical or technical


def _merge_technical_columns(tracked_columns: dict[str, _TrackedColumn]) -> None:
    for column_name, data_type in TECHNICAL_COLUMN_TYPES.items():
        _track_column(
            tracked_columns,
            column_name=column_name,
            data_type=data_type,
            nullable=column_name == "pipeline_run_id",
            technical=True,
        )


def _merge_types(left: BronzeColumnType, right: BronzeColumnType) -> BronzeColumnType:
    if left == right:
        return left
    if "string" in {left, right}:
        return "string"
    if {left, right} == {"integer", "decimal"}:
        return "decimal"
    if {left, right} == {"date", "timestamp"}:
        return "string"
    if "timestamp" in {left, right}:
        return "string"
    if "date" in {left, right}:
        return "string"
    return "string"
