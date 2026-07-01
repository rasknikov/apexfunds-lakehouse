"""Canonical contracts for operational control-plane tables."""

from __future__ import annotations

from apex_lakehouse.settings.contracts import ColumnContract, TableContract


SOURCE_FILE_REGISTRY_CONTRACT = TableContract(
    schema_name="ops",
    table_name="source_file_registry",
    description=(
        "Catalog of files discovered in external sources, including identity, "
        "period, hash, storage location and ingestion lifecycle."
    ),
    primary_key=("source_file_id",),
    unique_constraints=(
        ("dataset_name", "source_url", "file_hash"),
    ),
    indexes=(
        ("dataset_name", "status"),
        ("dataset_name", "competence"),
        ("dataset_name", "business_date"),
    ),
    columns=(
        ColumnContract("source_file_id", "uuid", nullable=False, is_primary_key=True),
        ColumnContract("source_system", "text", nullable=False),
        ColumnContract("dataset_name", "text", nullable=False),
        ColumnContract("source_url", "text", nullable=False),
        ColumnContract("file_name", "text", nullable=False),
        ColumnContract("storage_bucket", "text", nullable=True),
        ColumnContract("storage_key", "text", nullable=True),
        ColumnContract("competence", "text", nullable=True),
        ColumnContract("business_date", "date", nullable=True),
        ColumnContract("content_type", "text", nullable=True),
        ColumnContract("file_hash", "text", nullable=False),
        ColumnContract("file_size_bytes", "bigint", nullable=True),
        ColumnContract("source_last_modified_at", "timestamptz", nullable=True),
        ColumnContract("first_seen_at", "timestamptz", nullable=False),
        ColumnContract("last_seen_at", "timestamptz", nullable=False),
        ColumnContract("first_ingested_at", "timestamptz", nullable=True),
        ColumnContract("latest_ingested_at", "timestamptz", nullable=True),
        ColumnContract("status", "text", nullable=False),
        ColumnContract("last_pipeline_run_id", "uuid", nullable=True),
    ),
)

INGESTION_STATE_CONTRACT = TableContract(
    schema_name="ops",
    table_name="ingestion_state",
    description=(
        "Latest known ingestion watermark and execution references per source dataset."
    ),
    primary_key=("source_system", "dataset_name"),
    indexes=(
        ("updated_at",),
    ),
    columns=(
        ColumnContract("source_system", "text", nullable=False, is_primary_key=True),
        ColumnContract("dataset_name", "text", nullable=False, is_primary_key=True),
        ColumnContract("watermark_business_date", "date", nullable=True),
        ColumnContract("watermark_competence", "text", nullable=True),
        ColumnContract("last_successful_run_id", "uuid", nullable=True),
        ColumnContract("last_attempted_run_id", "uuid", nullable=True),
        ColumnContract("lock_version", "bigint", nullable=False),
        ColumnContract("updated_at", "timestamptz", nullable=False),
        ColumnContract("updated_by", "text", nullable=False),
    ),
)

PIPELINE_RUN_LOG_CONTRACT = TableContract(
    schema_name="ops",
    table_name="pipeline_run_log",
    description=(
        "Execution ledger for ingestion and transformation runs, including status, "
        "timings, counters and error details."
    ),
    primary_key=("pipeline_run_id",),
    indexes=(
        ("pipeline_name", "started_at"),
        ("dataset_name", "started_at"),
        ("status", "started_at"),
    ),
    columns=(
        ColumnContract("pipeline_run_id", "uuid", nullable=False, is_primary_key=True),
        ColumnContract("pipeline_name", "text", nullable=False),
        ColumnContract("source_system", "text", nullable=False),
        ColumnContract("dataset_name", "text", nullable=False),
        ColumnContract("trigger_mode", "text", nullable=False),
        ColumnContract("status", "text", nullable=False),
        ColumnContract("orchestration_job_name", "text", nullable=True),
        ColumnContract("orchestration_run_key", "text", nullable=True),
        ColumnContract("requested_by", "text", nullable=True),
        ColumnContract("requested_start_date", "date", nullable=True),
        ColumnContract("requested_end_date", "date", nullable=True),
        ColumnContract("started_at", "timestamptz", nullable=False),
        ColumnContract("finished_at", "timestamptz", nullable=True),
        ColumnContract("rows_read", "bigint", nullable=False),
        ColumnContract("rows_written", "bigint", nullable=False),
        ColumnContract("rows_quarantined", "bigint", nullable=False),
        ColumnContract("files_discovered", "bigint", nullable=False),
        ColumnContract("files_processed", "bigint", nullable=False),
        ColumnContract("files_skipped", "bigint", nullable=False),
        ColumnContract("error_code", "text", nullable=True),
        ColumnContract("error_message", "text", nullable=True),
        ColumnContract("details_json", "jsonb", nullable=False),
    ),
)

DATA_QUALITY_RESULTS_CONTRACT = TableContract(
    schema_name="ops",
    table_name="data_quality_results",
    description=(
        "Result ledger for quality checks executed against bronze, silver or gold datasets."
    ),
    primary_key=("quality_result_id",),
    indexes=(
        ("dataset_name", "evaluated_at"),
        ("pipeline_run_id",),
        ("severity", "status"),
    ),
    columns=(
        ColumnContract("quality_result_id", "uuid", nullable=False, is_primary_key=True),
        ColumnContract("pipeline_run_id", "uuid", nullable=False),
        ColumnContract("dataset_name", "text", nullable=False),
        ColumnContract("layer_name", "text", nullable=False),
        ColumnContract("rule_code", "text", nullable=False),
        ColumnContract("rule_name", "text", nullable=False),
        ColumnContract("severity", "text", nullable=False),
        ColumnContract("status", "text", nullable=False),
        ColumnContract("blocking", "boolean", nullable=False),
        ColumnContract("partition_key", "text", nullable=True),
        ColumnContract("row_count_evaluated", "bigint", nullable=False),
        ColumnContract("row_count_failed", "bigint", nullable=False),
        ColumnContract("failure_ratio", "numeric(12,6)", nullable=True),
        ColumnContract("details_json", "jsonb", nullable=False),
        ColumnContract("evaluated_at", "timestamptz", nullable=False),
    ),
)

QUARANTINE_BASE_RECORDS_CONTRACT = TableContract(
    schema_name="quarantine",
    table_name="base_records",
    description=(
        "Central quarantine table for invalid records diverted from promotion pipelines."
    ),
    primary_key=("quarantine_id",),
    indexes=(
        ("dataset_name", "quarantined_at"),
        ("pipeline_run_id",),
        ("quarantine_status",),
    ),
    columns=(
        ColumnContract("quarantine_id", "uuid", nullable=False, is_primary_key=True),
        ColumnContract("pipeline_run_id", "uuid", nullable=False),
        ColumnContract("source_system", "text", nullable=False),
        ColumnContract("dataset_name", "text", nullable=False),
        ColumnContract("layer_name", "text", nullable=False),
        ColumnContract("record_locator", "text", nullable=True),
        ColumnContract("rule_code", "text", nullable=False),
        ColumnContract("reason", "text", nullable=False),
        ColumnContract("quarantine_status", "text", nullable=False),
        ColumnContract("payload_json", "jsonb", nullable=False),
        ColumnContract("quarantined_at", "timestamptz", nullable=False),
        ColumnContract("resolved_at", "timestamptz", nullable=True),
        ColumnContract("resolution_note", "text", nullable=True),
    ),
)

CONTROL_PLANE_TABLE_CONTRACTS = (
    SOURCE_FILE_REGISTRY_CONTRACT,
    INGESTION_STATE_CONTRACT,
    PIPELINE_RUN_LOG_CONTRACT,
    DATA_QUALITY_RESULTS_CONTRACT,
    QUARANTINE_BASE_RECORDS_CONTRACT,
)