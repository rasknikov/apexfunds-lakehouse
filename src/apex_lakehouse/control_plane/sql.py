"""Canonical SQL definitions for the operational control plane."""

from __future__ import annotations


CREATE_CONTROL_PLANE_SCHEMAS_SQL = """
create schema if not exists ops;
create schema if not exists quarantine;
""".strip()


CREATE_SOURCE_FILE_REGISTRY_SQL = """
create table if not exists ops.source_file_registry (
    source_file_id uuid primary key,
    source_system text not null,
    dataset_name text not null,
    source_url text not null,
    file_name text not null,
    storage_bucket text,
    storage_key text,
    competence text,
    business_date date,
    content_type text,
    file_hash text not null,
    file_size_bytes bigint,
    source_last_modified_at timestamptz,
    first_seen_at timestamptz not null,
    last_seen_at timestamptz not null,
    first_ingested_at timestamptz,
    latest_ingested_at timestamptz,
    status text not null,
    last_pipeline_run_id uuid,
    constraint chk_source_file_registry_status
        check (status in ('discovered', 'ingested', 'skipped', 'failed', 'superseded')),
    constraint uq_source_file_registry_identity
        unique (dataset_name, source_url, file_hash)
);

create index if not exists idx_source_file_registry_dataset_status
    on ops.source_file_registry (dataset_name, status);

create index if not exists idx_source_file_registry_dataset_competence
    on ops.source_file_registry (dataset_name, competence);

create index if not exists idx_source_file_registry_dataset_business_date
    on ops.source_file_registry (dataset_name, business_date);
""".strip()


CREATE_INGESTION_STATE_SQL = """
create table if not exists ops.ingestion_state (
    source_system text not null,
    dataset_name text not null,
    watermark_business_date date,
    watermark_competence text,
    last_successful_run_id uuid,
    last_attempted_run_id uuid,
    lock_version bigint not null default 0,
    updated_at timestamptz not null,
    updated_by text not null,
    primary key (source_system, dataset_name)
);

create index if not exists idx_ingestion_state_updated_at
    on ops.ingestion_state (updated_at);
""".strip()


CREATE_PIPELINE_RUN_LOG_SQL = """
create table if not exists ops.pipeline_run_log (
    pipeline_run_id uuid primary key,
    pipeline_name text not null,
    source_system text not null,
    dataset_name text not null,
    trigger_mode text not null,
    status text not null,
    orchestration_job_name text,
    orchestration_run_key text,
    requested_by text,
    requested_start_date date,
    requested_end_date date,
    started_at timestamptz not null,
    finished_at timestamptz,
    rows_read bigint not null default 0,
    rows_written bigint not null default 0,
    rows_quarantined bigint not null default 0,
    files_discovered bigint not null default 0,
    files_processed bigint not null default 0,
    files_skipped bigint not null default 0,
    error_code text,
    error_message text,
    details_json jsonb not null default '{}'::jsonb,
    constraint chk_pipeline_run_log_status
        check (status in ('pending', 'running', 'succeeded', 'failed', 'partial', 'cancelled')),
    constraint chk_pipeline_run_log_trigger_mode
        check (trigger_mode in ('scheduled', 'manual', 'replay', 'backfill'))
);

create index if not exists idx_pipeline_run_log_pipeline_started_at
    on ops.pipeline_run_log (pipeline_name, started_at desc);

create index if not exists idx_pipeline_run_log_dataset_started_at
    on ops.pipeline_run_log (dataset_name, started_at desc);

create index if not exists idx_pipeline_run_log_status_started_at
    on ops.pipeline_run_log (status, started_at desc);
""".strip()


CREATE_DATA_QUALITY_RESULTS_SQL = """
create table if not exists ops.data_quality_results (
    quality_result_id uuid primary key,
    pipeline_run_id uuid not null references ops.pipeline_run_log (pipeline_run_id),
    dataset_name text not null,
    layer_name text not null,
    rule_code text not null,
    rule_name text not null,
    severity text not null,
    status text not null,
    blocking boolean not null,
    partition_key text,
    row_count_evaluated bigint not null default 0,
    row_count_failed bigint not null default 0,
    failure_ratio numeric(12,6),
    details_json jsonb not null default '{}'::jsonb,
    evaluated_at timestamptz not null,
    constraint chk_data_quality_results_severity
        check (severity in ('info', 'warn', 'error', 'critical')),
    constraint chk_data_quality_results_status
        check (status in ('passed', 'failed'))
);

create index if not exists idx_data_quality_results_dataset_evaluated_at
    on ops.data_quality_results (dataset_name, evaluated_at desc);

create index if not exists idx_data_quality_results_pipeline_run_id
    on ops.data_quality_results (pipeline_run_id);

create index if not exists idx_data_quality_results_severity_status
    on ops.data_quality_results (severity, status);
""".strip()


CREATE_QUARANTINE_BASE_RECORDS_SQL = """
create table if not exists quarantine.base_records (
    quarantine_id uuid primary key,
    pipeline_run_id uuid not null references ops.pipeline_run_log (pipeline_run_id),
    source_system text not null,
    dataset_name text not null,
    layer_name text not null,
    record_locator text,
    rule_code text not null,
    reason text not null,
    quarantine_status text not null,
    payload_json jsonb not null,
    quarantined_at timestamptz not null,
    resolved_at timestamptz,
    resolution_note text,
    constraint chk_quarantine_base_records_status
        check (quarantine_status in ('open', 'replay_pending', 'resolved', 'dismissed'))
);

create index if not exists idx_quarantine_base_records_dataset_quarantined_at
    on quarantine.base_records (dataset_name, quarantined_at desc);

create index if not exists idx_quarantine_base_records_pipeline_run_id
    on quarantine.base_records (pipeline_run_id);

create index if not exists idx_quarantine_base_records_status
    on quarantine.base_records (quarantine_status);
""".strip()


CONTROL_PLANE_DDL_STATEMENTS = (
    CREATE_CONTROL_PLANE_SCHEMAS_SQL,
    CREATE_SOURCE_FILE_REGISTRY_SQL,
    CREATE_INGESTION_STATE_SQL,
    CREATE_PIPELINE_RUN_LOG_SQL,
    CREATE_DATA_QUALITY_RESULTS_SQL,
    CREATE_QUARANTINE_BASE_RECORDS_SQL,
)