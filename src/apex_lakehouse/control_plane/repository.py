"""PostgreSQL repository for operational control-plane records."""

from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import asdict
from typing import Generator, Sequence
from uuid import UUID

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from apex_lakehouse.config import PlatformSettings, load_settings
from apex_lakehouse.control_plane.records import (
    DataQualityResultRecord,
    IngestionStateRecord,
    PipelineRunRecord,
    QuarantineRecord,
    SourceFileRecord,
)


class ControlPlaneRepository:
    """
    Repository responsible for persisting and reading operational metadata.

    This layer isolates SQL details from the rest of the pipeline code.
    """

    def __init__(self, engine: Engine):
        self._engine = engine
        self._session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    @classmethod
    def from_settings(
        cls,
        settings: PlatformSettings | None = None,
        *,
        echo: bool = False,
    ) -> "ControlPlaneRepository":
        resolved_settings = settings or load_settings()
        engine = create_engine(
            resolved_settings.postgres.sqlalchemy_url,
            future=True,
            pool_pre_ping=True,
            echo=echo,
        )
        return cls(engine)

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def insert_pipeline_run(self, record: PipelineRunRecord) -> None:
        payload = asdict(record)
        payload["pipeline_run_id"] = str(record.pipeline_run_id)
        payload["trigger_mode"] = record.trigger_mode.value
        payload["status"] = record.status.value
        payload["details_json"] = json.dumps(record.details_json)

        statement = text(
            """
            insert into ops.pipeline_run_log (
                pipeline_run_id,
                pipeline_name,
                source_system,
                dataset_name,
                trigger_mode,
                status,
                orchestration_job_name,
                orchestration_run_key,
                requested_by,
                requested_start_date,
                requested_end_date,
                started_at,
                finished_at,
                rows_read,
                rows_written,
                rows_quarantined,
                files_discovered,
                files_processed,
                files_skipped,
                error_code,
                error_message,
                details_json
            ) values (
                :pipeline_run_id,
                :pipeline_name,
                :source_system,
                :dataset_name,
                :trigger_mode,
                :status,
                :orchestration_job_name,
                :orchestration_run_key,
                :requested_by,
                :requested_start_date,
                :requested_end_date,
                :started_at,
                :finished_at,
                :rows_read,
                :rows_written,
                :rows_quarantined,
                :files_discovered,
                :files_processed,
                :files_skipped,
                :error_code,
                :error_message,
                cast(:details_json as jsonb)
            )
            """
        )

        with self.session() as session:
            session.execute(statement, payload)

    def upsert_source_file(self, record: SourceFileRecord) -> None:
        payload = asdict(record)
        payload["source_file_id"] = str(record.source_file_id)
        payload["status"] = record.status.value
        payload["last_pipeline_run_id"] = (
            str(record.last_pipeline_run_id) if record.last_pipeline_run_id else None
        )

        statement = text(
            """
            insert into ops.source_file_registry (
                source_file_id,
                source_system,
                dataset_name,
                source_url,
                file_name,
                storage_bucket,
                storage_key,
                competence,
                business_date,
                content_type,
                file_hash,
                file_size_bytes,
                source_last_modified_at,
                first_seen_at,
                last_seen_at,
                first_ingested_at,
                latest_ingested_at,
                status,
                last_pipeline_run_id
            ) values (
                :source_file_id,
                :source_system,
                :dataset_name,
                :source_url,
                :file_name,
                :storage_bucket,
                :storage_key,
                :competence,
                :business_date,
                :content_type,
                :file_hash,
                :file_size_bytes,
                :source_last_modified_at,
                :first_seen_at,
                :last_seen_at,
                :first_ingested_at,
                :latest_ingested_at,
                :status,
                :last_pipeline_run_id
            )
            on conflict (dataset_name, source_url, file_hash)
            do update set
                file_name = excluded.file_name,
                storage_bucket = excluded.storage_bucket,
                storage_key = excluded.storage_key,
                competence = excluded.competence,
                business_date = excluded.business_date,
                content_type = excluded.content_type,
                file_size_bytes = excluded.file_size_bytes,
                source_last_modified_at = excluded.source_last_modified_at,
                last_seen_at = excluded.last_seen_at,
                first_ingested_at = coalesce(
                    ops.source_file_registry.first_ingested_at,
                    excluded.first_ingested_at
                ),
                latest_ingested_at = excluded.latest_ingested_at,
                status = excluded.status,
                last_pipeline_run_id = excluded.last_pipeline_run_id
            """
        )

        with self.session() as session:
            session.execute(statement, payload)

    def upsert_ingestion_state(self, record: IngestionStateRecord) -> None:
        payload = asdict(record)
        payload["last_successful_run_id"] = (
            str(record.last_successful_run_id) if record.last_successful_run_id else None
        )
        payload["last_attempted_run_id"] = (
            str(record.last_attempted_run_id) if record.last_attempted_run_id else None
        )

        statement = text(
            """
            insert into ops.ingestion_state (
                source_system,
                dataset_name,
                watermark_business_date,
                watermark_competence,
                last_successful_run_id,
                last_attempted_run_id,
                lock_version,
                updated_at,
                updated_by
            ) values (
                :source_system,
                :dataset_name,
                :watermark_business_date,
                :watermark_competence,
                :last_successful_run_id,
                :last_attempted_run_id,
                :lock_version,
                :updated_at,
                :updated_by
            )
            on conflict (source_system, dataset_name)
            do update set
                watermark_business_date = excluded.watermark_business_date,
                watermark_competence = excluded.watermark_competence,
                last_successful_run_id = excluded.last_successful_run_id,
                last_attempted_run_id = excluded.last_attempted_run_id,
                lock_version = ops.ingestion_state.lock_version + 1,
                updated_at = excluded.updated_at,
                updated_by = excluded.updated_by
            """
        )

        with self.session() as session:
            session.execute(statement, payload)

    def insert_data_quality_result(self, record: DataQualityResultRecord) -> None:
        payload = asdict(record)
        payload["quality_result_id"] = str(record.quality_result_id)
        payload["pipeline_run_id"] = str(record.pipeline_run_id)
        payload["severity"] = record.severity.value
        payload["status"] = record.status.value
        payload["details_json"] = json.dumps(record.details_json)

        statement = text(
            """
            insert into ops.data_quality_results (
                quality_result_id,
                pipeline_run_id,
                dataset_name,
                layer_name,
                rule_code,
                rule_name,
                severity,
                status,
                blocking,
                partition_key,
                row_count_evaluated,
                row_count_failed,
                failure_ratio,
                details_json,
                evaluated_at
            ) values (
                :quality_result_id,
                :pipeline_run_id,
                :dataset_name,
                :layer_name,
                :rule_code,
                :rule_name,
                :severity,
                :status,
                :blocking,
                :partition_key,
                :row_count_evaluated,
                :row_count_failed,
                :failure_ratio,
                cast(:details_json as jsonb),
                :evaluated_at
            )
            """
        )

        with self.session() as session:
            session.execute(statement, payload)

    def insert_quarantine_record(self, record: QuarantineRecord) -> None:
        payload = asdict(record)
        payload["quarantine_id"] = str(record.quarantine_id)
        payload["pipeline_run_id"] = str(record.pipeline_run_id)
        payload["quarantine_status"] = record.quarantine_status.value
        payload["payload_json"] = json.dumps(record.payload_json)

        statement = text(
            """
            insert into quarantine.base_records (
                quarantine_id,
                pipeline_run_id,
                source_system,
                dataset_name,
                layer_name,
                record_locator,
                rule_code,
                reason,
                quarantine_status,
                payload_json,
                quarantined_at,
                resolved_at,
                resolution_note
            ) values (
                :quarantine_id,
                :pipeline_run_id,
                :source_system,
                :dataset_name,
                :layer_name,
                :record_locator,
                :rule_code,
                :reason,
                :quarantine_status,
                cast(:payload_json as jsonb),
                :quarantined_at,
                :resolved_at,
                :resolution_note
            )
            """
        )

        with self.session() as session:
            session.execute(statement, payload)

    def update_quarantine_status(
        self,
        quarantine_id: UUID,
        *,
        quarantine_status: str,
        resolved_at=None,
        resolution_note: str | None = None,
    ) -> None:
        statement = text(
            """
            update quarantine.base_records
            set
                quarantine_status = :quarantine_status,
                resolved_at = :resolved_at,
                resolution_note = :resolution_note
            where quarantine_id = :quarantine_id
            """
        )

        with self.session() as session:
            session.execute(
                statement,
                {
                    "quarantine_id": str(quarantine_id),
                    "quarantine_status": quarantine_status,
                    "resolved_at": resolved_at,
                    "resolution_note": resolution_note,
                },
            )

    def get_latest_pipeline_run(
        self,
        pipeline_name: str,
    ) -> dict[str, object] | None:
        statement = text(
            """
            select
                pipeline_run_id,
                pipeline_name,
                source_system,
                dataset_name,
                trigger_mode,
                status,
                started_at,
                finished_at,
                rows_read,
                rows_written,
                rows_quarantined,
                files_discovered,
                files_processed,
                files_skipped,
                error_code,
                error_message,
                details_json
            from ops.pipeline_run_log
            where pipeline_name = :pipeline_name
            order by started_at desc
            limit 1
            """
        )

        with self.session() as session:
            row = session.execute(statement, {"pipeline_name": pipeline_name}).mappings().first()
            return dict(row) if row else None

    def list_latest_pipeline_runs(
        self,
        *,
        limit: int = 20,
        pipeline_name: str | None = None,
    ) -> list[dict[str, object]]:
        conditions = []
        payload: dict[str, object] = {"limit": limit}
        if pipeline_name is not None:
            conditions.append("pipeline_name = :pipeline_name")
            payload["pipeline_name"] = pipeline_name

        where_clause = ""
        if conditions:
            where_clause = "where " + " and ".join(conditions)

        statement = text(
            f"""
            select
                pipeline_run_id,
                pipeline_name,
                source_system,
                dataset_name,
                trigger_mode,
                status,
                started_at,
                finished_at,
                rows_read,
                rows_written,
                rows_quarantined,
                files_discovered,
                files_processed,
                files_skipped,
                error_code,
                error_message,
                details_json
            from ops.pipeline_run_log
            {where_clause}
            order by started_at desc
            limit :limit
            """
        )

        with self.session() as session:
            rows = session.execute(statement, payload).mappings().all()
            return [dict(row) for row in rows]

    def get_ingestion_state(
        self,
        source_system: str,
        dataset_name: str,
    ) -> dict[str, object] | None:
        statement = text(
            """
            select
                source_system,
                dataset_name,
                watermark_business_date,
                watermark_competence,
                last_successful_run_id,
                last_attempted_run_id,
                lock_version,
                updated_at,
                updated_by
            from ops.ingestion_state
            where source_system = :source_system
              and dataset_name = :dataset_name
            """
        )

        with self.session() as session:
            row = session.execute(
                statement,
                {
                    "source_system": source_system,
                    "dataset_name": dataset_name,
                },
            ).mappings().first()
            return dict(row) if row else None

    def get_source_files_by_urls(
        self,
        *,
        source_system: str,
        source_urls: Sequence[str],
    ) -> list[dict[str, object]]:
        if not source_urls:
            return []

        statement = text(
            """
            select
                source_file_id,
                source_system,
                dataset_name,
                source_url,
                file_name,
                file_hash,
                status,
                source_last_modified_at
            from ops.source_file_registry
            where source_system = :source_system
              and source_url = any(:source_urls)
            """
        )

        with self.session() as session:
            rows = session.execute(
                statement,
                {
                    "source_system": source_system,
                    "source_urls": list(source_urls),
                },
            ).mappings().all()
            return [dict(row) for row in rows]

    def list_source_files(
        self,
        *,
        source_system: str,
        dataset_names: Sequence[str] | None = None,
        status: str | None = None,
        competence: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        conditions = ["source_system = :source_system"]
        payload: dict[str, object] = {
            "source_system": source_system,
            "limit": limit,
        }
        if dataset_names:
            conditions.append("dataset_name = any(:dataset_names)")
            payload["dataset_names"] = list(dataset_names)
        if status is not None:
            conditions.append("status = :status")
            payload["status"] = status
        if competence is not None:
            conditions.append("competence = :competence")
            payload["competence"] = competence

        statement = text(
            f"""
            select
                source_file_id,
                source_system,
                dataset_name,
                source_url,
                file_name,
                storage_bucket,
                storage_key,
                competence,
                business_date,
                content_type,
                file_hash,
                file_size_bytes,
                source_last_modified_at,
                first_seen_at,
                last_seen_at,
                first_ingested_at,
                latest_ingested_at,
                status,
                last_pipeline_run_id
            from ops.source_file_registry
            where {' and '.join(conditions)}
            order by coalesce(latest_ingested_at, last_seen_at) desc
            limit :limit
            """
        )

        with self.session() as session:
            rows = session.execute(statement, payload).mappings().all()
            return [dict(row) for row in rows]

    def list_latest_quality_results(
        self,
        *,
        limit: int = 20,
        dataset_name: str | None = None,
    ) -> list[dict[str, object]]:
        conditions = []
        payload: dict[str, object] = {"limit": limit}
        if dataset_name is not None:
            conditions.append("dataset_name = :dataset_name")
            payload["dataset_name"] = dataset_name

        where_clause = ""
        if conditions:
            where_clause = "where " + " and ".join(conditions)

        statement = text(
            f"""
            select
                quality_result_id,
                pipeline_run_id,
                dataset_name,
                layer_name,
                rule_code,
                rule_name,
                severity,
                status,
                blocking,
                partition_key,
                row_count_evaluated,
                row_count_failed,
                failure_ratio,
                details_json,
                evaluated_at
            from ops.data_quality_results
            {where_clause}
            order by evaluated_at desc
            limit :limit
            """
        )

        with self.session() as session:
            rows = session.execute(statement, payload).mappings().all()
            return [dict(row) for row in rows]

    def check_health(self) -> bool:
        statement = text("select 1")
        with self.session() as session:
            value = session.execute(statement).scalar_one()
            return value == 1

    def mark_pipeline_run_finished(
        self,
        pipeline_run_id: UUID,
        *,
        status: str,
        finished_at,
        rows_read: int,
        rows_written: int,
        rows_quarantined: int,
        files_discovered: int,
        files_processed: int,
        files_skipped: int,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        statement = text(
            """
            update ops.pipeline_run_log
            set
                status = :status,
                finished_at = :finished_at,
                rows_read = :rows_read,
                rows_written = :rows_written,
                rows_quarantined = :rows_quarantined,
                files_discovered = :files_discovered,
                files_processed = :files_processed,
                files_skipped = :files_skipped,
                error_code = :error_code,
                error_message = :error_message
            where pipeline_run_id = :pipeline_run_id
            """
        )

        with self.session() as session:
            session.execute(
                statement,
                {
                    "pipeline_run_id": str(pipeline_run_id),
                    "status": status,
                    "finished_at": finished_at,
                    "rows_read": rows_read,
                    "rows_written": rows_written,
                    "rows_quarantined": rows_quarantined,
                    "files_discovered": files_discovered,
                    "files_processed": files_processed,
                    "files_skipped": files_skipped,
                    "error_code": error_code,
                    "error_message": error_message,
                },
            )
