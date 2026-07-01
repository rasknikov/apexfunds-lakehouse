"""create control plane schemas and tables"""

from __future__ import annotations

from alembic import op

from apex_lakehouse.control_plane.sql import (
    CREATE_CONTROL_PLANE_SCHEMAS_SQL,
    CREATE_DATA_QUALITY_RESULTS_SQL,
    CREATE_INGESTION_STATE_SQL,
    CREATE_PIPELINE_RUN_LOG_SQL,
    CREATE_QUARANTINE_BASE_RECORDS_SQL,
    CREATE_SOURCE_FILE_REGISTRY_SQL,
)

revision = "20260701_01"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(CREATE_CONTROL_PLANE_SCHEMAS_SQL)
    op.execute(CREATE_PIPELINE_RUN_LOG_SQL)
    op.execute(CREATE_SOURCE_FILE_REGISTRY_SQL)
    op.execute(CREATE_INGESTION_STATE_SQL)
    op.execute(CREATE_DATA_QUALITY_RESULTS_SQL)
    op.execute(CREATE_QUARANTINE_BASE_RECORDS_SQL)


def downgrade() -> None:
    op.execute("drop table if exists quarantine.base_records;")
    op.execute("drop table if exists ops.data_quality_results;")
    op.execute("drop table if exists ops.ingestion_state;")
    op.execute("drop table if exists ops.source_file_registry;")
    op.execute("drop table if exists ops.pipeline_run_log;")
    op.execute("drop schema if exists quarantine;")
    op.execute("drop schema if exists ops;")
