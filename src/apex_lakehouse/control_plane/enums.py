from __future__ import annotations

from enum import Enum


class PipelineRunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PARTIAL = "partial"
    CANCELLED = "cancelled"


class SourceFileStatus(str, Enum):
    DISCOVERED = "discovered"
    INGESTED = "ingested"
    SKIPPED = "skipped"
    FAILED = "failed"
    SUPERSEDED = "superseded"


class QualitySeverity(str, Enum):
    INFO = "info"
    WARN = "warn"
    ERROR = "error"
    CRITICAL = "critical"


class QualityCheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"


class QuarantineStatus(str, Enum):
    OPEN = "open"
    REPLAY_PENDING = "replay_pending"
    RESOLVED = "resolved"
    DISMISSED = "dismissed"


class TriggerMode(str, Enum):
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    REPLAY = "replay"
    BACKFILL = "backfill"