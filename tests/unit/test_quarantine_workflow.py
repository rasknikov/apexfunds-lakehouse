from __future__ import annotations

from datetime import datetime
from pathlib import Path
from uuid import uuid4

from apex_lakehouse.quarantine.models import (
    QuarantineBuildRequest,
    QuarantineBuildResult,
    QuarantineReplayRequest,
)
from apex_lakehouse.quarantine.workflow import DatasetQuarantineBatch, DatasetQuarantineWorkflow


def test_quarantine_workflow_aggregates_record_count_and_forwards_replay() -> None:
    results = [
        QuarantineBuildResult(
            request=None,  # type: ignore[arg-type]
            records=[object(), object()],  # type: ignore[list-item]
            created_at=datetime(2024, 1, 15, 11, 0, 0),
        ),
        QuarantineBuildResult(
            request=None,  # type: ignore[arg-type]
            records=[object()],  # type: ignore[list-item]
            created_at=datetime(2024, 1, 15, 11, 5, 0),
        ),
    ]
    replay_calls = []
    service = type(
        "ServiceStub",
        (),
        {
            "build": lambda self, request: results.pop(0),
            "mark_for_replay": lambda self, request: replay_calls.append(request),
        },
    )()
    workflow = DatasetQuarantineWorkflow(service=service)  # type: ignore[arg-type]

    batch = workflow.build_many([None, None])  # type: ignore[list-item]
    replay_request = QuarantineReplayRequest(
        quarantine_id=uuid4(),
        requested_at=datetime(2024, 1, 15, 12, 0, 0),
        resolution_note="retry",
    )
    workflow.mark_for_replay(replay_request)

    assert isinstance(batch, DatasetQuarantineBatch)
    assert batch.record_count == 3
    assert replay_calls == [replay_request]
