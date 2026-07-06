from __future__ import annotations

from apex_lakehouse.quality.models import DatasetQualityEvaluation, DatasetQualityRequest, PromotionGateDecision
from apex_lakehouse.quality.workflow import DatasetQualityBatch, DatasetQualityWorkflow


def test_quality_workflow_builds_batch_and_aggregates_gate() -> None:
    evaluations = [
        DatasetQualityEvaluation(
            request=None,  # type: ignore[arg-type]
            local_dataset_path=__import__("pathlib").Path("a.csv"),
            records=[],
            gate=PromotionGateDecision(
                allowed=True,
                blocking_failures=0,
                failed_rules=0,
                reason="ok",
            ),
        ),
        DatasetQualityEvaluation(
            request=None,  # type: ignore[arg-type]
            local_dataset_path=__import__("pathlib").Path("b.csv"),
            records=[],
            gate=PromotionGateDecision(
                allowed=False,
                blocking_failures=1,
                failed_rules=1,
                reason="blocked",
            ),
        ),
    ]
    service = type(
        "ServiceStub",
        (),
        {"evaluate": lambda self, request: evaluations.pop(0)},
    )()
    workflow = DatasetQualityWorkflow(service=service)  # type: ignore[arg-type]

    batch = workflow.evaluate_many([None, None])  # type: ignore[list-item]

    assert isinstance(batch, DatasetQualityBatch)
    assert len(batch.evaluations) == 2
    assert batch.promotion_allowed is False
