from __future__ import annotations

from apex_lakehouse.ingestion.cvm.gold_workflow import CvmGoldBatch, CvmGoldWorkflow


def test_build_many_aggregates_gold_dataset_count() -> None:
    results = [
        type("ResultStub", (), {"outputs": [object(), object(), object()]})(),
        type("ResultStub", (), {"outputs": [object(), object(), object()]})(),
    ]
    service = type(
        "ServiceStub",
        (),
        {"build": lambda self, request: results.pop(0)},
    )()
    workflow = CvmGoldWorkflow(service=service)  # type: ignore[arg-type]

    batch = workflow.build_many([None, None])  # type: ignore[list-item]

    assert isinstance(batch, CvmGoldBatch)
    assert len(batch.results) == 2
    assert batch.dataset_count == 6
