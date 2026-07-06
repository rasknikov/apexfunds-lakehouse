"""Workflow orchestration for one or many quality evaluations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from apex_lakehouse.quality.models import DatasetQualityEvaluation, DatasetQualityRequest
from apex_lakehouse.quality.service import DatasetQualityService


@dataclass(frozen=True)
class DatasetQualityBatch:
    requests: list[DatasetQualityRequest]
    evaluations: list[DatasetQualityEvaluation]

    @property
    def promotion_allowed(self) -> bool:
        return all(evaluation.gate.allowed for evaluation in self.evaluations)


class DatasetQualityWorkflow:
    """Coordinate one or many quality evaluations."""

    def __init__(self, *, service: DatasetQualityService):
        self._service = service

    @classmethod
    def from_repository(cls, repository) -> "DatasetQualityWorkflow":
        return cls(service=DatasetQualityService.from_repository(repository))

    def evaluate_one(self, request: DatasetQualityRequest) -> DatasetQualityEvaluation:
        return self._service.evaluate(request)

    def evaluate_many(
        self,
        requests: Sequence[DatasetQualityRequest],
    ) -> DatasetQualityBatch:
        request_list = list(requests)
        return DatasetQualityBatch(
            requests=request_list,
            evaluations=[self.evaluate_one(request) for request in request_list],
        )
