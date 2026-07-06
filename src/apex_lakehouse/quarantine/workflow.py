"""Workflow orchestration for quarantine generation and replay requests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from apex_lakehouse.quarantine.models import (
    QuarantineBuildRequest,
    QuarantineBuildResult,
    QuarantineReplayRequest,
)
from apex_lakehouse.quarantine.service import DatasetQuarantineService


@dataclass(frozen=True)
class DatasetQuarantineBatch:
    requests: list[QuarantineBuildRequest]
    results: list[QuarantineBuildResult]

    @property
    def record_count(self) -> int:
        return sum(len(result.records) for result in self.results)


class DatasetQuarantineWorkflow:
    """Coordinate quarantine generation for one or many evaluated datasets."""

    def __init__(self, *, service: DatasetQuarantineService):
        self._service = service

    @classmethod
    def from_repository(cls, repository) -> "DatasetQuarantineWorkflow":
        return cls(service=DatasetQuarantineService.from_repository(repository))

    def build_one(self, request: QuarantineBuildRequest) -> QuarantineBuildResult:
        return self._service.build(request)

    def build_many(
        self,
        requests: Sequence[QuarantineBuildRequest],
    ) -> DatasetQuarantineBatch:
        request_list = list(requests)
        return DatasetQuarantineBatch(
            requests=request_list,
            results=[self.build_one(request) for request in request_list],
        )

    def mark_for_replay(self, request: QuarantineReplayRequest) -> None:
        self._service.mark_for_replay(request)
