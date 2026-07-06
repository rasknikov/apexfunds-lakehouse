"""Workflow orchestration for CVM gold mart publication."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from apex_lakehouse.ingestion.cvm.gold_models import GoldMartBuildRequest, GoldMartBuildResult
from apex_lakehouse.ingestion.cvm.gold_service import CvmGoldService


@dataclass(frozen=True)
class CvmGoldBatch:
    requests: list[GoldMartBuildRequest]
    results: list[GoldMartBuildResult]

    @property
    def dataset_count(self) -> int:
        return sum(len(result.outputs) for result in self.results)


class CvmGoldWorkflow:
    """Coordinate one or many gold mart builds."""

    def __init__(self, *, service: CvmGoldService):
        self._service = service

    @classmethod
    def from_settings(cls) -> "CvmGoldWorkflow":
        return cls(service=CvmGoldService.from_settings())

    def build_one(self, request: GoldMartBuildRequest) -> GoldMartBuildResult:
        return self._service.build(request)

    def build_many(self, requests: Sequence[GoldMartBuildRequest]) -> CvmGoldBatch:
        request_list = list(requests)
        return CvmGoldBatch(
            requests=request_list,
            results=[self.build_one(request) for request in request_list],
        )
