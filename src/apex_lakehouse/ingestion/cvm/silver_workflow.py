"""Workflow orchestration for CVM bronze-to-silver promotion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from apex_lakehouse.ingestion.cvm.silver_models import SilverBuildRequest, SilverBuildResult
from apex_lakehouse.ingestion.cvm.silver_service import CvmSilverService


@dataclass(frozen=True)
class CvmSilverBatch:
    requests: list[SilverBuildRequest]
    results: list[SilverBuildResult]

    @property
    def row_count(self) -> int:
        return sum(result.transform_summary.row_count for result in self.results)


class CvmSilverWorkflow:
    """Coordinate one or many silver promotions."""

    def __init__(self, *, service: CvmSilverService):
        self._service = service

    @classmethod
    def from_settings(cls) -> "CvmSilverWorkflow":
        return cls(service=CvmSilverService.from_settings())

    def build_one(self, request: SilverBuildRequest) -> SilverBuildResult:
        return self._service.build(request)

    def build_many(self, requests: Sequence[SilverBuildRequest]) -> CvmSilverBatch:
        request_list = list(requests)
        return CvmSilverBatch(
            requests=request_list,
            results=[self.build_one(request) for request in request_list],
        )
