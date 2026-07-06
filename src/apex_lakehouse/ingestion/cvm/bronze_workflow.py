"""Workflow orchestration for CVM raw-to-bronze promotion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from apex_lakehouse.ingestion.cvm.bronze_models import BronzeBuildRequest, BronzeBuildResult
from apex_lakehouse.ingestion.cvm.bronze_service import CvmBronzeService


@dataclass(frozen=True)
class CvmBronzeBatch:
    requests: list[BronzeBuildRequest]
    results: list[BronzeBuildResult]

    @property
    def row_count(self) -> int:
        return sum(result.parse_summary.row_count for result in self.results)


class CvmBronzeWorkflow:
    """Coordinate one or many raw-to-bronze promotions."""

    def __init__(self, *, service: CvmBronzeService):
        self._service = service

    @classmethod
    def from_settings(cls) -> "CvmBronzeWorkflow":
        return cls(service=CvmBronzeService.from_settings())

    def build_one(self, request: BronzeBuildRequest) -> BronzeBuildResult:
        return self._service.build(request)

    def build_many(self, requests: Sequence[BronzeBuildRequest]) -> CvmBronzeBatch:
        request_list = list(requests)
        return CvmBronzeBatch(
            requests=request_list,
            results=[self.build_one(request) for request in request_list],
        )
