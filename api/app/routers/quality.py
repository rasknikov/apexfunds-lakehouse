"""Operational quality endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from api.app.dependencies import get_control_plane_repository
from api.app.schemas import QualityResultListResponseModel, QualityResultSummaryModel
from apex_lakehouse.control_plane.repository import ControlPlaneRepository

router = APIRouter(tags=["quality"])


@router.get("/quality/latest", response_model=QualityResultListResponseModel)
def get_latest_quality_results(
    limit: int = Query(20, ge=1, le=100),
    dataset_name: str | None = Query(default=None),
    repository: ControlPlaneRepository = Depends(get_control_plane_repository),
) -> QualityResultListResponseModel:
    items = repository.list_latest_quality_results(limit=limit, dataset_name=dataset_name)
    return QualityResultListResponseModel(
        items=[QualityResultSummaryModel(**_normalize_row(item)) for item in items],
        count=len(items),
    )


def _normalize_row(row: dict[str, object]) -> dict[str, object]:
    normalized = dict(row)
    for field_name in ("quality_result_id", "pipeline_run_id"):
        if normalized.get(field_name) is not None:
            normalized[field_name] = str(normalized[field_name])
    if normalized.get("details_json") is None:
        normalized["details_json"] = {}
    return normalized
