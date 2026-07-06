"""Operational pipeline endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from api.app.dependencies import get_control_plane_repository
from api.app.schemas import PipelineRunListResponseModel, PipelineRunSummaryModel
from apex_lakehouse.control_plane.repository import ControlPlaneRepository

router = APIRouter(prefix="/ops", tags=["ops"])


@router.get("/pipelines/latest", response_model=PipelineRunListResponseModel)
def get_latest_pipelines(
    limit: int = Query(20, ge=1, le=100),
    pipeline_name: str | None = Query(default=None),
    repository: ControlPlaneRepository = Depends(get_control_plane_repository),
) -> PipelineRunListResponseModel:
    items = repository.list_latest_pipeline_runs(limit=limit, pipeline_name=pipeline_name)
    return PipelineRunListResponseModel(
        items=[PipelineRunSummaryModel(**_normalize_row(item)) for item in items],
        count=len(items),
    )


def _normalize_row(row: dict[str, object]) -> dict[str, object]:
    normalized = dict(row)
    if normalized.get("pipeline_run_id") is not None:
        normalized["pipeline_run_id"] = str(normalized["pipeline_run_id"])
    if normalized.get("details_json") is None:
        normalized["details_json"] = {}
    return normalized
