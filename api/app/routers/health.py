"""Health endpoint router."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from api.app.dependencies import get_control_plane_repository, get_settings
from api.app.schemas import HealthCheckModel, HealthResponseModel
from apex_lakehouse.config import PlatformSettings
from apex_lakehouse.control_plane.repository import ControlPlaneRepository

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponseModel)
def health(
    settings: PlatformSettings = Depends(get_settings),
    repository: ControlPlaneRepository = Depends(get_control_plane_repository),
) -> HealthResponseModel:
    try:
        postgres_ok = repository.check_health()
        postgres_detail = "reachable" if postgres_ok else "unreachable"
    except Exception as exc:
        postgres_ok = False
        postgres_detail = str(exc)

    status = "ok" if postgres_ok else "degraded"
    return HealthResponseModel(
        status=status,
        environment=settings.environment,
        version="0.1.0",
        checked_at=datetime.now(timezone.utc),
        checks={
            "postgres": HealthCheckModel(ok=postgres_ok, detail=postgres_detail),
            "object_storage": HealthCheckModel(ok=True, detail=settings.object_storage.endpoint),
            "trino": HealthCheckModel(ok=True, detail=settings.trino.base_url),
            "spark": HealthCheckModel(ok=True, detail=settings.spark.master_url),
        },
    )
