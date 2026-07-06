"""FastAPI entrypoint for operational and local sandbox validation."""

from fastapi import FastAPI

from api.app.routers.health import router as health_router
from api.app.routers.ops import router as ops_router
from api.app.routers.quality import router as quality_router
from apex_lakehouse.config import load_settings


settings = load_settings()

app = FastAPI(
    title="Apex Lakehouse API",
    version="0.1.0",
    summary="Operational API for the Apex Lakehouse platform.",
)


@app.get("/")
def root() -> dict[str, str]:
    return {
        "message": "Apex Lakehouse API is running.",
        "environment": settings.environment,
    }


app.include_router(health_router)
app.include_router(ops_router)
app.include_router(quality_router)
