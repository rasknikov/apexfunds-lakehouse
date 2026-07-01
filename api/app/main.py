"""FastAPI entrypoint for local sandbox validation."""

from fastapi import FastAPI

from apex_lakehouse.config import load_settings


settings = load_settings()

app = FastAPI(
    title="Apex Lakehouse API",
    version="0.1.0",
    summary="Sandbox API for the Apex Lakehouse platform.",
)


@app.get("/")
def root() -> dict[str, str]:
    return {
        "message": "Apex Lakehouse sandbox is running.",
        "environment": settings.environment,
    }


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "ok",
        "environment": settings.environment,
        "services": {
            "postgres": settings.postgres.host,
            "object_storage": settings.object_storage.endpoint,
            "trino": settings.trino.base_url,
            "spark": settings.spark.master_url,
        },
    }
