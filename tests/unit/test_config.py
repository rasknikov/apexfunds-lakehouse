from __future__ import annotations

from apex_lakehouse.config import load_settings, reset_settings_cache


def test_settings_defaults(monkeypatch) -> None:
    for key in [
        "APEX_ENV",
        "APEX_DEBUG",
        "APEX_LOG_LEVEL",
        "APEX_API_HOST",
        "APEX_API_PORT",
        "APEX_POSTGRES_HOST",
        "APEX_POSTGRES_PORT",
        "APEX_POSTGRES_DB",
        "APEX_POSTGRES_USER",
        "APEX_POSTGRES_PASSWORD",
        "APEX_MINIO_ENDPOINT",
        "APEX_MINIO_REGION",
        "APEX_MINIO_ACCESS_KEY",
        "APEX_MINIO_SECRET_KEY",
        "APEX_MINIO_RAW_BUCKET",
        "APEX_MINIO_LAKEHOUSE_BUCKET",
        "APEX_MINIO_ARTIFACTS_BUCKET",
        "APEX_TRINO_HOST",
        "APEX_TRINO_PORT",
        "APEX_TRINO_CATALOG",
        "APEX_TRINO_SCHEMA",
        "APEX_SPARK_MASTER_URL",
    ]:
        monkeypatch.delenv(key, raising=False)

    reset_settings_cache()
    settings = load_settings()

    assert settings.environment == "local"
    assert settings.debug is True
    assert settings.api.port == 8000
    assert settings.postgres.database == "apex_lakehouse"
    assert settings.object_storage.raw_bucket == "raw"
    assert settings.trino.base_url == "http://localhost:8081"
    assert settings.spark.master_url == "spark://localhost:7077"


def test_settings_env_override(monkeypatch) -> None:
    monkeypatch.setenv("APEX_ENV", "staging")
    monkeypatch.setenv("APEX_DEBUG", "false")
    monkeypatch.setenv("APEX_API_PORT", "9000")
    monkeypatch.setenv("APEX_POSTGRES_HOST", "postgres")
    monkeypatch.setenv("APEX_MINIO_SECRET_KEY", "super-secret")

    reset_settings_cache()
    settings = load_settings()

    assert settings.environment == "staging"
    assert settings.debug is False
    assert settings.api.port == 9000
    assert settings.postgres.host == "postgres"
    assert settings.public_dict()["object_storage"]["secret_key"] == "***"
