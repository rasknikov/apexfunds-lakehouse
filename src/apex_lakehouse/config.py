"""Environment-aware platform configuration."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from functools import lru_cache
import os
from typing import Dict


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default

    return int(value)


@dataclass(frozen=True)
class ApiSettings:
    host: str
    port: int

    @classmethod
    def from_env(cls) -> "ApiSettings":
        return cls(
            host=os.getenv("APEX_API_HOST", "0.0.0.0"),
            port=_get_int("APEX_API_PORT", 8000),
        )


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "PostgresSettings":
        return cls(
            host=os.getenv("APEX_POSTGRES_HOST", "localhost"),
            port=_get_int("APEX_POSTGRES_PORT", 5432),
            database=os.getenv("APEX_POSTGRES_DB", "apex_lakehouse"),
            user=os.getenv("APEX_POSTGRES_USER", "apex"),
            password=os.getenv("APEX_POSTGRES_PASSWORD", "apex"),
        )

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class ObjectStorageSettings:
    endpoint: str
    region: str
    access_key: str
    secret_key: str
    raw_bucket: str
    lakehouse_bucket: str
    artifacts_bucket: str

    @classmethod
    def from_env(cls) -> "ObjectStorageSettings":
        return cls(
            endpoint=os.getenv("APEX_MINIO_ENDPOINT", "http://localhost:9000"),
            region=os.getenv("APEX_MINIO_REGION", "us-east-1"),
            access_key=os.getenv("APEX_MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("APEX_MINIO_SECRET_KEY", "minioadmin"),
            raw_bucket=os.getenv("APEX_MINIO_RAW_BUCKET", "raw"),
            lakehouse_bucket=os.getenv("APEX_MINIO_LAKEHOUSE_BUCKET", "lakehouse"),
            artifacts_bucket=os.getenv("APEX_MINIO_ARTIFACTS_BUCKET", "artifacts"),
        )


@dataclass(frozen=True)
class TrinoSettings:
    host: str
    port: int
    catalog: str
    schema: str

    @classmethod
    def from_env(cls) -> "TrinoSettings":
        return cls(
            host=os.getenv("APEX_TRINO_HOST", "localhost"),
            port=_get_int("APEX_TRINO_PORT", 8081),
            catalog=os.getenv("APEX_TRINO_CATALOG", "memory"),
            schema=os.getenv("APEX_TRINO_SCHEMA", "default"),
        )

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


@dataclass(frozen=True)
class SparkSettings:
    master_url: str

    @classmethod
    def from_env(cls) -> "SparkSettings":
        return cls(master_url=os.getenv("APEX_SPARK_MASTER_URL", "spark://localhost:7077"))


@dataclass(frozen=True)
class PlatformSettings:
    environment: str
    debug: bool
    log_level: str
    api: ApiSettings
    postgres: PostgresSettings
    object_storage: ObjectStorageSettings
    trino: TrinoSettings
    spark: SparkSettings

    @classmethod
    def from_env(cls) -> "PlatformSettings":
        return cls(
            environment=os.getenv("APEX_ENV", "local"),
            debug=_get_bool("APEX_DEBUG", True),
            log_level=os.getenv("APEX_LOG_LEVEL", "INFO"),
            api=ApiSettings.from_env(),
            postgres=PostgresSettings.from_env(),
            object_storage=ObjectStorageSettings.from_env(),
            trino=TrinoSettings.from_env(),
            spark=SparkSettings.from_env(),
        )

    def public_dict(self) -> Dict[str, object]:
        payload = asdict(self)
        payload["postgres"]["password"] = "***"
        payload["object_storage"]["secret_key"] = "***"
        return payload


@lru_cache(maxsize=1)
def load_settings() -> PlatformSettings:
    return PlatformSettings.from_env()


def reset_settings_cache() -> None:
    load_settings.cache_clear()
