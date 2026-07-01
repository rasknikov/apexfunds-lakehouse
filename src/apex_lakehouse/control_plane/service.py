"""Operational helpers for control-plane initialization and access."""

from __future__ import annotations

from alembic import command
from alembic.config import Config

from apex_lakehouse.config import PlatformSettings, load_settings
from apex_lakehouse.control_plane.repository import ControlPlaneRepository
from apex_lakehouse.paths import get_project_paths


def build_alembic_config(settings: PlatformSettings | None = None) -> Config:
    resolved_settings = settings or load_settings()
    paths = get_project_paths()

    config = Config(str(paths.root / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", resolved_settings.postgres.sqlalchemy_url)
    config.set_main_option("script_location", str(paths.root / "alembic"))
    return config


def upgrade_control_plane(settings: PlatformSettings | None = None) -> None:
    config = build_alembic_config(settings)
    command.upgrade(config, "head")


def get_control_plane_repository(
    settings: PlatformSettings | None = None,
) -> ControlPlaneRepository:
    return ControlPlaneRepository.from_settings(settings=settings)