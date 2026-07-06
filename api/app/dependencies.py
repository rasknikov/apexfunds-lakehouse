"""Dependency providers for the FastAPI operational API."""

from __future__ import annotations

from apex_lakehouse.config import PlatformSettings, load_settings
from apex_lakehouse.control_plane.repository import ControlPlaneRepository


def get_settings() -> PlatformSettings:
    return load_settings()


def get_control_plane_repository() -> ControlPlaneRepository:
    return ControlPlaneRepository.from_settings()
