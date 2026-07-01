from __future__ import annotations

from unittest.mock import patch

from alembic.config import Config

from apex_lakehouse.config import PlatformSettings, load_settings
from apex_lakehouse.control_plane.service import (
    build_alembic_config,
    get_control_plane_repository,
    upgrade_control_plane,
)


def test_build_alembic_config_uses_settings_url() -> None:
    settings = PlatformSettings.from_env()

    config = build_alembic_config(settings)

    assert isinstance(config, Config)
    assert config.get_main_option("sqlalchemy.url") == settings.postgres.sqlalchemy_url
    assert config.get_main_option("script_location").endswith("alembic")


def test_upgrade_control_plane_calls_alembic_head() -> None:
    settings = load_settings()

    with patch("apex_lakehouse.control_plane.service.command.upgrade") as mock_upgrade:
        upgrade_control_plane(settings)

    assert mock_upgrade.call_count == 1
    config, revision = mock_upgrade.call_args[0]
    assert isinstance(config, Config)
    assert revision == "head"


def test_get_control_plane_repository_delegates_to_factory() -> None:
    settings = load_settings()

    with patch(
        "apex_lakehouse.control_plane.service.ControlPlaneRepository.from_settings"
    ) as mock_from_settings:
        get_control_plane_repository(settings)

    mock_from_settings.assert_called_once_with(settings=settings)
