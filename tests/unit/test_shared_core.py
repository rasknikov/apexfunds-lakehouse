from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pytest

from apex_lakehouse.logging import configure_logging, get_logger
from apex_lakehouse.paths import ProjectPaths, find_project_root, get_project_paths
from apex_lakehouse.time import Competence, ensure_not_future, parse_iso_date


def test_find_project_root_from_module_path() -> None:
    expected_root = Path(__file__).resolve().parents[2]
    module_path = expected_root / "src" / "apex_lakehouse" / "paths.py"

    assert find_project_root(module_path) == expected_root


def test_find_project_root_raises_outside_project(tmp_path: Path) -> None:
    outside_path = tmp_path / "isolated" / "file.py"
    outside_path.parent.mkdir(parents=True)
    outside_path.write_text("# test\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="Could not find project root"):
        find_project_root(outside_path)


def test_get_project_paths_returns_canonical_layout() -> None:
    paths = get_project_paths()

    assert isinstance(paths, ProjectPaths)
    assert paths.root.name == "apex-lakehouse"
    assert paths.src == paths.root / "src"
    assert paths.docs == paths.root / "docs"
    assert paths.tests == paths.root / "tests"
    assert paths.api == paths.root / "api"


def test_parse_iso_date_returns_date_object() -> None:
    assert parse_iso_date("2024-01-15") == date(2024, 1, 15)


def test_ensure_not_future_accepts_present_or_past_dates() -> None:
    ensure_not_future(date(2024, 1, 1), reference=date(2024, 1, 31))
    ensure_not_future(date(2024, 1, 31), reference=date(2024, 1, 31))


def test_ensure_not_future_rejects_future_dates() -> None:
    with pytest.raises(ValueError, match="cannot be in the future"):
        ensure_not_future(date(2024, 2, 1), reference=date(2024, 1, 31))


def test_competence_roundtrip_and_partition() -> None:
    competence = Competence.from_string("2024-01")

    assert competence.year == 2024
    assert competence.month == 1
    assert competence.to_date() == date(2024, 1, 1)
    assert competence.to_partition() == "ano=2024/mes=01"
    assert str(competence) == "2024-01"


def test_competence_rejects_invalid_month() -> None:
    with pytest.raises(ValueError, match="Month must be between 1 and 12"):
        Competence(year=2024, month=13)


def test_configure_logging_sets_root_level() -> None:
    configure_logging(level="debug")

    assert logging.getLogger().getEffectiveLevel() == logging.DEBUG


def test_get_logger_returns_named_logger() -> None:
    logger = get_logger("apex.tests.shared_core")

    assert logger.name == "apex.tests.shared_core"
