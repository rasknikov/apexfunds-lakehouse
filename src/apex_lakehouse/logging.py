"""Logging helpers shared across ingestion, API and batch modules."""

from __future__ import annotations

import logging
from typing import Optional

from apex_lakehouse.config import load_settings


LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | "
    "%(message)s"
)


def configure_logging(level: Optional[str] = None) -> None:
    """
    Configure the root logging system once for the whole process.

    If no level is provided, the value comes from the platform settings.
    """
    settings = load_settings()
    resolved_level = (level or settings.log_level).upper()

    logging.basicConfig(
        level=resolved_level,
        format=LOG_FORMAT,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    """
    Return a module-specific logger.

    Modules should call this instead of `logging.getLogger(...)` directly so
    the project keeps one logging convention everywhere.
    """
    return logging.getLogger(name)