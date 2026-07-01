"""Project path resolution helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def find_project_root(start: Path | None = None) -> Path:
    """
    Walk upwards until a directory containing `pyproject.toml` is found.

    This avoids hardcoding absolute paths and lets every module discover the
    project root from wherever it is executed.
    """
    current = (start or Path(__file__)).resolve()

    for candidate in [current, *current.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate

    raise RuntimeError("Could not find project root from current path.")


@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    src: Path
    docs: Path
    scripts: Path
    tests: Path
    orchestration: Path
    ingestion: Path
    dbt: Path
    quality: Path
    api: Path
    raw: Path
    lakehouse: Path
    artifacts: Path

    @classmethod
    def from_root(cls, root: Path) -> "ProjectPaths":
        return cls(
            root=root,
            src=root / "src",
            docs=root / "docs",
            scripts=root / "scripts",
            tests=root / "tests",
            orchestration=root / "orchestration",
            ingestion=root / "ingestion",
            dbt=root / "dbt",
            quality=root / "quality",
            api=root / "api",
            raw=root / "raw",
            lakehouse=root / "lakehouse",
            artifacts=root / "artifacts",
        )


def get_project_paths() -> ProjectPaths:
    """Return the canonical path map for the repository."""
    root = find_project_root()
    return ProjectPaths.from_root(root)