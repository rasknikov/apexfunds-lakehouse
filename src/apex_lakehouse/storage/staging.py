"""Local staging helpers for files before object-storage upload."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil

from apex_lakehouse.paths import get_project_paths
from apex_lakehouse.storage.files import ensure_file_exists


@dataclass(frozen=True)
class StagingLocation:
    """
    Canonical local staging location for one file prepared by the pipeline.
    """

    directory: Path
    file_path: Path

    @property
    def file_name(self) -> str:
        return self.file_path.name


class LocalStagingManager:
    """
    Manage deterministic local staging directories for ingestion workflows.

    This keeps temporary operational files out of arbitrary ad-hoc folders.
    """

    def __init__(self, base_directory: Path):
        self._base_directory = base_directory.resolve()

    @classmethod
    def from_project_paths(cls) -> "LocalStagingManager":
        project_paths = get_project_paths()
        return cls(project_paths.artifacts / "staging")

    @property
    def base_directory(self) -> Path:
        return self._base_directory

    def prepare_directory(
        self,
        *,
        source_system: str,
        dataset_name: str,
    ) -> Path:
        """
        Create and return the canonical staging directory for one dataset.
        """
        directory = (
            self._base_directory
            / _sanitize_token(source_system)
            / _sanitize_token(dataset_name)
        )
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def stage_copy(
        self,
        source_path: Path,
        *,
        source_system: str,
        dataset_name: str,
        target_file_name: str | None = None,
    ) -> StagingLocation:
        """
        Copy a source file into the canonical staging area.

        We use copy instead of move by default because ingestion usually should
        preserve the original local artifact until the workflow explicitly decides
        otherwise.
        """
        resolved_source = ensure_file_exists(source_path)
        staging_directory = self.prepare_directory(
            source_system=source_system,
            dataset_name=dataset_name,
        )
        destination = staging_directory / (target_file_name or resolved_source.name)

        shutil.copy2(resolved_source, destination)

        return StagingLocation(
            directory=staging_directory,
            file_path=destination,
        )

    def clear_dataset_stage(
        self,
        *,
        source_system: str,
        dataset_name: str,
    ) -> None:
        """
        Remove the staging directory for one dataset if it exists.
        """
        directory = (
            self._base_directory
            / _sanitize_token(source_system)
            / _sanitize_token(dataset_name)
        )

        if directory.exists():
            shutil.rmtree(directory)

    def clear_all(self) -> None:
        """
        Remove the whole local staging area.

        This is useful for test cleanup and controlled local resets.
        """
        if self._base_directory.exists():
            shutil.rmtree(self._base_directory)


def _sanitize_token(value: str) -> str:
    normalized = value.strip().lower()
    normalized = normalized.replace(" ", "_")
    normalized = normalized.replace("-", "_")
    return normalized