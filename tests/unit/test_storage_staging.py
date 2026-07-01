from __future__ import annotations

from pathlib import Path

from apex_lakehouse.storage.staging import LocalStagingManager, StagingLocation


def test_prepare_directory_creates_canonical_dataset_path(tmp_path: Path) -> None:
    manager = LocalStagingManager(tmp_path / "staging")

    directory = manager.prepare_directory(
        source_system="CVM",
        dataset_name="Informe Diario",
    )

    assert directory.exists()
    assert directory == (tmp_path / "staging" / "cvm" / "informe_diario").resolve()


def test_stage_copy_preserves_source_and_returns_location(tmp_path: Path) -> None:
    source_file = tmp_path / "source.zip"
    source_file.write_text("payload", encoding="utf-8")
    manager = LocalStagingManager(tmp_path / "staging")

    location = manager.stage_copy(
        source_file,
        source_system="CVM",
        dataset_name="Informe Diario",
    )

    assert isinstance(location, StagingLocation)
    assert location.directory == (tmp_path / "staging" / "cvm" / "informe_diario").resolve()
    assert location.file_name == "source.zip"
    assert location.file_path.exists()
    assert location.file_path.read_text(encoding="utf-8") == "payload"
    assert source_file.exists()


def test_stage_copy_allows_target_file_name_override(tmp_path: Path) -> None:
    source_file = tmp_path / "source.zip"
    source_file.write_text("payload", encoding="utf-8")
    manager = LocalStagingManager(tmp_path / "staging")

    location = manager.stage_copy(
        source_file,
        source_system="BCB",
        dataset_name="Selic Diario",
        target_file_name="renamed.json",
    )

    assert location.file_name == "renamed.json"
    assert location.file_path.name == "renamed.json"


def test_clear_dataset_stage_removes_only_selected_dataset(tmp_path: Path) -> None:
    manager = LocalStagingManager(tmp_path / "staging")
    first_dir = manager.prepare_directory(source_system="CVM", dataset_name="Informe Diario")
    second_dir = manager.prepare_directory(source_system="BCB", dataset_name="Selic Diario")
    (first_dir / "file.txt").write_text("x", encoding="utf-8")
    (second_dir / "file.txt").write_text("y", encoding="utf-8")

    manager.clear_dataset_stage(source_system="CVM", dataset_name="Informe Diario")

    assert not first_dir.exists()
    assert second_dir.exists()


def test_clear_all_removes_whole_staging_tree(tmp_path: Path) -> None:
    manager = LocalStagingManager(tmp_path / "staging")
    directory = manager.prepare_directory(source_system="CVM", dataset_name="Informe Diario")
    (directory / "file.txt").write_text("x", encoding="utf-8")

    manager.clear_all()

    assert not manager.base_directory.exists()
