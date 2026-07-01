from __future__ import annotations

from datetime import timezone
import hashlib
from pathlib import Path

import pytest

from apex_lakehouse.storage.files import (
    DEFAULT_HASH_ALGORITHM,
    collect_local_file_metadata,
    compute_file_hash,
    ensure_file_exists,
)


def test_compute_file_hash_matches_sha256_digest(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.txt"
    file_path.write_text("apex lakehouse\n", encoding="utf-8")

    digest = compute_file_hash(file_path)

    expected = hashlib.sha256(file_path.read_bytes()).hexdigest()
    assert digest == expected


def test_compute_file_hash_reads_in_small_chunks(tmp_path: Path) -> None:
    file_path = tmp_path / "chunked.bin"
    payload = b"abcdef" * 100
    file_path.write_bytes(payload)

    digest = compute_file_hash(file_path, chunk_size=7)

    assert digest == hashlib.sha256(payload).hexdigest()


def test_collect_local_file_metadata_returns_resolved_metadata(tmp_path: Path) -> None:
    file_path = tmp_path / "metadata.csv"
    file_path.write_text("a,b\n1,2\n", encoding="utf-8")

    metadata = collect_local_file_metadata(file_path)

    assert metadata.path == file_path.resolve()
    assert metadata.file_name == "metadata.csv"
    assert metadata.size_bytes == file_path.stat().st_size
    assert metadata.hash_algorithm == DEFAULT_HASH_ALGORITHM
    assert metadata.content_hash == compute_file_hash(file_path)
    assert metadata.detected_at.tzinfo == timezone.utc


def test_ensure_file_exists_returns_resolved_path(tmp_path: Path) -> None:
    file_path = tmp_path / "artifact.json"
    file_path.write_text("{}", encoding="utf-8")

    resolved = ensure_file_exists(file_path)

    assert resolved == file_path.resolve()


def test_ensure_file_exists_raises_for_missing_file(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.zip"

    with pytest.raises(FileNotFoundError, match="File does not exist"):
        ensure_file_exists(missing_path)


def test_ensure_file_exists_raises_for_directory(tmp_path: Path) -> None:
    directory_path = tmp_path / "a_directory"
    directory_path.mkdir()

    with pytest.raises(IsADirectoryError, match="Path is not a regular file"):
        ensure_file_exists(directory_path)
