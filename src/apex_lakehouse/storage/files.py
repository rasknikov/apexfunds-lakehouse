
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import BinaryIO


DEFAULT_HASH_ALGORITHM = "sha256"
DEFAULT_CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True)
class LocalFileMetadata:
    path: Path
    size_bytes: int
    content_hash: str
    hash_algorithm: str
    detected_at: datetime

    @property
    def file_name(self) -> str:
        return self.path.name
    

def compute_file_hash(
        path: Path,
        *,
        algorithm: str = DEFAULT_HASH_ALGORITHM,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> str:
    hasher = hashlib.new(algorithm)

    with path.open("rb") as file_obj:
        _update_hash_from_stream(
            file_obj,
            hasher=hasher,
            chunk_size=chunk_size,
        )
    
    return hasher.hexdigest()

def collect_local_file_metadata(
        path: Path,
        *,
        algorithm: str = DEFAULT_HASH_ALGORITHM,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> LocalFileMetadata:
    resolved_path = path.resolve()
    start_result = resolved_path.stat()

    return LocalFileMetadata(
        path=resolved_path,
        size_bytes=start_result.st_size,
        content_hash=compute_file_hash(
            resolved_path,
            algorithm=algorithm,
            chunk_size=chunk_size,
        ),
        hash_algorithm=algorithm,
        detected_at=datetime.now(timezone.utc)
    )


def ensure_file_exists(path: Path) -> Path:
    resolved_path = path.resolve()

    if not resolved_path.exists():
        raise FileNotFoundError(f"File does not exist: {resolved_path}")
    
    if not resolved_path.is_file():
        raise IsADirectoryError(f"Path is not a regular file: {resolved_path}")
    
    return resolved_path


def _update_hash_from_stream(
        file_obj: BinaryIO,
        *,
        hasher: hashlib._hashlib.HASH,
        chunk_size: int,
) -> None:
    while True:
        chunk = file_obj.read(chunk_size)
        if not chunk:
            break
        hasher.update(chunk)