from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence


@dataclass(frozen=True)
class ColumnContract:
    name: str
    data_type: str
    nullable: bool = True
    description: str = ""
    is_primary_key: bool = False


@dataclass(frozen=True)
class TableContract:
    schema_name: str
    table_name: str
    description: str
    columns: Sequence[ColumnContract]
    primary_key: Sequence[str] = field(default_factory=tuple)
    unique_constraints: Sequence[Sequence[str]] = field(default_factory=tuple)
    indexes: Sequence[Sequence[str]] = field(default_factory=tuple)

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}"