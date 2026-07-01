"""Time and business-period utilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


def parse_iso_date(value: str) -> date:
    """Parse a YYYY-MM-DD string into a date object."""
    return date.fromisoformat(value)


def ensure_not_future(value: date, reference: date | None = None) -> None:
    """
    Validate that a date is not in the future.

    Useful for source publication dates, competence dates and data-quality rules.
    """
    today = reference or date.today()
    if value > today:
        raise ValueError(f"Date {value.isoformat()} cannot be in the future.")


@dataclass(frozen=True, order=True)
class Competence:
    """
    Represents a monthly business competence, like 2024-01.

    In financial and regulatory data, 'competence' usually means the reference
    month the data belongs to, not the month it was processed.
    """

    year: int
    month: int

    def __post_init__(self) -> None:
        if self.month < 1 or self.month > 12:
            raise ValueError("Month must be between 1 and 12.")

    @classmethod
    def from_date(cls, value: date) -> "Competence":
        return cls(year=value.year, month=value.month)

    @classmethod
    def from_string(cls, value: str) -> "Competence":
        """
        Accepts values in the form YYYY-MM.
        """
        year_str, month_str = value.split("-", maxsplit=1)
        return cls(year=int(year_str), month=int(month_str))

    def to_date(self) -> date:
        """Return the first day of the competence month."""
        return date(self.year, self.month, 1)

    def to_partition(self) -> str:
        """Return a stable partition label like ano=2024/mes=01."""
        return f"ano={self.year:04d}/mes={self.month:02d}"

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def utc_now() -> datetime:
    """Return current UTC timestamp."""
    return datetime.utcnow()