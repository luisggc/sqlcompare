from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ColumnPair:
    key: str
    previous_name: str
    current_name: str
    previous_type: str | None = None
    current_type: str | None = None


@dataclass
class StatsContext:
    db: Any
    previous_name: str
    current_name: str
    previous_columns: list[str]
    current_columns: list[str]
    common_columns: list[ColumnPair]
    previous_only_columns: list[str]
    current_only_columns: list[str]
    previous_column_types: dict[str, str | None]
    current_column_types: dict[str, str | None]


@dataclass
class CheckResult:
    name: str
    label: str
    has_differences: bool
    summary: str | None = None
    columns: list[str] = field(default_factory=list)
    rows: list[tuple[Any, ...]] = field(default_factory=list)
    skipped_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CheckDefinition:
    name: str
    label: str
    runner: Any
