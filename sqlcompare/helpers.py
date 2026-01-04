"""Shared helper utilities for SQL compare operations."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import typer
import yaml

from sqlcompare.db import DBConnection
from sqlcompare.log import log


# Dataset helpers

def expand_dataset_value(value: Any, base_dir: Path) -> Any:
    """
    Recursively expand template variables in dataset configuration values.

    Replaces {{here}} with the dataset file's parent directory path.
    """
    if isinstance(value, dict):
        return {
            key: expand_dataset_value(item, base_dir) for key, item in value.items()
        }
    if isinstance(value, list):
        return [expand_dataset_value(item, base_dir) for item in value]
    if isinstance(value, str):
        return value.replace("{{here}}", str(base_dir))
    return value


def load_dataset_config(path: Path) -> dict[str, Any]:
    """Load and validate dataset configuration from YAML file."""
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise typer.BadParameter("Dataset config must be a YAML mapping.")
    return expand_dataset_value(payload, path.parent)


def get_connector_name(section: dict[str, Any]) -> str | None:
    """Extract connector name from dataset section (supports 'conn' or 'connection' keys)."""
    return section.get("conn") or section.get("connection")


def validate_index(section: dict[str, Any], label: str) -> list[str]:
    """
    Validate that dataset section contains valid index column specification.

    Raises BadParameter if index is missing or invalid.
    """
    index_cols = section.get("index")
    if not index_cols:
        raise typer.BadParameter(f"Dataset config is missing {label}.index.")
    if not isinstance(index_cols, list) or not all(
        isinstance(item, str) for item in index_cols
    ):
        raise typer.BadParameter(f"{label}.index must be a list of column names.")
    return index_cols


def normalize_columns(columns: list[str]) -> list[str]:
    """Normalize column names to uppercase with whitespace stripped."""
    return [col.strip().upper() for col in columns]


def ensure_schema(db: DBConnection, schema: str) -> None:
    """Create schema in database if it doesn't exist."""
    if not schema:
        return
    db.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def create_table_from_file(db: DBConnection, table: str, file_path: str) -> None:
    """
    Create database table from CSV or XLSX file.

    Wraps DBConnection.create_table_from_file with better error handling.
    """
    try:
        db.create_table_from_file(table, file_path)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


def create_table_from_select(db: DBConnection, table: str, select_sql: str) -> None:
    """Create database table from SELECT query."""
    query = select_sql.strip().rstrip(";")
    db.execute(f"CREATE TABLE {table} AS {query}")


def uses_file_only(section: dict[str, Any]) -> bool:
    """Check if dataset section uses file_name without select_sql."""
    return "file_name" in section and "select_sql" not in section


# Query helpers

def resolve_output_filename(output: str, query: str) -> str | None:
    """
    Resolve output filename for query results.

    - Returns None if XLSX requested (not supported)
    - Returns output as-is if it ends with .csv
    - Generates CSV filename from query if output has no extension
    - Returns None for unsupported extensions
    """
    if output.lower().endswith(".xlsx"):
        log.error("❌ XLSX output is not supported without pandas. Use CSV instead.")
        return None

    if output.lower().endswith(".csv"):
        return output

    match = re.search(r"(?:SELECT\s+)(\w+)|(?:FROM\s+)(\w+)", query, re.IGNORECASE)
    if match:
        query_identifier = next((m for m in match.groups() if m), "query_result")
    else:
        query_identifier = "query_result"

    if "." in output:
        log.error("❌ Only CSV output is supported without pandas.")
        return None

    return f"{query_identifier}.csv"
