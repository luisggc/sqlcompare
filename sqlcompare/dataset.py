from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import typer
import yaml

from sqlcompare.compare.table import compare_table
from sqlcompare.config import (
    get_default_schema,
)
from sqlcompare.db import DBConnection
from sqlcompare.log import log


def _expand_dataset_value(value: Any, base_dir: Path) -> Any:
    if isinstance(value, dict):
        return {
            key: _expand_dataset_value(item, base_dir) for key, item in value.items()
        }
    if isinstance(value, list):
        return [_expand_dataset_value(item, base_dir) for item in value]
    if isinstance(value, str):
        return value.replace("{{here}}", str(base_dir))
    return value


def _load_dataset_config(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise typer.BadParameter("Dataset config must be a YAML mapping.")
    return _expand_dataset_value(payload, path.parent)


def _get_connector_name(section: dict[str, Any]) -> str | None:
    return section.get("conn") or section.get("connection")


def _validate_index(section: dict[str, Any], label: str) -> list[str]:
    index_cols = section.get("index")
    if not index_cols:
        raise typer.BadParameter(f"Dataset config is missing {label}.index.")
    if not isinstance(index_cols, list) or not all(
        isinstance(item, str) for item in index_cols
    ):
        raise typer.BadParameter(f"{label}.index must be a list of column names.")
    return index_cols


def _normalize_columns(columns: list[str]) -> list[str]:
    return [col.strip().upper() for col in columns]


def _ensure_schema(db: DBConnection, schema: str) -> None:
    if not schema:
        return
    db.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def _create_table_from_file(
    db: DBConnection, table: str, file_path: str
) -> None:
    try:
        db.create_table_from_file(table, file_path)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _create_table_from_select(db: DBConnection, table: str, select_sql: str) -> None:
    query = select_sql.strip().rstrip(";")
    db.execute(f"CREATE TABLE {table} AS {query}")


def compare_dataset(path: str, connection: str | None, schema: str | None) -> None:
    dataset_path = Path(path)
    if not dataset_path.exists():
        raise typer.BadParameter(f"Dataset file not found: {path}")

    dataset = _load_dataset_config(dataset_path)
    previous = dataset.get("previous")
    new = dataset.get("new")
    if not isinstance(previous, dict) or not isinstance(new, dict):
        raise typer.BadParameter(
            "Dataset config must include previous and new sections."
        )

    prev_conn = _get_connector_name(previous)
    new_conn = _get_connector_name(new)

    if connection:
        if prev_conn and prev_conn != connection:
            raise typer.BadParameter(
                "Dataset connector for previous does not match --connection."
            )
        if new_conn and new_conn != connection:
            raise typer.BadParameter(
                "Dataset connector for new does not match --connection."
            )
        connector = connection
    else:
        if prev_conn and new_conn and prev_conn != new_conn:
            raise typer.BadParameter(
                "Previous and new dataset entries must use the same connector."
            )
        connector = prev_conn or new_conn

    if not connector:
        raise typer.BadParameter(
            "No connector specified. Use --connection or set SQLCOMPARE_CONN_DEFAULT."
        )

    schema = schema or get_default_schema()

    base = "".join(c if c.isalnum() else "_" for c in dataset_path.stem)
    suffix = uuid.uuid4().hex[:8]
    previous_table = ""
    new_table = ""

    index_cols = _validate_index(previous, "previous")
    new_index_cols = _validate_index(new, "new")
    if _normalize_columns(index_cols) != _normalize_columns(new_index_cols):
        raise typer.BadParameter("previous.index and new.index must match.")

    with DBConnection(connector) as db:
        schema_prefix = f"{schema}." if schema else ""

        previous_table = f"{schema_prefix}dataset_{base}_{suffix}_previous"
        new_table = f"{schema_prefix}dataset_{base}_{suffix}_new"

        _ensure_schema(db, schema)

        if "file_name" in previous and "select_sql" in previous:
            raise typer.BadParameter(
                "previous cannot include both file_name and select_sql."
            )
        if "file_name" in previous:
            _create_table_from_file(
                db, previous_table, previous["file_name"]
            )
        elif "select_sql" in previous:
            _create_table_from_select(db, previous_table, previous["select_sql"])
        else:
            raise typer.BadParameter("previous must include file_name or select_sql.")

        if "file_name" in new and "select_sql" in new:
            raise typer.BadParameter(
                "new cannot include both file_name and select_sql."
            )
        if "file_name" in new:
            _create_table_from_file(db, new_table, new["file_name"])
        elif "select_sql" in new:
            _create_table_from_select(db, new_table, new["select_sql"])
        else:
            raise typer.BadParameter("new must include file_name or select_sql.")

    log.info(f"Prepared dataset tables: {previous_table}, {new_table}")
    compare_table(previous_table, new_table, ",".join(index_cols), connector, schema)
