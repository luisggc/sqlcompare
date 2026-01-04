from __future__ import annotations

import uuid
from pathlib import Path

import typer

from sqlcompare.config import DB_TEST_DB, get_default_schema
from sqlcompare.db import DBConnection
from sqlcompare.helpers import (
    create_table_from_file,
    create_table_from_select,
    ensure_schema,
    get_connector_name,
    load_dataset_config,
    normalize_columns,
    uses_file_only,
    validate_index,
)
from sqlcompare.log import log


def compare_dataset(path: str, connection: str | None, schema: str | None) -> None:
    """
    Compare two datasets defined in a YAML configuration file.

    Datasets can be defined using:
    - file_name: Path to CSV/XLSX file
    - select_sql: SQL SELECT query to generate data

    The YAML file should have 'previous' and 'new' sections,
    each with an 'index' field specifying key columns.
    """
    from sqlcompare.table import compare_table

    dataset_path = Path(path)
    if not dataset_path.exists():
        raise typer.BadParameter(f"Dataset file not found: {path}")

    dataset = load_dataset_config(dataset_path)
    previous = dataset.get("previous")
    new = dataset.get("new")
    if not isinstance(previous, dict) or not isinstance(new, dict):
        raise typer.BadParameter(
            "Dataset config must include previous and new sections."
        )

    prev_conn = get_connector_name(previous)
    new_conn = get_connector_name(new)

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
        if uses_file_only(previous) and uses_file_only(new):
            DB_TEST_DB.parent.mkdir(parents=True, exist_ok=True)
            db_path = DB_TEST_DB.parent / f"dataset_compare_{uuid.uuid4().hex}.duckdb"
            connector = f"duckdb:///{db_path}"
        else:
            raise typer.BadParameter(
                "No connector specified. Use --connection or set SQLCOMPARE_CONN_DEFAULT."
            )

    schema = schema or get_default_schema()

    base = "".join(c if c.isalnum() else "_" for c in dataset_path.stem)
    suffix = uuid.uuid4().hex[:8]
    previous_table = ""
    new_table = ""

    index_cols = validate_index(previous, "previous")
    new_index_cols = validate_index(new, "new")
    if normalize_columns(index_cols) != normalize_columns(new_index_cols):
        raise typer.BadParameter("previous.index and new.index must match.")

    with DBConnection(connector) as db:
        schema_prefix = f"{schema}." if schema else ""

        previous_table = f"{schema_prefix}dataset_{base}_{suffix}_previous"
        new_table = f"{schema_prefix}dataset_{base}_{suffix}_new"

        ensure_schema(db, schema)

        if "file_name" in previous and "select_sql" in previous:
            raise typer.BadParameter(
                "previous cannot include both file_name and select_sql."
            )
        if "file_name" in previous:
            create_table_from_file(db, previous_table, previous["file_name"])
        elif "select_sql" in previous:
            create_table_from_select(db, previous_table, previous["select_sql"])
        else:
            raise typer.BadParameter("previous must include file_name or select_sql.")

        if "file_name" in new and "select_sql" in new:
            raise typer.BadParameter(
                "new cannot include both file_name and select_sql."
            )
        if "file_name" in new:
            create_table_from_file(db, new_table, new["file_name"])
        elif "select_sql" in new:
            create_table_from_select(db, new_table, new["select_sql"])
        else:
            raise typer.BadParameter("new must include file_name or select_sql.")

    log.info(f"Prepared dataset tables: {previous_table}, {new_table}")
    compare_table(previous_table, new_table, ",".join(index_cols), connector, schema)


def dataset_cmd(
    path: str = typer.Argument(..., help="Path to dataset YAML file"),
    connection: str | None = typer.Option(
        None,
        "--connection",
        "-c",
        help="Database connector name (optional when both datasets use file_name)",
    ),
    schema: str | None = typer.Option(
        None, "--schema", help="Schema for dataset tables"
    ),
) -> None:
    """Compare datasets defined in YAML configuration file."""
    compare_dataset(path, connection, schema)
