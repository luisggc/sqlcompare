from __future__ import annotations

import os
import uuid

import typer

from sqlcompare.config import get_default_schema
from sqlcompare.db import DBConnection
from sqlcompare.helpers import create_table_from_select, detect_input, ensure_schema
from sqlcompare.table import compare_table


def _resolve_connection(connection: str | None) -> str:
    if connection:
        return connection
    default_conn = os.getenv("SQLCOMPARE_CONN_DEFAULT") or os.getenv("DTK_CONN_DEFAULT")
    if not default_conn:
        raise typer.BadParameter(
            "No connection specified. Use --connection or set SQLCOMPARE_CONN_DEFAULT."
        )
    return default_conn


def run_cmd(
    previous: str = typer.Argument(
        ..., help="Previous table name, CSV/XLSX file path, or SQL"
    ),
    current: str = typer.Argument(
        ..., help="Current table name, CSV/XLSX file path, or SQL"
    ),
    index: str = typer.Argument(
        ..., help="Comma-separated key column(s), e.g. 'id' or 'user_id,tenant_id'"
    ),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Database connector name"
    ),
    schema: str | None = typer.Option(None, "--schema", help="Schema for test tables"),
    columns: str | None = typer.Option(
        None,
        "--columns",
        help="Comma-separated non-index columns to compare (default: all common columns)",
    ),
    ignore_columns: str | None = typer.Option(
        None,
        "--ignore-columns",
        help="Comma-separated non-index columns to skip from comparison",
    ),
) -> None:
    """Run a comparison from tables, files, or SQL text.

    This command auto-detects inputs:
      - CSV/XLSX paths are treated as files
      - .sql paths are read as SQL text
      - Inline SQL starting with SELECT/WITH is treated as SQL
      - Otherwise, inputs are treated as table names

    Examples:
        # Tables
        sqlcompare run analytics.users analytics.users_new id

        # SQL inline
        sqlcompare run "SELECT * FROM previous" "SELECT * FROM current" id -c duckdb_test

        # SQL files
        sqlcompare run queries/previous.sql queries/current.sql id -c snowflake_prod

        # Files (uses DuckDB)
        sqlcompare run exports/prev.csv exports/current.csv customer_id
    """
    schema = schema or get_default_schema()

    prev_spec = detect_input(previous)
    new_spec = detect_input(current)

    if prev_spec.kind == "file" or new_spec.kind == "file":
        if prev_spec.kind != "file" or new_spec.kind != "file":
            raise typer.BadParameter(
                "Both table arguments must be file paths when using CSV/XLSX inputs."
            )
        compare_table(
            prev_spec.value,
            new_spec.value,
            index,
            connection,
            schema,
            include_columns=columns,
            ignore_columns=ignore_columns,
        )
        return

    if prev_spec.kind == "sql" or new_spec.kind == "sql":
        connection = _resolve_connection(connection)
        schema_prefix = f"{schema}." if schema else ""
        suffix = uuid.uuid4().hex[:8]
        previous_table = (
            prev_spec.value
            if prev_spec.kind == "table"
            else f"{schema_prefix}sqlcompare_sql_{suffix}_previous"
        )
        new_table = (
            new_spec.value
            if new_spec.kind == "table"
            else f"{schema_prefix}sqlcompare_sql_{suffix}_new"
        )

        with DBConnection(connection) as db:
            ensure_schema(db, schema)
            if prev_spec.kind == "sql":
                create_table_from_select(db, previous_table, prev_spec.value)
            if new_spec.kind == "sql":
                create_table_from_select(db, new_table, new_spec.value)

        compare_table(
            previous_table,
            new_table,
            index,
            connection,
            schema,
            include_columns=columns,
            ignore_columns=ignore_columns,
        )
        return

    compare_table(
        previous,
        current,
        index,
        connection,
        schema,
        include_columns=columns,
        ignore_columns=ignore_columns,
    )
