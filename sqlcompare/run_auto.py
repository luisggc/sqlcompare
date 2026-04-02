from __future__ import annotations

import uuid

import typer

from sqlcompare.config import get_default_schema
from sqlcompare.db import DBConnection
from sqlcompare.helpers import (
    detect_input,
    materialize_sql_inputs,
    resolve_connection,
    resolve_materialized_tables,
)
from sqlcompare.run_table import compare_table


def run_auto_cmd(
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
        connection = resolve_connection(connection)
        previous_table, new_table = resolve_materialized_tables(
            prev_spec,
            new_spec,
            schema=schema,
            prefix="sqlcompare_sql",
            suffix=uuid.uuid4().hex[:8],
        )

        with DBConnection(connection) as db:
            materialize_sql_inputs(
                db,
                previous_spec=prev_spec,
                current_spec=new_spec,
                previous_table=previous_table,
                current_table=new_table,
                schema=schema,
            )

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
