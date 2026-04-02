from __future__ import annotations

from pathlib import Path
import uuid

import typer

from sqlcompare.helpers import detect_input
from sqlcompare.run_table import compare_table, run_table_cmd
from sqlcompare.run_dataset import run_dataset_cmd
from sqlcompare.run_stats import run_stats_cmd
from sqlcompare.run_auto import _resolve_connection, run_auto_cmd
from sqlcompare.config import get_default_schema
from sqlcompare.db import DBConnection
from sqlcompare.helpers import create_table_from_select, ensure_schema


def _ensure_file_input(value: str, label: str) -> str:
    spec = detect_input(value)
    if spec.kind != "file":
        raise typer.BadParameter(f"{label} must be a CSV/XLSX file path.")
    return spec.value


def run_file_cmd(
    previous: str = typer.Argument(..., help="Previous CSV/XLSX file path"),
    current: str = typer.Argument(..., help="Current CSV/XLSX file path"),
    index: str = typer.Argument(
        ..., help="Comma-separated key column(s), e.g. 'id' or 'user_id,tenant_id'"
    ),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Database connector name (optional for files)"
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
    """Compare two CSV/XLSX files and create a diff run."""
    prev_path = _ensure_file_input(previous, "previous")
    curr_path = _ensure_file_input(current, "current")
    compare_table(
        prev_path,
        curr_path,
        index,
        connection,
        schema,
        include_columns=columns,
        ignore_columns=ignore_columns,
    )


def _load_sql_input(value: str, label: str) -> str:
    spec = detect_input(value)
    if spec.kind != "sql":
        raise typer.BadParameter(f"{label} must be SQL text or a .sql file path.")
    return spec.value


def run_query_cmd(
    previous: str = typer.Option(..., "--previous", help="Previous SQL text or .sql file"),
    current: str = typer.Option(..., "--current", help="Current SQL text or .sql file"),
    index: str = typer.Option(
        ...,
        "--index",
        help="Comma-separated key column(s), e.g. 'id' or 'user_id,tenant_id'",
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
    """Compare two SQL query results and create a diff run."""
    schema = schema or get_default_schema()
    previous_sql = _load_sql_input(previous, "previous")
    current_sql = _load_sql_input(current, "current")
    connection = _resolve_connection(connection)

    schema_prefix = f"{schema}." if schema else ""
    suffix = _suffix_from_paths(previous, current)
    previous_table = f"{schema_prefix}sqlcompare_sql_{suffix}_previous"
    current_table = f"{schema_prefix}sqlcompare_sql_{suffix}_new"

    with DBConnection(connection) as db:
        ensure_schema(db, schema)
        create_table_from_select(db, previous_table, previous_sql)
        create_table_from_select(db, current_table, current_sql)

    compare_table(
        previous_table,
        current_table,
        index,
        connection,
        schema,
        include_columns=columns,
        ignore_columns=ignore_columns,
    )


def _suffix_from_paths(previous: str, current: str) -> str:
    prev_name = Path(previous).stem if Path(previous).exists() else "previous"
    curr_name = Path(current).stem if Path(current).exists() else "current"
    base = f"{prev_name}_{curr_name}"
    safe = "".join(c if c.isalnum() else "_" for c in base)[:20]
    return f"{safe}_{uuid.uuid4().hex[:6]}"


def run_unified_cmd(
    mode_or_previous: str = typer.Argument(
        ..., help="Mode (table|file|query|dataset|stats) or previous input"
    ),
    arg1: str | None = typer.Argument(
        None, help="First input (varies by mode)"
    ),
    arg2: str | None = typer.Argument(
        None, help="Second input (varies by mode)"
    ),
    arg3: str | None = typer.Argument(
        None, help="Third input (varies by mode)"
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
    previous: str | None = typer.Option(
        None, "--previous", help="Previous SQL text or .sql file (query mode)"
    ),
    current: str | None = typer.Option(
        None, "--current", help="Current SQL text or .sql file (query mode)"
    ),
    index: str | None = typer.Option(
        None, "--index", help="Index columns (query mode)"
    ),
    checks: str | None = typer.Option(
        None,
        "--checks",
        help="Comma-separated checks to run for stats mode (default: all checks)",
    ),
) -> None:
    """Run comparisons using a concise run command.

    Examples:
      sqlcompare run table <previous> <current> <index>
      sqlcompare run file <previous.csv> <current.csv> <index>
      sqlcompare run query --previous "<sql>" --current "<sql>" --index id
      sqlcompare run dataset <dataset.yml>
      sqlcompare run stats <previous> <current>
      sqlcompare run <previous> <current> <index>
    """
    mode = mode_or_previous.lower().strip()
    modes = {"table", "file", "query", "dataset", "stats"}

    if mode in modes:
        if mode == "dataset":
            if arg1 is None:
                raise typer.BadParameter("dataset mode requires a dataset path.")
            if arg2 is not None or arg3 is not None:
                raise typer.BadParameter("dataset mode only accepts one argument.")
            run_dataset_cmd(path=arg1, connection=connection, schema=schema)
            return

        if mode == "stats":
            if arg1 is None or arg2 is None:
                raise typer.BadParameter("stats mode requires previous and current inputs.")
            if arg3 is not None:
                raise typer.BadParameter("stats mode only accepts two arguments.")
            run_stats_cmd(
                previous=arg1,
                current=arg2,
                connection=connection,
                checks=checks,
            )
            return

        if mode == "query":
            if previous or current or index:
                if not (previous and current and index):
                    raise typer.BadParameter(
                        "query mode requires --previous, --current, and --index together."
                    )
                run_query_cmd(
                    previous=previous,
                    current=current,
                    index=index,
                    connection=connection,
                    schema=schema,
                    columns=columns,
                    ignore_columns=ignore_columns,
                )
                return
            if arg1 is None or arg2 is None or arg3 is None:
                raise typer.BadParameter(
                    "query mode requires previous, current, and index arguments."
                )
            run_query_cmd(
                previous=arg1,
                current=arg2,
                index=arg3,
                connection=connection,
                schema=schema,
                columns=columns,
                ignore_columns=ignore_columns,
            )
            return

        if arg1 is None or arg2 is None or arg3 is None:
            raise typer.BadParameter(f"{mode} mode requires previous, current, and index.")

        if mode == "table":
            run_table_cmd(
                previous=arg1,
                current=arg2,
                index=arg3,
                connection=connection,
                schema=schema,
                columns=columns,
                ignore_columns=ignore_columns,
            )
            return

        if mode == "file":
            run_file_cmd(
                previous=arg1,
                current=arg2,
                index=arg3,
                connection=connection,
                schema=schema,
                columns=columns,
                ignore_columns=ignore_columns,
            )
            return

    if arg1 is None or arg2 is None:
        raise typer.BadParameter(
            "previous, current, and index are required when no mode is specified."
        )
    if arg3 is not None:
        raise typer.BadParameter(
            "Too many arguments. Use: sqlcompare run <previous> <current> <index>"
        )
    run_auto_cmd(
        previous=mode_or_previous,
        current=arg1,
        index=arg2,
        connection=connection,
        schema=schema,
        columns=columns,
        ignore_columns=ignore_columns,
    )
