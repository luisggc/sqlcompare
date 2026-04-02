from __future__ import annotations

import os
import uuid
from pathlib import Path

import typer

from sqlcompare.config import get_default_schema
from sqlcompare.db import DBConnection
from sqlcompare.helpers import create_table_from_select, detect_input, ensure_schema
from sqlcompare.log import log
from sqlcompare.stats.checks import get_check_map
from sqlcompare.stats.models import ColumnPair, StatsContext
from sqlcompare.stats.report import render_report


def _resolve_connection(connection: str | None) -> str:
    if connection:
        return connection
    default_conn = os.getenv("SQLCOMPARE_CONN_DEFAULT") or os.getenv("DTK_CONN_DEFAULT")
    if not default_conn:
        raise ValueError(
            "No connection specified. Use --connection or set SQLCOMPARE_CONN_DEFAULT."
        )
    return default_conn


def _resolve_checks(checks: str | None) -> list[str]:
    check_map = get_check_map()
    if not checks:
        return list(check_map.keys())

    selected: list[str] = []
    invalid: list[str] = []
    seen: set[str] = set()
    for raw_value in checks.split(","):
        name = raw_value.strip().lower()
        if not name:
            continue
        if name not in check_map:
            invalid.append(raw_value.strip())
            continue
        if name in seen:
            continue
        seen.add(name)
        selected.append(name)

    if invalid:
        raise typer.BadParameter(
            "Unknown check name(s): "
            + ", ".join(invalid)
            + ". Valid checks: "
            + ", ".join(check_map.keys())
        )
    if not selected:
        raise typer.BadParameter(
            "At least one check must be provided. Valid checks: "
            + ", ".join(check_map.keys())
        )
    return selected


def _build_context(db: DBConnection, previous_name: str, current_name: str) -> StatsContext:
    previous_columns = db.get_table_columns(previous_name)
    current_columns = db.get_table_columns(current_name)
    previous_map = {column.upper(): column for column in previous_columns}
    current_map = {column.upper(): column for column in current_columns}

    common_columns = [
        ColumnPair(
            key=key,
            previous_name=previous_map[key],
            current_name=current_map[key],
        )
        for key in previous_map
        if key in current_map
    ]
    previous_only_columns = [
        previous_map[key] for key in previous_map if key not in current_map
    ]
    current_only_columns = [
        current_map[key] for key in current_map if key not in previous_map
    ]

    return StatsContext(
        db=db,
        previous_name=previous_name,
        current_name=current_name,
        previous_columns=previous_columns,
        current_columns=current_columns,
        common_columns=common_columns,
        previous_only_columns=previous_only_columns,
        current_only_columns=current_only_columns,
    )


def compare_table_stats(
    table1: str,
    table2: str,
    connection: str | None,
    checks: str | None = None,
) -> None:
    selected_check_names = _resolve_checks(checks)
    table1_name = table1
    table2_name = table2
    connection_id = connection

    spec_prev = detect_input(table1)
    spec_new = detect_input(table2)

    if spec_prev.kind == "file" or spec_new.kind == "file":
        if spec_prev.kind != "file" or spec_new.kind != "file":
            raise ValueError(
                "Both table arguments must be file paths when using CSV/XLSX inputs."
            )
        if connection is None:
            connection_id = "duckdb:///:memory:"
        table1_name = Path(spec_prev.value).stem
        table2_name = Path(spec_new.value).stem
    elif spec_prev.kind == "sql" or spec_new.kind == "sql":
        connection_id = _resolve_connection(connection)
        schema = get_default_schema()
        schema_prefix = f"{schema}." if schema else ""
        suffix = uuid.uuid4().hex[:8]
        table1_name = (
            spec_prev.value
            if spec_prev.kind == "table"
            else f"{schema_prefix}sqlcompare_stats_{suffix}_previous"
        )
        table2_name = (
            spec_new.value
            if spec_new.kind == "table"
            else f"{schema_prefix}sqlcompare_stats_{suffix}_new"
        )
    else:
        table1_name = spec_prev.value
        table2_name = spec_new.value

    with DBConnection(connection_id) as db:
        if spec_prev.kind == "file" and spec_new.kind == "file":
            if connection is None and connection_id == "duckdb:///:memory:":
                db.create_table_from_file(table1_name, spec_prev.value)
                db.create_table_from_file(table2_name, spec_new.value)
        if spec_prev.kind == "sql" or spec_new.kind == "sql":
            schema = get_default_schema()
            ensure_schema(db, schema)
            if spec_prev.kind == "sql":
                create_table_from_select(db, table1_name, spec_prev.value)
            if spec_new.kind == "sql":
                create_table_from_select(db, table2_name, spec_new.value)

        context = _build_context(db, table1_name, table2_name)
        check_map = get_check_map()
        results = [
            check_map[name].runner(context, check_map[name])
            for name in selected_check_names
        ]

    log.info(render_report(context, selected_check_names, results))
