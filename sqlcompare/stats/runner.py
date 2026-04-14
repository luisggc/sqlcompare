from __future__ import annotations

from dataclasses import replace
import uuid
from pathlib import Path

import typer

from sqlcompare.config import get_default_schema
from sqlcompare.db import DBConnection
from sqlcompare.helpers import (
    detect_input,
    materialize_sql_inputs,
    resolve_connection,
    resolve_materialized_tables,
)
from sqlcompare.log import log
from sqlcompare.stats.checks import get_check_map
from sqlcompare.stats.models import ColumnPair, StatsContext
from sqlcompare.stats.report import render_report
from sqlcompare.utils.concurrency import run_ordered


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
    previous_column_types = db.get_table_column_types(previous_name)
    current_column_types = db.get_table_column_types(current_name)
    previous_map = {column.upper(): column for column in previous_columns}
    current_map = {column.upper(): column for column in current_columns}

    common_columns = [
        ColumnPair(
            key=key,
            previous_name=previous_map[key],
            current_name=current_map[key],
            previous_type=previous_column_types.get(previous_map[key]),
            current_type=current_column_types.get(current_map[key]),
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
        previous_column_types=previous_column_types,
        current_column_types=current_column_types,
    )


def _run_selected_checks(
    context: StatsContext,
    selected_check_names: list[str],
    check_map: dict[str, object],
    connection_id: str | None,
    parallel_safe: bool,
) -> list:
    def run_check(name: str):
        definition = check_map[name]
        if not parallel_safe:
            return definition.runner(context, definition)

        with DBConnection(connection_id) as db:
            return definition.runner(replace(context, db=db), definition)

    return run_ordered(
        selected_check_names,
        run_check,
        enabled=parallel_safe,
        max_workers=4,
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
        connection_id = resolve_connection(connection, error_cls=ValueError)
        schema = get_default_schema()
        suffix = uuid.uuid4().hex[:8]
        table1_name, table2_name = resolve_materialized_tables(
            spec_prev,
            spec_new,
            schema=schema,
            prefix="sqlcompare_stats",
            suffix=suffix,
        )
    else:
        table1_name = spec_prev.value
        table2_name = spec_new.value

    parallel_safe = not (
        spec_prev.kind == "file" and spec_new.kind == "file" and connection is None
    )

    def prepare_inputs(db: DBConnection) -> None:
        if spec_prev.kind == "file" and spec_new.kind == "file":
            db.create_table_from_file(table1_name, spec_prev.value)
            db.create_table_from_file(table2_name, spec_new.value)
        materialize_sql_inputs(
            db,
            previous_spec=spec_prev,
            current_spec=spec_new,
            previous_table=table1_name,
            current_table=table2_name,
            schema=get_default_schema(),
        )

    if not parallel_safe:
        with DBConnection(connection_id) as db:
            prepare_inputs(db)
            context = _build_context(db, table1_name, table2_name)
            check_map = get_check_map()
            results = _run_selected_checks(
                context=context,
                selected_check_names=selected_check_names,
                check_map=check_map,
                connection_id=connection_id,
                parallel_safe=False,
            )
    else:
        with DBConnection(connection_id) as db:
            prepare_inputs(db)

        with DBConnection(connection_id) as db:
            context = _build_context(db, table1_name, table2_name)
            check_map = get_check_map()
            results = _run_selected_checks(
                context=context,
                selected_check_names=selected_check_names,
                check_map=check_map,
                connection_id=connection_id,
                parallel_safe=True,
            )

    log.info(render_report(context, selected_check_names, results))
