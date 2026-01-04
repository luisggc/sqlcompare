from __future__ import annotations

import os

import typer

from sqlcompare.analysis.utils import (
    _display,
    find_diff_file,
    find_diff_run,
    list_available_diffs,
)
from sqlcompare.compare.comparator import DatabaseComparator
from sqlcompare.db import DBConnection
from sqlcompare.log import log


def analyze_diff(
    diff_id: str,
    column: str | None = None,
    limit: int = 25,
    save: bool = False,
    list_columns: bool = False,
    stats: bool = False,
    missing_current: bool = False,
    missing_previous: bool = False,
) -> None:
    """
    Analyze diff results from a previous comparison run.

    Supports database-backed diffs and legacy pickle files.
    """
    run = find_diff_run(diff_id)
    if run:
        tables = run["tables"]
        idx = run.get("index_cols", [])
        cols_prev = run.get("cols_prev", [])
        cols_new = run.get("cols_new", [])
        if "duckdb_file" in run:
            db_file = run["duckdb_file"]
            comp = DatabaseComparator.from_saved(
                db_file, tables, idx, cols_prev, cols_new
            )
            # Use DBConnection with duckdb URL
            conn_url = f"duckdb:///{db_file}"
            with DBConnection(conn_url) as db:
                if stats:
                    rows, columns = db.query(
                        comp.get_stats_query(column=column), include_columns=True
                    )
                    _display(
                        columns,
                        rows,
                        column,
                        limit,
                        save,
                        diff_id,
                        is_stats=True,
                        list_columns=list_columns,
                    )
                    return
                if missing_current:
                    rows, columns = db.query(
                        comp.get_in_current_only_query(), include_columns=True
                    )
                    _display(
                        columns,
                        rows,
                        column,
                        limit,
                        save,
                        diff_id,
                        list_columns=list_columns,
                    )
                    return
                if missing_previous:
                    rows, columns = db.query(
                        comp.get_in_previous_only_query(), include_columns=True
                    )
                    _display(
                        columns,
                        rows,
                        column,
                        limit,
                        save,
                        diff_id,
                        list_columns=list_columns,
                    )
                    return

                # Use limit in SQL only if we are NOT saving and NOT listing columns
                sql_limit = limit if not save and not list_columns else None
                rows, columns = db.query(
                    comp.get_diff_query(column=column, limit=sql_limit),
                    include_columns=True,
                )
                _display(
                    columns,
                    rows,
                    column,
                    limit,
                    save,
                    diff_id,
                    list_columns=list_columns,
                )
            return
        conn = run["conn"]
        comp = DatabaseComparator.from_saved(conn, tables, idx, cols_prev, cols_new)
        with DBConnection(conn) as db:
            if stats:
                q = comp.get_stats_query(column=column)
                rows, columns = db.query(q, include_columns=True)
                _display(
                    columns,
                    rows,
                    column,
                    limit,
                    save,
                    diff_id,
                    is_stats=True,
                    list_columns=list_columns,
                )
                return
            if missing_current:
                rows, columns = db.query(
                    comp.get_in_current_only_query(), include_columns=True
                )
                _display(
                    columns,
                    rows,
                    column,
                    limit,
                    save,
                    diff_id,
                    list_columns=list_columns,
                )
                return
            if missing_previous:
                rows, columns = db.query(
                    comp.get_in_previous_only_query(), include_columns=True
                )
                _display(
                    columns,
                    rows,
                    column,
                    limit,
                    save,
                    diff_id,
                    list_columns=list_columns,
                )
                return

            # Use limit in SQL only if we are NOT saving and NOT listing columns
            sql_limit = limit if not save and not list_columns else None
            rows, columns = db.query(
                comp.get_diff_query(column=column, limit=sql_limit),
                include_columns=True,
            )
            _display(
                columns,
                rows,
                column,
                limit,
                save,
                diff_id,
                list_columns=list_columns,
            )
        return

    diff_file = find_diff_file(diff_id)
    if not diff_file:
        log.error(f"❌ Diff data with ID '{diff_id}' not found.")
        log.info("💡 Available diff IDs:")
        list_available_diffs()
        return

    if stats:
        stats_file = diff_file.replace(".pkl", "_stats.pkl")
        if not os.path.exists(stats_file):
            log.error(f"❌ Statistics file not found: {stats_file}")
            return
        log.error("❌ Pickle-based stats files are not supported without pandas.")
        return

    if missing_current:
        current_only_file = diff_file.replace(".pkl", "_in_current_only.pkl")
        if not os.path.exists(current_only_file):
            log.info(f"✅ No rows found that exist only in current dataset")
            return
        log.error("❌ Pickle-based diff files are not supported without pandas.")
        return

    if missing_previous:
        previous_only_file = diff_file.replace(".pkl", "_in_previous_only.pkl")
        if not os.path.exists(previous_only_file):
            log.info(f"✅ No rows found that exist only in previous dataset")
            return
        log.error("❌ Pickle-based diff files are not supported without pandas.")
        return

    log.error("❌ Pickle-based diff files are not supported without pandas.")


def analyze_diff_cmd(
    diff_id: str = typer.Argument(..., help="Diff run ID"),
    column: str | None = typer.Option(
        None, "--column", "-c", help="Filter by specific column name"
    ),
    limit: int = typer.Option(25, "--limit", "-l", help="Limit results to display"),
    save: bool = typer.Option(
        False, "--save", help="Save filtered results to CSV file"
    ),
    list_columns: bool = typer.Option(
        False, "--list-columns", help="List available columns in the diff data"
    ),
    stats: bool = typer.Option(
        False, "--stats", help="Show statistics table instead of differences"
    ),
    missing_current: bool = typer.Option(
        False, "--missing-current", help="Show rows only in current dataset"
    ),
    missing_previous: bool = typer.Option(
        False, "--missing-previous", help="Show rows only in previous dataset"
    ),
) -> None:
    """Analyze diff results from a previous comparison run."""
    analyze_diff(
        diff_id,
        column=column,
        limit=limit,
        save=save,
        list_columns=list_columns,
        stats=stats,
        missing_current=missing_current,
        missing_previous=missing_previous,
    )
