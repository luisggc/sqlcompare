from __future__ import annotations

import os
from typing import Any

import typer

from sqlcompare.analysis.utils import (
    SUMMARY_TAB_LIMIT,
    _display,
    find_diff_file,
    find_diff_run,
    list_available_diffs,
    resolve_report_path,
    write_inspect_report_xlsx,
)
from sqlcompare.compare.comparator import DatabaseComparator
from sqlcompare.db import DBConnection
from sqlcompare.log import log

SAVE_MODES = {"none", "summary", "complete"}


def review_diff(
    diff_id: str,
    column: str | None = None,
    limit: int = 25,
    save: str = "none",
    file_path: str | None = None,
    list_columns: bool = False,
    stats: bool = False,
    missing_current: bool = False,
    missing_previous: bool = False,
) -> None:
    """
    Review diff results from a previous comparison run.

    Supports database-backed diffs and pickle files.
    """
    save_mode = _normalize_save_mode(save)
    if save_mode != "none" and (
        stats or missing_current or missing_previous or list_columns
    ):
        raise typer.BadParameter(
            "Report export (--save summary/complete) only supports standard diff view. "
            "Do not combine with --stats, --missing-current, --missing-previous, or --list-columns."
        )

    run = find_diff_run(diff_id)
    if run:
        tables = run["tables"]
        idx = run.get("index_cols", [])
        cols_prev = run.get("cols_prev", [])
        cols_new = run.get("cols_new", [])
        common_cols = run.get("common_cols")

        if "duckdb_file" in run:
            db_file = run["duckdb_file"]
            comp = DatabaseComparator.from_saved(
                db_file, tables, idx, cols_prev, cols_new, common_cols
            )
            conn_url = f"duckdb:///{db_file}"
            with DBConnection(conn_url) as db:
                _inspect_db_run(
                    db=db,
                    comp=comp,
                    diff_id=diff_id,
                    column=column,
                    limit=limit,
                    save_mode=save_mode,
                    file_path=file_path,
                    list_columns=list_columns,
                    stats=stats,
                    missing_current=missing_current,
                    missing_previous=missing_previous,
                )
            return

        conn = run.get("conn")
        comp = DatabaseComparator.from_saved(
            conn, tables, idx, cols_prev, cols_new, common_cols
        )
        with DBConnection(conn) as db:
            _inspect_db_run(
                db=db,
                comp=comp,
                diff_id=diff_id,
                column=column,
                limit=limit,
                save_mode=save_mode,
                file_path=file_path,
                list_columns=list_columns,
                stats=stats,
                missing_current=missing_current,
                missing_previous=missing_previous,
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

    if save_mode != "none":
        log.error(
            "❌ Report export is only supported for database-backed diff runs (not pickle diffs)."
        )
        return

    log.error("❌ Pickle-based diff files are not supported without pandas.")


def review_cmd(
    diff_id: str = typer.Argument(..., help="Diff run ID"),
    column: str | None = typer.Option(
        None, "--column", "-c", help="Filter by specific column name"
    ),
    limit: int = typer.Option(25, "--limit", "-l", help="Limit results to display"),
    save: str = typer.Option(
        "none",
        "--save",
        help="Report mode: none, summary, complete",
    ),
    file_path: str | None = typer.Option(
        None,
        "--file-path",
        help="Output path for the saved report (.xlsx). Defaults to current directory.",
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
    """Review results from a previous comparison.

    After running 'table' or 'dataset' comparison, use this command to analyze
    the saved diff results. You can view statistics, filter by column, examine
    missing rows, or export results.

    Examples:
        # View diff summary with statistics
        sqlcompare review stats <diff_id>

        # Filter differences for specific column
        sqlcompare review <diff_id> --column revenue

        # Show rows only in current dataset
        sqlcompare review missing <diff_id> --side current

        # Show rows only in previous dataset
        sqlcompare review missing <diff_id> --side previous

        # List all available columns
        sqlcompare review columns <diff_id>

        # Show more results
        sqlcompare review <diff_id> --limit 100

        # Save summary workbook report
        sqlcompare review export <diff_id> --mode summary

        # Save complete workbook report to a custom path
        sqlcompare review export <diff_id> --mode complete --output /tmp/my_report.xlsx
    """
    review_diff(
        diff_id,
        column=column,
        limit=limit,
        save=save,
        file_path=file_path,
        list_columns=list_columns,
        stats=stats,
        missing_current=missing_current,
        missing_previous=missing_previous,
    )


def _normalize_save_mode(save: str) -> str:
    save_mode = save.lower().strip()
    if save_mode not in SAVE_MODES:
        options = ", ".join(sorted(SAVE_MODES))
        raise typer.BadParameter(f"Invalid save mode '{save}'. Use one of: {options}.")
    return save_mode


def _inspect_db_run(
    db: DBConnection,
    comp: DatabaseComparator,
    diff_id: str,
    column: str | None,
    limit: int,
    save_mode: str,
    file_path: str | None,
    list_columns: bool,
    stats: bool,
    missing_current: bool,
    missing_previous: bool,
) -> None:
    if stats:
        rows, columns = db.query(comp.get_stats_query(column=column), include_columns=True)
        _display(
            columns,
            rows,
            column,
            limit,
            diff_id,
            is_stats=True,
            list_columns=list_columns,
        )
        return

    if missing_current:
        rows, columns = db.query(comp.get_in_current_only_query(), include_columns=True)
        _display(columns, rows, column, limit, diff_id, list_columns=list_columns)
        return

    if missing_previous:
        rows, columns = db.query(comp.get_in_previous_only_query(), include_columns=True)
        _display(columns, rows, column, limit, diff_id, list_columns=list_columns)
        return

    if save_mode == "none":
        total_differences: int | None = None
        if not list_columns:
            count_rows = db.query(comp.get_diff_count_query(column=column))
            count_value = count_rows[0][0] if count_rows and count_rows[0] else 0
            total_differences = int(count_value or 0)
        sql_limit = limit if not list_columns else None
        rows, columns = db.query(
            comp.get_diff_query(column=column, limit=sql_limit),
            include_columns=True,
        )
        _display(
            columns,
            rows,
            column,
            limit,
            diff_id,
            list_columns=list_columns,
            total_differences=total_differences,
        )

    if save_mode != "none":
        _save_inspect_report(
            db=db,
            comp=comp,
            diff_id=diff_id,
            save_mode=save_mode,
            column=column,
            file_path=file_path,
        )


def _save_inspect_report(
    db: DBConnection,
    comp: DatabaseComparator,
    diff_id: str,
    save_mode: str,
    column: str | None,
    file_path: str | None,
) -> None:
    def _count_rows_for_query(query: str) -> int:
        count_rows = db.query(f"SELECT COUNT(*) AS row_count FROM ({query}) AS src")
        count_value = count_rows[0][0] if count_rows and count_rows[0] else 0
        return int(count_value or 0)

    stats_query = comp.get_stats_query(column=column)
    stats_rows, _ = db.query(stats_query, include_columns=True)
    changed_stats = [
        (str(row[0]), int(row[1]))
        for row in stats_rows
        if row and row[0] is not None and int(row[1]) > 0
    ]

    total_diff_count = sum(count for _, count in changed_stats)
    changed_columns = [col_name for col_name, _ in changed_stats]
    in_current_only_count = _count_rows_for_query(comp.get_in_current_only_query())
    in_previous_only_count = _count_rows_for_query(comp.get_in_previous_only_query())

    overview_rows: list[tuple[Any, Any, Any]] = [
        ("Summary", "Diff ID", diff_id),
        ("Summary", "Mode", save_mode),
        ("Summary", "Filtered Column", column or "(all)"),
        ("Summary", "Total Differences", total_diff_count),
        ("Summary", "Changed Columns", len(changed_columns)),
        ("Summary", "Rows Only In Current", in_current_only_count),
        ("Summary", "Rows Only In Previous", in_previous_only_count),
    ]
    for col_name, count in changed_stats:
        overview_rows.append(("Column Diff Count", col_name, count))

    sql_reference_rows: list[tuple[str, str, str]] = [
        ("overview", "stats_query", stats_query),
        ("debug", "combined_diff_query", comp.get_diff_query(column=column, limit=None)),
        ("debug", "in_current_only_query", comp.get_in_current_only_query()),
        ("debug", "in_previous_only_query", comp.get_in_previous_only_query()),
    ]

    column_diffs: dict[str, tuple[list[str], list[tuple[Any, ...]]]] = {}
    per_column_limit = SUMMARY_TAB_LIMIT if save_mode == "summary" else None
    for col_name in changed_columns:
        query = comp.get_column_diff_query(col_name)
        if per_column_limit is not None:
            query = f"{query} LIMIT {per_column_limit}"
        rows, columns = db.query(query, include_columns=True)
        column_diffs[col_name] = (columns, rows)
        sql_reference_rows.append(("column_tab", col_name, query))

    report_path = resolve_report_path(diff_id=diff_id, save_mode=save_mode, file_path=file_path)
    write_result = write_inspect_report_xlsx(
        output_path=report_path,
        overview_rows=overview_rows,
        column_diffs=column_diffs,
        sql_reference_rows=sql_reference_rows,
    )
    truncated_rows = write_result.get("truncated_rows", 0)
    if truncated_rows:
        log.warning(f"⚠️  Report truncated {truncated_rows} row(s) due to XLSX limits.")
    log.info(f"💾 Report saved to: {report_path}")
