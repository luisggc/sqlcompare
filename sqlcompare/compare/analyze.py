import click
import os
import duckdb
import pandas as pd
from data_toolkit.core.log import log
from ..analysis.utils import (
    _display,
    find_diff_file,
    find_diff_run,
    list_available_diffs,
)
from ..db_connection import DBConnection
from .comparator import DatabaseComparator

RENAME_COLUMNS = {
    "COLUMN": "Column",
    "COLUMN_NAME": "Column",
    "BEFORE": "Before",
    "CURRENT": "Current",
}


@click.command()
@click.argument("diff-id", type=str)
@click.option("-c", "--column", type=str, help="Filter by specific column name")
@click.option(
    "-l", "--limit", type=int, default=25, help="Limit number of results to display"
)
@click.option("--save", is_flag=True, help="Save filtered results to Excel file")
@click.option(
    "--list-columns", is_flag=True, help="List all available columns in the diff data"
)
@click.option(
    "--stats", is_flag=True, help="Show statistics table instead of differences"
)
@click.option(
    "--missing-current", is_flag=True, help="Show rows that are only in current dataset"
)
@click.option(
    "--missing-previous",
    is_flag=True,
    help="Show rows that are only in previous dataset",
)
def analyze_diff(
    diff_id, column, limit, save, list_columns, stats, missing_current, missing_previous
):
    run_analysis(
        diff_id,
        column,
        limit,
        save,
        list_columns,
        stats,
        missing_current,
        missing_previous,
    )


def run_analysis(
    diff_id,
    column=None,
    limit=25,
    save=False,
    list_columns=False,
    stats=False,
    missing_current=False,
    missing_previous=False,
):
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
            con = duckdb.connect(db_file)
            if stats:
                df = con.execute(comp.get_stats_query(column=column)).df()
                df = df.rename(columns=RENAME_COLUMNS)
                _display(
                    df,
                    column,
                    limit,
                    save,
                    diff_id,
                    is_stats=True,
                    list_columns=list_columns,
                )
                return
            if missing_current:
                df = con.execute(comp.get_in_current_only_query()).df()
                _display(df, column, limit, save, diff_id, list_columns=list_columns)
                return
            if missing_previous:
                df = con.execute(comp.get_in_previous_only_query()).df()
                _display(df, column, limit, save, diff_id, list_columns=list_columns)
                return

            # Use limit in SQL only if we are NOT saving and NOT listing columns
            sql_limit = limit if not save and not list_columns else None
            diff_df = con.execute(
                comp.get_diff_query(column=column, limit=sql_limit)
            ).df()
            diff_df = diff_df.rename(columns=RENAME_COLUMNS)
            _display(diff_df, column, limit, save, diff_id, list_columns=list_columns)
            return
        conn = run["conn"]
        comp = DatabaseComparator.from_saved(conn, tables, idx, cols_prev, cols_new)
        with DBConnection(conn) as db:
            if stats:
                q = comp.get_stats_query(column=column)
                df = db.to_df(q)
                df = df.rename(columns=RENAME_COLUMNS)
                _display(
                    df,
                    column,
                    limit,
                    save,
                    diff_id,
                    is_stats=True,
                    list_columns=list_columns,
                )
                return
            if missing_current:
                df = db.to_df(comp.get_in_current_only_query())
                _display(df, column, limit, save, diff_id, list_columns=list_columns)
                return
            if missing_previous:
                df = db.to_df(comp.get_in_previous_only_query())
                _display(df, column, limit, save, diff_id, list_columns=list_columns)
                return

            # Use limit in SQL only if we are NOT saving and NOT listing columns
            sql_limit = limit if not save and not list_columns else None
            diff_df = db.to_df(comp.get_diff_query(column=column, limit=sql_limit))
            diff_df = diff_df.rename(columns=RENAME_COLUMNS)
            _display(diff_df, column, limit, save, diff_id, list_columns=list_columns)
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
        stats_df = pd.read_pickle(stats_file)
        stats_df = stats_df.rename(columns=RENAME_COLUMNS)
        _display(stats_df, column, limit, save, diff_id, is_stats=True)
        return

    if missing_current:
        current_only_file = diff_file.replace(".pkl", "_in_current_only.pkl")
        if not os.path.exists(current_only_file):
            log.info(f"✅ No rows found that exist only in current dataset")
            return
        current_only_df = pd.read_pickle(current_only_file)
        current_only_df = current_only_df.rename(columns=RENAME_COLUMNS)
        _display(current_only_df, column, limit, save, diff_id)
        return

    if missing_previous:
        previous_only_file = diff_file.replace(".pkl", "_in_previous_only.pkl")
        if not os.path.exists(previous_only_file):
            log.info(f"✅ No rows found that exist only in previous dataset")
            return
        previous_only_df = pd.read_pickle(previous_only_file)
        previous_only_df = previous_only_df.rename(columns=RENAME_COLUMNS)
        _display(previous_only_df, column, limit, save, diff_id)
        return

    diff_df = pd.read_pickle(diff_file)
    diff_df = diff_df.rename(columns=RENAME_COLUMNS)
    _display(diff_df, column, limit, save, diff_id, list_columns=list_columns)
