from __future__ import annotations

import json
import typer

from sqlcompare.analysis.utils import find_diff_file, find_diff_run, list_available_diffs
from sqlcompare.log import log


def list_diff_queries(diff_id: str) -> None:
    """List queryable tables for a diff run along with their contents."""
    run = find_diff_run(diff_id)
    if not run:
        diff_file = find_diff_file(diff_id)
        if diff_file:
            log.error(
                "❌ Diff data found, but it is a pickle-based diff without queryable tables."
            )
        else:
            log.error(f"❌ Diff data with ID '{diff_id}' not found.")
        log.info("💡 Available diff IDs:")
        list_available_diffs()
        return

    resolved_id = run.get("id", diff_id)
    tables = run.get("tables")
    if not tables:
        log.error("❌ Diff metadata missing table definitions.")
        return

    conn_label = run.get("conn")
    duckdb_file = run.get("duckdb_file")
    if duckdb_file:
        connection_display = f"duckdb:///{duckdb_file}"
    else:
        connection_display = conn_label or "(default connection)"

    cols_prev = run.get("cols_prev", [])
    cols_new = run.get("cols_new", [])
    index_cols = run.get("index_cols", [])
    common_cols = run.get("common_cols")

    payload = _build_llm_payload(
        diff_id=diff_id,
        resolved_id=resolved_id,
        connection_display=connection_display,
        tables=tables,
        cols_prev=cols_prev,
        cols_new=cols_new,
        index_cols=index_cols,
        common_cols=common_cols,
    )
    log.info(json.dumps(payload, indent=2))


def _format_columns(columns: list[str]) -> list[str]:
    return columns


def _build_llm_payload(
    *,
    diff_id: str,
    resolved_id: str,
    connection_display: str,
    tables: dict[str, str],
    cols_prev: list[str],
    cols_new: list[str],
    index_cols: list[str],
    common_cols: list[str] | None,
) -> dict[str, object]:
    join_columns = [f"{col}_previous" for col in cols_prev] + [
        f"{col}_new" for col in cols_new
    ]
    entries = [
        {
            "name": tables.get("previous"),
            "role": "previous",
            "content": "Previous dataset (original table)",
            "columns": _format_columns(cols_prev),
        },
        {
            "name": tables.get("new"),
            "role": "current",
            "content": "Current dataset (new table)",
            "columns": _format_columns(cols_new),
        },
        {
            "name": tables.get("join"),
            "role": "join",
            "content": "Full outer join on index columns with _previous/_new suffixes",
            "columns": _format_columns(join_columns),
        },
    ]
    queries = _build_queries(tables, index_cols)
    return {
        "diff_id": diff_id,
        "resolved_diff_id": resolved_id,
        "connection": connection_display,
        "index_columns": index_cols,
        "common_columns": common_cols or [],
        "tables": entries,
        "queries": queries,
    }


def _build_queries(tables: dict[str, str], index_cols: list[str]) -> list[dict[str, str]]:
    join_table = tables.get("join")
    if not join_table or not index_cols:
        return []

    prev_null_cond = " AND ".join([f'\"{c}_previous\" IS NULL' for c in index_cols])
    new_null_cond = " AND ".join([f'\"{c}_new\" IS NULL' for c in index_cols])
    idx_expr = ", ".join(
        [f'COALESCE(\"{c}_new\", \"{c}_previous\") AS \"{c}\"' for c in index_cols]
    )
    col_placeholder = "<column>"
    diff_cond = (
        f'NOT (\"{col_placeholder}_previous\" = \"{col_placeholder}_new\" OR '
        f'(\"{col_placeholder}_previous\" IS NULL AND \"{col_placeholder}_new\" IS NULL))'
        f" AND NOT ({prev_null_cond}) AND NOT ({new_null_cond})"
    )

    def _query(name: str, sql: str) -> dict[str, str]:
        return {"name": name, "sql": sql}

    return [
        _query(
            "rows_only_in_current",
            f"SELECT * FROM {join_table} WHERE {prev_null_cond};",
        ),
        _query(
            "rows_only_in_previous",
            f"SELECT * FROM {join_table} WHERE {new_null_cond};",
        ),
        _query(
            "count_rows_only_in_current",
            f"SELECT COUNT(*) AS rows_only_in_current FROM {join_table} WHERE {prev_null_cond};",
        ),
        _query(
            "count_rows_only_in_previous",
            f"SELECT COUNT(*) AS rows_only_in_previous FROM {join_table} WHERE {new_null_cond};",
        ),
        _query(
            "rows_with_column_differences",
            f"SELECT {idx_expr}, '{col_placeholder}' AS \"COLUMN\", "
            f'CAST(\"{col_placeholder}_previous\" AS VARCHAR) AS \"BEFORE\", '
            f'CAST(\"{col_placeholder}_new\" AS VARCHAR) AS \"CURRENT\" '
            f"FROM {join_table} WHERE {diff_cond};",
        ),
        _query(
            "count_column_differences",
            f"SELECT COUNT(*) AS diff_count FROM {join_table} WHERE {diff_cond};",
        ),
        _query(
            "top_10_diff_samples",
            f"""SELECT {idx_expr}, "COLUMN", "BEFORE", "CURRENT"
FROM (
  SELECT {idx_expr}, '{col_placeholder}' AS "COLUMN",
    CAST("{col_placeholder}_previous" AS VARCHAR) AS "BEFORE",
    CAST("{col_placeholder}_new" AS VARCHAR) AS "CURRENT"
  FROM {join_table} WHERE {diff_cond}
) AS diffs
LIMIT 10;""",
        ),
    ]


def diff_queries_cmd(
    diff_id: str = typer.Argument(..., help="Diff run ID"),
) -> None:
    """List queryable tables and SQL templates for a diff run.

    Examples:
        sqlcompare diff-queries <diff_id>
    """
    list_diff_queries(diff_id)
