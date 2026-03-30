import os
import uuid
from pathlib import Path

from sqlcompare.config import get_default_schema
from sqlcompare.db import DBConnection
from sqlcompare.log import log
from sqlcompare.helpers import create_table_from_select, detect_input, ensure_schema
from sqlcompare.utils.format import format_table


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _collect_table_stats(
    db: DBConnection, table: str, columns: list[str]
) -> tuple[int, dict[str, dict[str, int]]]:
    if not columns:
        return 0, {}

    select_parts = ["COUNT(*) AS row_count"]
    alias_map: dict[str, tuple[str, str]] = {}
    for idx, col in enumerate(columns):
        col_ref = _quote_ident(col)
        null_alias = f"c{idx}_nulls"
        distinct_alias = f"c{idx}_distinct"
        select_parts.append(
            f"SUM(CASE WHEN {col_ref} IS NULL THEN 1 ELSE 0 END) AS {null_alias}"
        )
        select_parts.append(f"COUNT(DISTINCT {col_ref}) AS {distinct_alias}")
        alias_map[col.upper()] = (null_alias, distinct_alias)

    query = f"SELECT {', '.join(select_parts)} FROM {table}"
    rows, cols = db.query(query, include_columns=True)
    if not rows:
        return 0, {}

    row = rows[0]
    col_index = {col.upper(): idx for idx, col in enumerate(cols)}
    row_count = row[col_index.get("ROW_COUNT", 0)]

    stats: dict[str, dict[str, int]] = {}
    for col_key, (null_alias, distinct_alias) in alias_map.items():
        null_idx = col_index.get(null_alias.upper())
        distinct_idx = col_index.get(distinct_alias.upper())
        stats[col_key] = {
            "null_count": row[null_idx] if null_idx is not None else 0,
            "distinct_count": row[distinct_idx] if distinct_idx is not None else 0,
        }
    return int(row_count), stats


def _is_supported_file(path: Path) -> bool:
    return path.exists() and path.suffix.lower() in (".csv", ".xlsx")


def _resolve_connection(connection: str | None) -> str:
    if connection:
        return connection
    default_conn = os.getenv("SQLCOMPARE_CONN_DEFAULT") or os.getenv("DTK_CONN_DEFAULT")
    if not default_conn:
        raise ValueError(
            "No connection specified. Use --connection or set SQLCOMPARE_CONN_DEFAULT."
        )
    return default_conn


def compare_table_stats(table1: str, table2: str, connection: str | None) -> None:
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

        cols_prev = db.get_table_columns(table1_name)
        cols_new = db.get_table_columns(table2_name)

        prev_map = {col.upper(): col for col in cols_prev}
        new_map = {col.upper(): col for col in cols_new}
        common_keys = [key for key in prev_map if key in new_map]

        if not common_keys:
            log.info("No common columns found between the two tables.")
            return

        prev_cols = [prev_map[key] for key in common_keys]
        new_cols = [new_map[key] for key in common_keys]

        prev_count, prev_stats = _collect_table_stats(db, table1_name, prev_cols)
        new_count, new_stats = _collect_table_stats(db, table2_name, new_cols)

    output_columns = [
        "COLUMN_NAME",
        "PREV_ROWS",
        "NEW_ROWS",
        "PREV_NULLS",
        "NEW_NULLS",
        "PREV_DISTINCT",
        "NEW_DISTINCT",
        "ROWS_DIFF",
        "NULLS_DIFF",
        "DISTINCT_DIFF",
        "STATUS_PCT",
    ]
    rows = []
    row_base = max(prev_count, new_count, 1)
    for key in common_keys:
        prev_nulls = prev_stats.get(key, {}).get("null_count", 0)
        new_nulls = new_stats.get(key, {}).get("null_count", 0)
        prev_distinct = prev_stats.get(key, {}).get("distinct_count", 0)
        new_distinct = new_stats.get(key, {}).get("distinct_count", 0)
        distinct_base = max(prev_distinct, new_distinct, 1)
        row_sim = 1 - (abs(new_count - prev_count) / row_base)
        null_sim = 1 - (abs(new_nulls - prev_nulls) / row_base)
        distinct_sim = 1 - (abs(new_distinct - prev_distinct) / distinct_base)
        status_pct = max(0.0, min(100.0, (row_sim + null_sim + distinct_sim) / 3 * 100))
        rows.append(
            (
                prev_map[key],
                prev_count,
                new_count,
                prev_nulls,
                new_nulls,
                prev_distinct,
                new_distinct,
                new_count - prev_count,
                new_nulls - prev_nulls,
                new_distinct - prev_distinct,
                round(status_pct, 2),
            )
        )

    rows.sort(key=lambda row: row[-1])

    log.info("📊 Table statistics comparison:")
    log.info(
        format_table(
            output_columns,
            rows,
            max_rows=len(rows),
            max_cols=len(output_columns),
        )
    )
