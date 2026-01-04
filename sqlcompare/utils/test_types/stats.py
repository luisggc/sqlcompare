from pathlib import Path

from sqlcompare.db import DBConnection
from sqlcompare.log import log
from sqlcompare.utils.format import format_table


def _quote_ident(name: str) -> str:
    return f'"{name.replace('"', '""')}"'


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


def compare_table_stats(table1: str, table2: str, connection: str | None) -> None:
    table1_name = table1
    table2_name = table2
    connection_id = connection

    if connection is None:
        path1 = Path(table1).expanduser()
        path2 = Path(table2).expanduser()
        if _is_supported_file(path1) and _is_supported_file(path2):
            connection_id = "duckdb:///:memory:"
            table1_name = path1.stem
            table2_name = path2.stem

    with DBConnection(connection_id) as db:
        if connection is None and connection_id == "duckdb:///:memory:":
            db.create_table_from_file(table1_name, table1)
            db.create_table_from_file(table2_name, table2)

        _, cols_prev = db.query(
            f"SELECT * FROM {table1_name} WHERE 1=0", include_columns=True
        )
        _, cols_new = db.query(
            f"SELECT * FROM {table2_name} WHERE 1=0", include_columns=True
        )

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
    ]
    rows = []
    for key in common_keys:
        prev_nulls = prev_stats.get(key, {}).get("null_count", 0)
        new_nulls = new_stats.get(key, {}).get("null_count", 0)
        prev_distinct = prev_stats.get(key, {}).get("distinct_count", 0)
        new_distinct = new_stats.get(key, {}).get("distinct_count", 0)
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
            )
        )

    log.info("📊 Table statistics comparison:")
    log.info(format_table(output_columns, rows))
