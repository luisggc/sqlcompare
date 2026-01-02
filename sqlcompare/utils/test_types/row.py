from data_toolkit.core.log import log
from data_toolkit.core.config import (
    load_test_runs,
    save_test_runs,
    DB_TEST_DB,
)
import os
import uuid
from datetime import datetime
import duckdb


def _diff_condition(column: str, index_cols: list[str]) -> str:
    null_prev = " AND ".join([f"{c}_previous IS NULL" for c in index_cols]) or "FALSE"
    null_new = " AND ".join([f"{c}_new IS NULL" for c in index_cols]) or "FALSE"
    return (
        f"NOT ({column}_previous = {column}_new OR ("
        f"{column}_previous IS NULL AND {column}_new IS NULL))"
        f" AND NOT ({null_prev}) AND NOT ({null_new})"
    )


def _column_diff_query(column: str, index_cols: list[str], join_table: str) -> str:
    idx_expr = ", ".join(
        [f"COALESCE({c}_new, {c}_previous) AS {c}" for c in index_cols]
    )
    cond = _diff_condition(column, index_cols)
    return (
        f"SELECT {idx_expr}, '{column}' AS Column, "
        f"CAST({column}_previous AS VARCHAR) AS Before, "
        f"CAST({column}_new AS VARCHAR) AS Current "
        f"FROM {join_table} WHERE {cond}"
    )


def test_row(df_previous, df_current, top_diff, test_config, save):
    log.info("Comparing...")

    for df in (df_previous, df_current):
        for col in df.select_dtypes(include=["float64"]).columns:
            df[col] = df[col].round(10)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    test_id = f"{test_config['test_name']}_{timestamp}_{uuid.uuid4().hex[:8]}"
    base = f"dtk_test_{test_id}"
    tables = {
        "previous": f"{base}_previous",
        "new": f"{base}_new",
        "join": f"{base}_join",
    }

    index_cols = df_previous.attrs.get("index_cols", [])
    if not index_cols:
        raise ValueError("Index columns are required for row comparison")

    db_path = DB_TEST_DB
    if not db_path.parent.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.register("previous_df", df_previous)
    con.register("new_df", df_current)

    cols_prev = [c.upper() for c in df_previous.columns]
    cols_new = [c.upper() for c in df_current.columns]
    common_cols = [c for c in cols_prev if c in cols_new and c not in index_cols]

    select_cols = [f"p.{c} AS {c}_previous" for c in cols_prev]
    select_cols += [f"n.{c} AS {c}_new" for c in cols_new]
    join_cond = " AND ".join([f"n.{c}=p.{c}" for c in index_cols])

    for t in tables.values():
        con.execute(f"DROP TABLE IF EXISTS {t}")
    con.execute(f"CREATE TABLE {tables['previous']} AS SELECT * FROM previous_df")
    con.execute(f"CREATE TABLE {tables['new']} AS SELECT * FROM new_df")
    con.execute(
        "CREATE TABLE "
        + tables["join"]
        + " AS SELECT "
        + ", ".join(select_cols)
        + f" FROM {tables['new']} n FULL OUTER JOIN {tables['previous']} p ON "
        + join_cond
    )

    cond_prev = " AND ".join([f"{c}_previous IS NULL" for c in index_cols])
    cond_new = " AND ".join([f"{c}_new IS NULL" for c in index_cols])
    missing_prev = con.execute(
        f"SELECT COUNT(*) FROM {tables['join']} WHERE {cond_prev}"
    ).fetchone()[0]
    missing_new = con.execute(
        f"SELECT COUNT(*) FROM {tables['join']} WHERE {cond_new}"
    ).fetchone()[0]

    if missing_prev:
        log.info(f"Rows only in current dataset: {missing_prev}")
    else:
        log.info("No rows only in current dataset")

    if missing_new:
        log.info(f"Rows only in previous dataset: {missing_new}")
    else:
        log.info("No rows only in previous dataset")

    diff_sql = " UNION ALL ".join(
        [_column_diff_query(c, index_cols, tables["join"]) for c in common_cols]
    )
    diff_df = con.execute(diff_sql).fetchdf() if diff_sql else df_previous.__class__()
    log.info(str(len(diff_df)) + " rows in total")

    if len(diff_df) == 0:
        log.info("No differences found.")
        return

    log.info(diff_df.head(top_diff))
    log.info("")

    differences_by_columns = (
        diff_df.groupby("Column").size().reset_index(name="diff_count")
    )

    total_common_rows = con.execute(
        f"SELECT COUNT(*) FROM {tables['join']} WHERE NOT ({cond_prev}) AND NOT ({cond_new})"
    ).fetchone()[0]
    differences_by_columns["per_diff"] = (
        differences_by_columns["diff_count"] / total_common_rows * 100
    ).round(1).astype(str) + "%"

    def add_null_percentages(row):
        column_name = row["Column"]
        total_rows = total_common_rows
        null_before = con.execute(
            f"SELECT COUNT(*) FROM {tables['join']} WHERE {column_name}_previous IS NULL"
        ).fetchone()[0]
        null_after = con.execute(
            f"SELECT COUNT(*) FROM {tables['join']} WHERE {column_name}_new IS NULL"
        ).fetchone()[0]
        row["per_null_before"] = f"{(null_before / total_rows * 100):.1f}%"
        row["per_null_after"] = f"{(null_after / total_rows * 100):.1f}%"
        return row

    differences_by_columns = differences_by_columns.apply(add_null_percentages, axis=1)
    differences_by_columns = differences_by_columns.sort_values(
        ["diff_count", "per_null_before"], ascending=[False, True]
    )

    log.info("Columns with different values:")
    log.info(differences_by_columns)
    log.info("")

    total_columns = len(common_cols)
    columns_with_diffs = set(diff_df["Column"].unique())
    columns_100_match = total_columns - len(columns_with_diffs)
    columns_90_match = columns_100_match + len(
        differences_by_columns[
            differences_by_columns["per_diff"].str.replace("%", "").astype(float) < 10
        ]
    )
    columns_50_match = columns_100_match + len(
        differences_by_columns[
            differences_by_columns["per_diff"].str.replace("%", "").astype(float) < 50
        ]
    )

    log.info("📊 Column Matching Summary:")
    log.info(
        f"   {columns_100_match} columns matching 100% ({(columns_100_match / total_columns * 100):.1f}% of total columns)"
    )
    log.info(
        f"   {columns_90_match} columns matching >90% ({(columns_90_match / total_columns * 100):.1f}% of total columns)"
    )
    log.info(
        f"   {columns_50_match} columns matching >50% ({(columns_50_match / total_columns * 100):.1f}% of total columns)"
    )
    log.info(f"   Total columns compared: {total_columns}")
    log.info("")

    runs = load_test_runs()
    runs[test_id] = {
        "duckdb_file": str(db_path),
        "tables": tables,
        "index_cols": index_cols,
        "cols_prev": cols_prev,
        "cols_new": cols_new,
    }
    save_test_runs(runs)

    column_with_most_diffs = (
        differences_by_columns.iloc[0]["Column"]
        if not differences_by_columns.empty
        else "COLUMN_NAME"
    )

    log.info(f"📁 Analysis data saved with ID: {test_id}")
    log.info("💡 To analyze specific columns from this diff, run:")
    log.info(
        f"   dtk db analyze-diff {test_id} --limit 100 --column {column_with_most_diffs}"
    )
    log.info("")

    if save:
        dt_suffix = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"{test_config['test_folder']}/extracts/{test_config['test_name']}-{dt_suffix}.xlsx"
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        diff_df.head(top_diff).to_excel(file_name, index=True)
