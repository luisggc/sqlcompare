import uuid
from datetime import datetime
from typing import Any, Sequence

from contextlib import nullcontext

from sqlcompare.config import load_test_runs, save_test_runs
from sqlcompare.db import DBConnection
from sqlcompare.log import log
from sqlcompare.utils.format import format_table


class DatabaseComparator:
    """ANSI SQL comparator operating on a database connection."""

    def __init__(self, connection: Any):
        self.connection = connection
        self.tables: dict[str, str] = {}
        self.index_cols: list[str] = []
        self.cols_prev: list[str] = []
        self.cols_new: list[str] = []
        self.common_cols: list[str] = []

    def _ensure_schema(self, db: DBConnection, test_schema: str) -> None:
        """Ensure the test schema exists without changing the database context permanently."""
        try:
            # Try direct creation first. Snowflake and DuckDB often support CREATE SCHEMA IF NOT EXISTS schema_name
            # even if it's already in the name (e.g. database.schema)
            db.execute(f"CREATE SCHEMA IF NOT EXISTS {test_schema}")
            log.info(f"✅ Ensured schema '{test_schema}' exists")
        except Exception as e:
            # Fallback for cases where full qualification in CREATE SCHEMA is not supported or needs db switch
            log.debug(
                f"Direct schema creation failed: {e}. Trying database switch approach for {test_schema}."
            )
            try:
                if "." in test_schema:
                    db_name, schema_name = test_schema.split(".", 1)
                    # Use absolute path if possible
                    db.execute(f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema_name}")
                else:
                    db.execute(f"CREATE SCHEMA IF NOT EXISTS {test_schema}")
                log.info(f"✅ Ensured schema '{test_schema}' exists")
            except Exception as e2:
                log.warning(f"Could not create schema '{test_schema}': {e2}")

    def _sort_rows(
        self, columns: list[str], rows: list[tuple], sort_col: str | None
    ) -> list[tuple]:
        if not sort_col or sort_col not in columns:
            return rows
        idx = columns.index(sort_col)
        return sorted(rows, key=lambda row: (row[idx] is None, row[idx]))

    @classmethod
    def from_saved(
        cls,
        connection: Any,
        tables: dict[str, str],
        index_cols: Sequence[str],
        cols_prev: Sequence[str],
        cols_new: Sequence[str],
    ) -> "DatabaseComparator":
        inst = cls(connection)
        inst.tables = tables
        inst.index_cols = list(index_cols)
        inst.cols_prev = list(cols_prev)
        inst.cols_new = list(cols_new)
        inst.common_cols = [
            c for c in inst.cols_prev if c in inst.cols_new and c not in inst.index_cols
        ]
        return inst

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def get_join_query(self) -> str:
        return f"SELECT * FROM {self.tables['join']}"

    def get_in_current_only_query(self) -> str:
        cond = " AND ".join([f'"{c}_previous" IS NULL' for c in self.index_cols])
        return f"SELECT * FROM {self.tables['join']} WHERE {cond}"

    def get_in_previous_only_query(self) -> str:
        cond = " AND ".join([f'"{c}_new" IS NULL' for c in self.index_cols])
        return f"SELECT * FROM {self.tables['join']} WHERE {cond}"

    def _diff_condition(self, column: str) -> str:
        null_prev = " AND ".join([f'"{c}_previous" IS NULL' for c in self.index_cols])
        null_new = " AND ".join([f'"{c}_new" IS NULL' for c in self.index_cols])
        return (
            f'NOT ("{column}_previous" = "{column}_new" OR ('
            f'"{column}_previous" IS NULL AND "{column}_new" IS NULL))'
            f" AND NOT ({null_prev}) AND NOT ({null_new})"
        )

    def get_column_diff_query(self, column: str) -> str:
        idx_expr = ", ".join(
            [f'COALESCE("{c}_new", "{c}_previous") AS "{c}"' for c in self.index_cols]
        )
        cond = self._diff_condition(column)
        return (
            f"SELECT {idx_expr}, '{column}' AS \"COLUMN\", "
            f'CAST("{column}_previous" AS VARCHAR) AS "BEFORE", '
            f'CAST("{column}_new" AS VARCHAR) AS "CURRENT" '
            f"FROM {self.tables['join']} WHERE {cond}"
        )

    def get_diff_query(self, column: str = None, limit: int = None) -> str:
        if column:
            # Case-insensitive match for the specific column
            match = next(
                (c for c in self.common_cols if c.upper() == column.upper()), None
            )
            if not match:
                # Still return an empty result if column not found in common cols
                return "SELECT NULL AS ID, NULL AS COLUMN, NULL AS BEFORE, NULL AS CURRENT WHERE 1=0"
            query = self.get_column_diff_query(match)
        else:
            queries = [self.get_column_diff_query(c) for c in self.common_cols]
            query = " UNION ALL ".join(queries)

        if limit:
            query += f" LIMIT {limit}"
        return query

    def get_stats_query(self, column: str = None) -> str:
        cols_to_stat = self.common_cols
        if column:
            match = next(
                (c for c in self.common_cols if c.upper() == column.upper()), None
            )
            if match:
                cols_to_stat = [match]
            else:
                cols_to_stat = []

        if not cols_to_stat:
            return "SELECT NULL AS COLUMN_NAME, 0 AS diff_count WHERE 1=0"

        parts = [
            f"SELECT '{c}' AS COLUMN_NAME, COUNT(*) AS diff_count"
            f" FROM {self.tables['join']} WHERE {self._diff_condition(c)}"
            for c in cols_to_stat
        ]
        return (
            "SELECT COLUMN_NAME, diff_count FROM ("
            + " UNION ALL ".join(parts)
            + ") ORDER BY diff_count DESC"
        )

    # ------------------------------------------------------------------
    def compare(
        self,
        prev_table: str,
        new_table: str,
        index_cols: Sequence[str],
        test_name: str,
        test_schema: str = "sqlcompare",
    ) -> str:
        """Compare two tables in the database."""
        if not index_cols:
            raise ValueError("Index columns are required for SQL comparison")

        # Store uppercased version for case-insensitive matching later
        # Will be replaced with actual DB column names after inspecting temp tables
        self.index_cols = [c.upper() for c in index_cols]

        diff_id = f"{test_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        base = f"sqlcompare_{diff_id}"

        ctx = DBConnection(self.connection)
        with ctx as db:
            # Qualify table names with schema for the join table.
            schema_prefix = f"{test_schema}." if test_schema else ""
            if test_schema and isinstance(self.connection, str):
                # Check if connection might be DuckDB
                if "duckdb" in self.connection.lower() and "." not in test_schema:
                    try:
                        result = db.query("PRAGMA database_list")
                        catalog_name = result[0][1] if result and result[0] else None
                        if catalog_name:
                            schema_prefix = f"{catalog_name}.{test_schema}."
                    except Exception:
                        pass

            join_table = f"{schema_prefix}{base}_join"
            tables = {
                "previous": prev_table,
                "new": new_table,
                "join": join_table,
            }
            self.tables = tables

            # Create schema for the join table if it doesn't exist
            if test_schema:
                self._ensure_schema(db, test_schema)

            try:
                db.execute(f"DROP TABLE IF EXISTS {tables['join']}")
            except Exception:
                pass

            # Get column names from pre-existing tables
            _, cols_prev = db.query(
                f"SELECT * FROM {tables['previous']} WHERE 1=0", include_columns=True
            )
            _, cols_new = db.query(
                f"SELECT * FROM {tables['new']} WHERE 1=0", include_columns=True
            )
            self.cols_prev = cols_prev
            self.cols_new = cols_new

            # Match index columns case-insensitively and verify existence in BOTH tables
            actual_index_cols_prev = []
            actual_index_cols_new = []
            valid_index_cols = []

            for idx_col in self.index_cols:
                match_prev = next(
                    (c for c in cols_prev if c.upper() == idx_col.upper()), None
                )
                match_new = next(
                    (c for c in cols_new if c.upper() == idx_col.upper()), None
                )

                if not match_prev:
                    available = ", ".join(cols_prev[:10])
                    raise ValueError(
                        f"Index column '{idx_col}' not found in previous table '{tables['previous']}'. Available cols: {available}..."
                    )
                if not match_new:
                    available = ", ".join(cols_new[:10])
                    raise ValueError(
                        f"Index column '{idx_col}' not found in new table '{tables['new']}'. Available cols: {available}..."
                    )

                actual_index_cols_prev.append(match_prev)
                actual_index_cols_new.append(match_new)
                valid_index_cols.append(
                    match_prev
                )  # Standardized to prev casing for join result

            # Build join condition using side-specific casing to be safe with quoted identifiers
            join_cond = " AND ".join(
                [
                    f'n."{n_col}"=p."{p_col}"'
                    for p_col, n_col in zip(
                        actual_index_cols_prev, actual_index_cols_new
                    )
                ]
            )
            self.index_cols = valid_index_cols
            self.common_cols = [
                c for c in cols_prev if c in cols_new and c not in self.index_cols
            ]

            # Build join table with side specific columns
            select_cols = [f'p."{c}" AS "{c}_previous"' for c in cols_prev]
            select_cols += [f'n."{c}" AS "{c}_new"' for c in cols_new]
            join_sql = (
                f"CREATE TABLE {tables['join']} AS SELECT "
                + ", ".join(select_cols)
                + f" FROM {tables['new']} n FULL OUTER JOIN {tables['previous']} p ON {join_cond}"
            )

            log.info(f"Joining tables: {tables['previous']} and {tables['new']}")

            db.execute(join_sql)

            cond_prev = " AND ".join(
                [f'"{c}_previous" IS NULL' for c in self.index_cols]
            )
            result = db.query(
                "SELECT COUNT(*) FROM " + tables["join"] + " WHERE " + cond_prev
            )
            missing_prev = result[0][0] if result else 0
            cond_new = " AND ".join([f'"{c}_new" IS NULL' for c in self.index_cols])
            result = db.query(
                "SELECT COUNT(*) FROM " + tables["join"] + " WHERE " + cond_new
            )
            missing_new = result[0][0] if result else 0
            if missing_prev:
                log.info(f"Rows only in current dataset: {missing_prev}")
                sample_q = (
                    "SELECT "
                    + ", ".join([f'"{c}_new" AS "{c}"' for c in self.cols_new])
                    + f" FROM {tables['join']} WHERE {cond_prev} LIMIT 5"
                )
                rows, columns = db.query(sample_q, include_columns=True)
                rows = self._sort_rows(
                    columns, rows, self.index_cols[0] if self.index_cols else None
                )
                log.info(format_table(columns, rows))
            else:
                log.info("No rows only in current dataset")

            if missing_new:
                log.info(f"Rows only in previous dataset: {missing_new}")
                sample_q = (
                    "SELECT "
                    + ", ".join([f'"{c}_previous" AS "{c}"' for c in self.cols_prev])
                    + f" FROM {tables['join']} WHERE {cond_new} LIMIT 5"
                )
                rows, columns = db.query(sample_q, include_columns=True)
                rows = self._sort_rows(
                    columns, rows, self.index_cols[0] if self.index_cols else None
                )
                log.info(format_table(columns, rows))
            else:
                log.info("No rows only in previous dataset")

            diff_sql = self.get_diff_query()
            if diff_sql:
                result = db.query(f"SELECT COUNT(*) FROM ({diff_sql})")
                diff_total = result[0][0] if result else 0
                log.info(
                    f"\U0001f6a8 Differences in values for common rows: {diff_total} rows in total"
                )

                # Show stats by column
                if self.common_cols:
                    log.info("\n📊 Difference statistics by column:")
                    stats_sql = self.get_stats_query()
                    rows, columns = db.query(stats_sql, include_columns=True)
                    col_lookup = {col.upper(): idx for idx, col in enumerate(columns)}
                    col_name_idx = col_lookup.get("COLUMN_NAME")
                    diff_count_idx = col_lookup.get("DIFF_COUNT")
                    diff_rows = []
                    no_diff_cols = []
                    for row in rows:
                        diff_count = (
                            row[diff_count_idx] if diff_count_idx is not None else 0
                        )
                        if diff_count and diff_count > 0:
                            diff_rows.append(row)
                        elif col_name_idx is not None:
                            no_diff_cols.append(str(row[col_name_idx]))
                    if diff_rows:
                        log.info(format_table(columns, diff_rows))
                    if no_diff_cols:
                        log.info(
                            f"\n✅ Columns with no differences: {', '.join(no_diff_cols)}"
                        )

                log.info("\n📋 Sample differences (first 10 rows):")
                sample_q = f"SELECT * FROM ({diff_sql}) t LIMIT 10"
                rows, columns = db.query(sample_q, include_columns=True)
                rows = self._sort_rows(
                    columns, rows, self.index_cols[0] if self.index_cols else None
                )
                log.info(format_table(columns, rows))
            else:
                log.info("0 rows in total")

        runs = load_test_runs()

        # Save metadata for later analysis
        # For DuckDB connections (file-based tests), save "duckdb"
        # For remote databases (Snowflake, etc.), save the connection name
        conn_name = self.connection if isinstance(self.connection, str) else "duckdb"

        run_data = {
            "tables": tables,
            "index_cols": list(self.index_cols),
            "cols_prev": self.cols_prev,
            "cols_new": self.cols_new,
            "conn": conn_name,
        }

        runs[diff_id] = run_data
        save_test_runs(runs)

        log.debug(f"\U0001f4c1 Analysis data saved with ID: {diff_id}")
        log.debug(f"Use 'sqlcompare analyze-diff {diff_id}' to review differences")

        return diff_id
