import uuid
from datetime import datetime
from typing import Any, Sequence

from contextlib import nullcontext
from data_toolkit.db.db_connection import DBConnection
from data_toolkit.core.config import load_test_runs, save_test_runs
from data_toolkit.core.log import log


class DatabaseComparator:
    """ANSI SQL comparator operating on a database connection."""

    def __init__(self, connection: Any):
        self.connection = connection
        self.tables: dict[str, str] = {}
        self.index_cols: list[str] = []
        self.cols_prev: list[str] = []
        self.cols_new: list[str] = []
        self.common_cols: list[str] = []

    def _ensure_schema(self, cur, test_schema: str) -> None:
        """Ensure the test schema exists without changing the database context permanently."""
        try:
            # Try direct creation first. Snowflake and DuckDB often support CREATE SCHEMA IF NOT EXISTS schema_name
            # even if it's already in the name (e.g. database.schema)
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {test_schema}")
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
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema_name}")
                else:
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {test_schema}")
                log.info(f"✅ Ensured schema '{test_schema}' exists")
            except Exception as e2:
                log.warning(f"Could not create schema '{test_schema}': {e2}")

    def _get_to_df(self, connection: Any):
        """Get a function to convert query results to a DataFrame."""
        if hasattr(connection, "to_df"):
            return connection.to_df

        def _to_df(query, conn=connection):
            import pandas as pd
            import warnings

            # Detect if it's duckdb connection
            is_duckdb = "duckdb" in str(type(conn)).lower()
            if is_duckdb:
                # Handle connection object or DBConnection wrapper
                c = conn.conn if hasattr(conn, "conn") else conn
                return c.execute(query).df()
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", message=".*pandas only supports SQLAlchemy connectable.*"
                )
                c = conn.conn if hasattr(conn, "conn") else conn
                return pd.read_sql(query, c)

        return _to_df

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
        test_schema: str = "dtk_tests",
    ) -> str:
        """Compare two tables in the database."""
        if not index_cols:
            raise ValueError("Index columns are required for SQL comparison")

        # Store uppercased version for case-insensitive matching later
        # Will be replaced with actual DB column names after inspecting temp tables
        self.index_cols = [c.upper() for c in index_cols]

        diff_id = f"{test_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        base = f"dtk_test_{diff_id}"

        # Qualify table names with schema for the join table
        schema_prefix = f"{test_schema}." if test_schema else ""
        join_table = f"{schema_prefix}{base}_join"

        tables = {
            "previous": prev_table,
            "new": new_table,
            "join": join_table,
        }
        self.tables = tables

        ctx = (
            DBConnection(self.connection)
            if isinstance(self.connection, str)
            else nullcontext(self.connection)
        )
        with ctx as db:
            to_df_func = self._get_to_df(db)
            conn = db.conn if hasattr(db, "conn") else db
            cur = conn.cursor()

            # Create schema for the join table if it doesn't exist
            if test_schema:
                self._ensure_schema(cur, test_schema)

            try:
                cur.execute(f"DROP TABLE IF EXISTS {tables['join']}")
            except Exception:
                pass

            # Get column names from pre-existing tables
            cur.execute(f"SELECT * FROM {tables['previous']} WHERE 1=0")
            cols_prev = [c[0] for c in cur.description]
            cur.execute(f"SELECT * FROM {tables['new']} WHERE 1=0")
            cols_new = [c[0] for c in cur.description]
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

            cur.execute(join_sql)

            cond_prev = " AND ".join(
                [f'"{c}_previous" IS NULL' for c in self.index_cols]
            )
            cur.execute(
                "SELECT COUNT(*) FROM " + tables["join"] + " WHERE " + cond_prev
            )
            missing_prev = cur.fetchone()[0]
            cond_new = " AND ".join([f'"{c}_new" IS NULL' for c in self.index_cols])
            cur.execute("SELECT COUNT(*) FROM " + tables["join"] + " WHERE " + cond_new)
            missing_new = cur.fetchone()[0]
            if missing_prev:
                log.info(f"Rows only in current dataset: {missing_prev}")
                sample_q = (
                    "SELECT "
                    + ", ".join([f'"{c}_new" AS "{c}"' for c in self.cols_new])
                    + f" FROM {tables['join']} WHERE {cond_prev} LIMIT 5"
                )
                df_prev = to_df_func(sample_q)
                df_fmt = df_prev
                if self.index_cols and self.index_cols[0] in df_fmt.columns:
                    df_fmt = df_fmt.sort_values(self.index_cols[0]).reset_index(
                        drop=True
                    )

                # Limit columns to display to avoid bad formatting
                if len(df_fmt.columns) > 10:
                    display_cols = list(df_fmt.columns[:10])
                    log.info(
                        df_fmt[display_cols].to_string(index=False)
                        + f"\n... and {len(df_fmt.columns) - 10} more columns"
                    )
                else:
                    log.info(df_fmt.to_string(index=False))
            else:
                log.info("No rows only in current dataset")

            if missing_new:
                log.info(f"Rows only in previous dataset: {missing_new}")
                sample_q = (
                    "SELECT "
                    + ", ".join([f'"{c}_previous" AS "{c}"' for c in self.cols_prev])
                    + f" FROM {tables['join']} WHERE {cond_new} LIMIT 5"
                )
                df_new = to_df_func(sample_q)
                df_fmt = df_new
                if self.index_cols and self.index_cols[0] in df_fmt.columns:
                    df_fmt = df_fmt.sort_values(self.index_cols[0]).reset_index(
                        drop=True
                    )

                # Limit columns to display to avoid bad formatting
                if len(df_fmt.columns) > 10:
                    display_cols = list(df_fmt.columns[:10])
                    log.info(
                        df_fmt[display_cols].to_string(index=False)
                        + f"\n... and {len(df_fmt.columns) - 10} more columns"
                    )
                else:
                    log.info(df_fmt.to_string(index=False))
            else:
                log.info("No rows only in previous dataset")

            diff_sql = self.get_diff_query()
            if diff_sql:
                cur.execute(f"SELECT COUNT(*) FROM ({diff_sql})")
                diff_total = cur.fetchone()[0]
                log.info(
                    f"\U0001f6a8 Differences in values for common rows: {diff_total} rows in total"
                )

                # Show stats by column
                if self.common_cols:
                    log.info("\n📊 Difference statistics by column:")
                    stats_sql = self.get_stats_query()
                    df_stats = to_df_func(stats_sql)
                    # Support both lowercase and uppercase results from DB
                    df_stats.columns = [c.upper() for c in df_stats.columns]
                    df_stats = df_stats.rename(
                        columns={"COLUMN_NAME": "Column", "DIFF_COUNT": "Differences"}
                    )

                    # Only show columns with differences
                    diff_stats = df_stats[df_stats["Differences"] > 0]
                    no_diff_cols = df_stats[df_stats["Differences"] == 0][
                        "Column"
                    ].tolist()

                    if not diff_stats.empty:
                        log.info(diff_stats.to_string(index=False))

                    if no_diff_cols:
                        log.info(
                            f"\n✅ Columns with no differences: {', '.join(no_diff_cols)}"
                        )

                log.info("\n📋 Sample differences (first 10 rows):")
                sample_q = f"SELECT * FROM ({diff_sql}) t LIMIT 10"
                df_sample = to_df_func(sample_q)
                # Support both lowercase and uppercase results from DB
                df_sample.columns = [c.upper() for c in df_sample.columns]
                rename = {"COLUMN": "Column", "BEFORE": "Before", "CURRENT": "Current"}
                df_fmt = df_sample.rename(columns=rename)
                if self.index_cols and self.index_cols[0] in df_fmt.columns:
                    df_fmt = df_fmt.sort_values(self.index_cols[0]).reset_index(
                        drop=True
                    )

                # Limit columns to display to avoid bad formatting
                if len(df_fmt.columns) > 10:
                    display_cols = list(df_fmt.columns[:10])
                    log.info(
                        df_fmt[display_cols].to_string(index=False)
                        + f"\n... and {len(df_fmt.columns) - 10} more columns"
                    )
                else:
                    log.info(df_fmt.to_string(index=False))
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
        log.debug(f"Use 'dtk db analyze-diff {diff_id}' to review differences")

        return diff_id
