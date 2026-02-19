from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, Result
from sqlalchemy.exc import SQLAlchemyError

from .engine import get_engine, dispose_all_engines
from .exceptions import DBConnectionError
from .models import ExecMeta, FetchMode
from .resolver import resolve_connection_url


@dataclass
class DBConnection:
    """
    SQLAlchemy 2.x helper with:
      - conn_id-driven URL resolution (env var lookup OR YAML file) OR direct URL usage
      - cached engines (Option A)
      - context-managed connection + transaction (Mode 1)
      - one query method that can return rows, rows+cols, dicts, etc.

    Example:
        db = DBConnection("snowflake_prod")
        with db as dbc:
            dbc.query("CREATE TEMP TABLE t1 AS SELECT 1")
            rows = dbc.query("SELECT * FROM t1")
            rows, cols = dbc.query("SELECT * FROM t1", include_columns=True)
            dicts = dbc.query("SELECT * FROM t1", as_dict=True)

    Connection resolution order (first match wins):
        1. Use default connection if conn_id is None (SQLCOMPARE_CONN_DEFAULT, DTK_CONN_DEFAULT)
        2. Direct URL (if contains "://")
        3. Environment variables: SQLCOMPARE_CONN_<NAME>, DTK_CONN_<NAME>
        4. YAML files: ~/.sqlcompare/connections.yml, ~/.dtk/connections.yml
    """

    conn_id: str | None = None
    engine_kwargs: dict[str, Any] = field(default_factory=dict)

    # runtime fields
    _engine: Engine | None = field(init=False, default=None)
    _conn: Connection | None = field(init=False, default=None)
    _tx: Any = field(init=False, default=None)

    # last execution metadata (handy for logging/debug)
    last_sql: str | None = field(init=False, default=None)
    last_elapsed_ms: int | None = field(init=False, default=None)
    last_rowcount: int | None = field(init=False, default=None)

    def __enter__(self) -> "DBConnection":
        url = resolve_connection_url(self.conn_id)

        # Check if this is a file-based connection
        file_path = None
        if "_file_path=" in url:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            if "_file_path" in params:
                file_path = Path(params["_file_path"][0])
                # Remove the _file_path parameter from the URL
                url = url.split("?")[0]

        self._engine = get_engine(url, engine_kwargs=self.engine_kwargs)
        self._conn = self._engine.connect()
        # Mode 1: transaction per context
        self._tx = self._conn.begin()

        # If this is a file-based connection, load the file into DuckDB
        if file_path:
            self._load_file_into_duckdb(file_path)

        return self

    def __exit__(self, exc_type, _exc_val, _exc_tb) -> None:
        # Commit / rollback transaction
        try:
            if self._tx is not None:
                if exc_type is None:
                    self._tx.commit()
                else:
                    self._tx.rollback()
        finally:
            # Always close the SQLAlchemy Connection (returns DBAPI connection to pool,
            # OR fully closes it for NullPool / no-pool cases).
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
            self._conn = None
            self._tx = None

    def _load_file_into_duckdb(self, file_path: Path) -> None:
        """
        Load a CSV or Excel file into DuckDB as a table.
        The table name will be the file stem (filename without extension).
        """
        self.create_table_from_file(file_path.stem, file_path)

    def _ensure_duckdb(self) -> None:
        if self._engine is None:
            raise DBConnectionError(
                "Connection is not open. Use `with DBConnection(...) as dbc:`",
                conn_id=self.conn_id or "<direct>",
            )
        if self._engine.dialect.name != "duckdb":
            raise DBConnectionError(
                "File loading requires a DuckDB connection.",
                conn_id=self.conn_id or "<direct>",
            )

    def _infer_column_type(self, table: str, column: str) -> str:
        for t in ["BIGINT", "DOUBLE", "TIMESTAMP", "DATE"]:
            q = (
                f"SELECT COUNT(*) FROM {table} "
                f'WHERE TRY_CAST("{column}" AS {t}) IS NULL AND "{column}" IS NOT NULL'
            )
            if self.conn.execute(text(q)).fetchone()[0] == 0:
                return t
        return "VARCHAR"

    def _ensure_excel_extension(self) -> None:
        try:
            self.conn.execute(text("LOAD excel"))
        except Exception:
            try:
                self.conn.execute(text("INSTALL excel"))
                self.conn.execute(text("LOAD excel"))
            except Exception as exc:
                raise RuntimeError(
                    "DuckDB excel extension is required to read XLSX files."
                ) from exc

    def create_table_from_file(self, table_name: str, file_path: str | Path) -> None:
        path = Path(file_path)
        suffix = path.suffix.lower()
        self._ensure_duckdb()

        if suffix == ".csv":
            read_query = f"SELECT * FROM read_csv_auto('{path}')"
        elif suffix == ".xlsx":
            self._ensure_excel_extension()
            read_query = f"SELECT * FROM read_xlsx('{path}')"
        else:
            raise ValueError(f"Unsupported file type: {suffix}")

        tmp = f"tmp_{uuid.uuid4().hex}"
        self.conn.execute(text(f"CREATE TABLE {tmp} AS {read_query}"))
        cols = [
            row[1]
            for row in self.conn.execute(text(f"PRAGMA table_info('{tmp}')")).fetchall()
        ]
        exprs = []
        for c in cols:
            t = self._infer_column_type(tmp, c)
            if t == "VARCHAR":
                exprs.append(f'"{c}"')
            else:
                exprs.append(f'CAST("{c}" AS {t}) AS "{c}"')
        self.conn.execute(
            text(f"CREATE TABLE {table_name} AS SELECT {', '.join(exprs)} FROM {tmp}")
        )
        self.conn.execute(text(f"DROP TABLE {tmp}"))

    @property
    def conn(self) -> Connection:
        if self._conn is None:
            raise DBConnectionError(
                "Connection is not open. Use `with DBConnection(...) as dbc:`",
                conn_id=self.conn_id,
            )
        return self._conn

    @property
    def raw_connection(self) -> Any:
        """Return the underlying DBAPI connection (e.g., DuckDB connection)."""
        if self._conn is None:
            raise DBConnectionError(
                "Connection is not open. Use `with DBConnection(...) as dbc:`",
                conn_id=self.conn_id,
            )
        return self._conn.connection.dbapi_connection

    def _run(self, sql: str, params: dict[str, Any] | None = None) -> Result:
        self.last_sql = sql
        t0 = time.perf_counter()
        try:
            # Using SQLAlchemy text() as requested
            res = self.conn.execute(text(sql), params or {})
            return res
        except SQLAlchemyError as e:
            # Extract just the database error message without the full SQL statement
            error_msg = str(e)
            # SQLAlchemy often includes the SQL in square brackets at the end
            # Format: "error message [SQL: long query here]"
            if "[SQL:" in error_msg:
                # Extract just the part before [SQL:
                db_error = error_msg.split("[SQL:")[0].strip()
            else:
                db_error = error_msg

            raise DBConnectionError(
                db_error,
                conn_id=self.conn_id,
                sql=sql,
                original=e,
            ) from None  # Suppress the original exception chain to avoid showing SQL twice
        finally:
            self.last_elapsed_ms = int((time.perf_counter() - t0) * 1000)

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> ExecMeta:
        """
        Execute a statement and return lightweight metadata.
        Intended for DDL/DML (no row fetching).
        """
        res = self._run(sql, params=params)

        rowcount = res.rowcount if res.rowcount is not None else None
        cols = list(res.keys()) if res.returns_rows else None

        self.last_rowcount = rowcount
        return ExecMeta(
            elapsed_ms=self.last_elapsed_ms or 0, rowcount=rowcount, columns=cols
        )

    def get_table_columns(self, table_name: str) -> list[str]:
        """
        Get the actual column names from a table, using database-specific metadata queries
        to ensure correct case sensitivity (especially important for Snowflake).

        Args:
            table_name: Fully qualified table name (e.g., "schema.table" or "db.schema.table")

        Returns:
            List of column names with their actual case as stored in the database
        """
        # Detect if we're using Snowflake by checking the dialect
        try:
            dialect_name = self._engine.dialect.name.lower() if self._engine else None
        except Exception:
            dialect_name = None

        # For Snowflake, use DESCRIBE TABLE to get actual column names
        if dialect_name == "snowflake":
            try:
                # DESCRIBE TABLE returns columns with their actual case
                result = self.query(f"DESCRIBE TABLE {table_name}")
                # First column is the column name
                return [row[0] for row in result]
            except Exception:
                pass

        # Fallback: use SELECT * WHERE 1=0 and get column names from result
        _, columns = self.query(f"SELECT * FROM {table_name} WHERE 1=0", include_columns=True)
        return columns

    def query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        fetch: FetchMode = "auto",
        include_columns: bool = False,
        as_dict: bool = False,
    ):
        """
        Execute SQL and (optionally) fetch results.

        Returns (depending on options + SQL):
          - None                           (DDL/DML with fetch="auto" or fetch=False)
          - list[tuple]                    (default for SELECT)
          - (list[tuple], list[str])       (include_columns=True)
          - list[dict[str, Any]]           (as_dict=True)
          - (list[dict[str, Any]], cols)   (as_dict=True + include_columns=True)
        """
        res = self._run(sql, params=params)

        if fetch == "auto":
            should_fetch = bool(res.returns_rows)
        else:
            should_fetch = bool(fetch)

        self.last_rowcount = res.rowcount if res.rowcount is not None else None

        if not should_fetch:
            return None

        columns = list(res.keys())

        if as_dict:
            mappings = res.mappings().all()  # list[RowMapping]
            dicts = [dict(m) for m in mappings]
            return (dicts, columns) if include_columns else dicts

        rows = res.all()  # list[Row]
        tuples = [tuple(r) for r in rows]
        return (tuples, columns) if include_columns else tuples

    @staticmethod
    def dispose_all_engines() -> None:
        """
        Force-close pooled connections and clear the engine cache.
        Useful in tests.
        """
        dispose_all_engines()

    @staticmethod
    def get_engine(url: str, *, engine_kwargs: dict[str, Any] | None = None) -> Engine:
        """
        Return a cached Engine for this URL (thread-safe).

        Default behavior:
        - Reuse engines (and their pools) across DBConnection instances for the same URL.
        - For SQLite in-memory URLs, auto-inject NullPool unless user overrides poolclass,
            so each `with DBConnection(...):` gets a truly fresh in-memory database.
        """
        return get_engine(url, engine_kwargs=engine_kwargs)
