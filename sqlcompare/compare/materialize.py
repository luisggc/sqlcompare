from contextlib import nullcontext
from datetime import datetime
from typing import Any, Sequence
import uuid

from sqlcompare.db import DBConnection
from sqlcompare.log import log
from .comparator import DatabaseComparator


def compare_queries_in_db(
    previous_sql: str,
    new_sql: str,
    index_cols: Sequence[str],
    connection: Any,
    test_name: str,
    test_schema: str = "sqlcompare",
) -> str:
    """Compatibility wrapper for DatabaseComparator. Materializes queries first."""
    ctx = (
        DBConnection(connection)
        if isinstance(connection, str)
        else nullcontext(connection)
    )

    with ctx as db:
        conn = db.conn if hasattr(db, "conn") else db
        cur = conn.cursor()

        # Unique IDs for temp tables
        run_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:4]}"
        schema_prefix = f"{test_schema}." if test_schema else ""
        prev_table = f"{schema_prefix}tmp_prev_{run_id}"
        new_table = f"{schema_prefix}tmp_new_{run_id}"

        if test_schema:
            try:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {test_schema.split('.')[-1]}")
            except Exception:
                pass

        log.info(f"Materializing previous query to {prev_table}")
        cur.execute(f"CREATE TABLE {prev_table} AS {previous_sql}")
        log.info(f"Materializing new query to {new_table}")
        cur.execute(f"CREATE TABLE {new_table} AS {new_sql}")

        comparator = DatabaseComparator(connection)
        return comparator.compare(
            prev_table, new_table, index_cols, test_name, test_schema
        )
