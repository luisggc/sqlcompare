import uuid
from pathlib import Path

import typer

from sqlcompare.config import DB_TEST_DB, get_default_schema
from sqlcompare.log import log
from sqlcompare.utils.test_types.stats import compare_table_stats

from .comparator import DatabaseComparator


def compare_table(
    table1: str,
    table2: str,
    ids: str | None,
    connection: str | None,
    schema: str | None,
    *,
    stats: bool = False,
) -> None:
    """Compare two tables in the database.

    TABLE1 and TABLE2 are the names of the tables to compare.
    IDS is a comma-separated list of column names to use as the unique key.
    """
    if stats:
        compare_table_stats(table1, table2, connection)
        return

    if not ids:
        raise typer.BadParameter("Key columns are required unless --stats is provided.")

    schema = schema or get_default_schema()

    table1_path = Path(table1).expanduser()
    table2_path = Path(table2).expanduser()
    table1_is_file = table1_path.exists() and table1_path.suffix.lower() in (
        ".csv",
        ".xlsx",
    )
    table2_is_file = table2_path.exists() and table2_path.suffix.lower() in (
        ".csv",
        ".xlsx",
    )

    if table1_is_file != table2_is_file:
        raise typer.BadParameter(
            "Both table arguments must be table names or file paths."
        )

    if table1_is_file and table2_is_file:
        connection_id = connection
        if connection_id is None:
            DB_TEST_DB.parent.mkdir(parents=True, exist_ok=True)
            db_path = DB_TEST_DB.parent / f"table_compare_{uuid.uuid4().hex}.duckdb"
            connection_id = f"duckdb:///{db_path}"

        safe_prev = "".join(c if c.isalnum() else "_" for c in table1_path.stem)
        safe_new = "".join(c if c.isalnum() else "_" for c in table2_path.stem)
        suffix = uuid.uuid4().hex[:8]
        table1 = f"{safe_prev}_{suffix}"
        table2 = f"{safe_new}_{suffix}"

        try:
            from sqlcompare.db import DBConnection

            with DBConnection(connection_id) as db:
                db.create_table_from_file(table1, table1_path)
                db.create_table_from_file(table2, table2_path)
        except Exception as exc:
            raise typer.BadParameter(str(exc)) from exc

        connection = connection_id

    # Parse IDs
    id_cols = [x.strip() for x in ids.split(",")]

    # Generate a test name based on tables
    safe_t1 = "".join(c if c.isalnum() else "_" for c in table1)
    safe_t2 = "".join(c if c.isalnum() else "_" for c in table2)
    test_name = f"compare_{safe_t1}_{safe_t2}"

    # Run comparison
    comparator = DatabaseComparator(connection)
    diff_id = comparator.compare(table1, table2, id_cols, test_name, schema)
    log.info(f"🔎 To review the diff, run: sqlcompare analyze-diff {diff_id}")
    log.info(
        "💡 Tips: --stats for per-column counts, --missing-current/--missing-previous for row-only, "
        "--column <name> to filter, --list-columns to inspect available fields."
    )
