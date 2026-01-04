import typer

from sqlcompare.config import get_default_schema
from sqlcompare.log import log

from .comparator import DatabaseComparator


def compare_table(
    table1: str, table2: str, ids: str, connection: str | None, schema: str | None
) -> None:
    """Compare two tables in the database.

    TABLE1 and TABLE2 are the names of the tables to compare.
    IDS is a comma-separated list of column names to use as the unique key.
    """
    schema = schema or get_default_schema()

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
