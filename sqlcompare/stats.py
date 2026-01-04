from __future__ import annotations

import typer

from sqlcompare.utils.test_types.stats import compare_table_stats


def stats_cmd(
    table1: str = typer.Argument(..., help="Previous table name or CSV/XLSX file path"),
    table2: str = typer.Argument(..., help="Current table name or CSV/XLSX file path"),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Database connector name"
    ),
) -> None:
    """Compare table statistics without row-by-row comparison.

    Displays column-level statistics including:
    - Row counts
    - Null counts
    - Distinct value counts
    - Differences between previous and current

    This is useful for:
    - Quick data quality checks
    - Schema validation
    - High-level change detection
    - Large tables where row comparison would be slow

    Examples:
        # Compare table statistics
        sqlcompare stats analytics.users analytics.users_new

        # Compare CSV file statistics
        sqlcompare stats exports/jan.csv exports/feb.csv

        # Compare with specific connection
        sqlcompare stats orders_v1 orders_v2 -c postgres_prod

    Output Columns:
        PREV_ROWS, NEW_ROWS: Total row counts
        PREV_NULLS, NEW_NULLS: Null counts per column
        PREV_DISTINCT, NEW_DISTINCT: Unique value counts
        *_DIFF: Calculated differences
    """
    compare_table_stats(table1, table2, connection)
