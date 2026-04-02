from __future__ import annotations

import typer

from sqlcompare.utils.test_types.stats import compare_table_stats


def run_stats_cmd(
    previous: str = typer.Argument(
        ..., help="Previous table name or CSV/XLSX file path"
    ),
    current: str = typer.Argument(..., help="Current table name or CSV/XLSX file path"),
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
        sqlcompare run stats analytics.users analytics.users_new

        # Compare CSV file statistics
        sqlcompare run stats exports/jan.csv exports/feb.csv

        # Compare with specific connection
        sqlcompare run stats orders_v1 orders_v2 -c postgres_prod

    Output Columns:
        PREV_ROWS, NEW_ROWS: Total row counts
        PREV_NULLS, NEW_NULLS: Null counts per column
        PREV_DISTINCT, NEW_DISTINCT: Unique value counts
        *_DIFF: Calculated differences
    """
    compare_table_stats(previous, current, connection)
