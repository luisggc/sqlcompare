from __future__ import annotations

import typer

from sqlcompare.compare.analyze import analyze_diff
from sqlcompare.compare.listing import list_diffs
from sqlcompare.compare.table import compare_table
from sqlcompare.dataset import compare_dataset
from sqlcompare.query import query

app = typer.Typer(help="Compare database tables and inspect diffs.")


@app.command("table")
def table(
    table1: str = typer.Argument(..., help="Previous table name"),
    table2: str = typer.Argument(..., help="Current table name"),
    ids: str | None = typer.Argument(
        None, help="Comma-separated list of key columns (required unless --stats)"
    ),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Database connector name"
    ),
    schema: str | None = typer.Option(None, "--schema", help="Schema for test tables"),
    stats: bool = typer.Option(
        False, "--stats", help="Compare tables statistically without joining rows"
    ),
) -> None:
    compare_table(table1, table2, ids, connection, schema, stats=stats)


@app.command("analyze-diff")
def analyze(
    diff_id: str = typer.Argument(..., help="Diff run ID"),
    column: str | None = typer.Option(
        None, "--column", "-c", help="Filter by specific column name"
    ),
    limit: int = typer.Option(25, "--limit", "-l", help="Limit results to display"),
    save: bool = typer.Option(
        False, "--save", help="Save filtered results to CSV file"
    ),
    list_columns: bool = typer.Option(
        False, "--list-columns", help="List available columns in the diff data"
    ),
    stats: bool = typer.Option(
        False, "--stats", help="Show statistics table instead of differences"
    ),
    missing_current: bool = typer.Option(
        False, "--missing-current", help="Show rows only in current dataset"
    ),
    missing_previous: bool = typer.Option(
        False, "--missing-previous", help="Show rows only in previous dataset"
    ),
) -> None:
    analyze_diff(
        diff_id,
        column=column,
        limit=limit,
        save=save,
        list_columns=list_columns,
        stats=stats,
        missing_current=missing_current,
        missing_previous=missing_previous,
    )


@app.command("list-diffs")
def list_diffs_cmd(
    pattern: str | None = typer.Argument(None, help="Match diff IDs"),
    test: str | None = typer.Option(None, "--test", help="Filter by test name"),
) -> None:
    list_diffs(pattern, test)


@app.command("query")
def query_cmd(
    q: str = typer.Argument(..., help="SQL query to run"),
    connection: str | None = typer.Option(
        None, "--connection", "-c", "--conn", help="Connector name"
    ),
    output: str = typer.Option(
        "terminal",
        "--output",
        "-o",
        help="Output format or file path. Use 'terminal' or provide a .csv filename",
    ),
) -> None:
    query(q, connection, output)


@app.command("dataset")
def dataset_cmd(
    path: str = typer.Argument(..., help="Path to dataset YAML file"),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Database connector name"
    ),
    schema: str | None = typer.Option(
        None, "--schema", help="Schema for dataset tables"
    ),
) -> None:
    compare_dataset(path, connection, schema)


def main() -> None:
    app()
