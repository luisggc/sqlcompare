from __future__ import annotations

import typer

from sqlcompare.analyze_diff import analyze_diff_cmd
from sqlcompare.dataset import dataset_cmd
from sqlcompare.list_diffs import list_diffs_cmd
from sqlcompare.query import query_cmd
from sqlcompare.table import table_cmd

app = typer.Typer(help="Compare database tables and inspect diffs.")

# Register all commands
app.command("table")(table_cmd)
app.command("analyze-diff")(analyze_diff_cmd)
app.command("list-diffs")(list_diffs_cmd)
app.command("query")(query_cmd)
app.command("dataset")(dataset_cmd)


def main() -> None:
    app()
