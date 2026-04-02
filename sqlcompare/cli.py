from __future__ import annotations

import typer

from sqlcompare.query import query_cmd
from sqlcompare.history import history_cmd
from sqlcompare.review_cmds import (
    review_columns_cmd,
    review_diff_cmd,
    review_export_cmd,
    review_meta_cmd,
    review_missing_cmd,
    review_stats_cmd,
)
from sqlcompare.run import run_unified_cmd

app = typer.Typer(help="Compare datasets and review diffs.")

review_app = typer.Typer(help="Review diff results and export reports.")

app.command("run")(run_unified_cmd)
app.add_typer(review_app, name="review")
app.command("history")(history_cmd)

app.command("query")(query_cmd)

review_app.command("diff")(review_diff_cmd)
review_app.command("stats")(review_stats_cmd)
review_app.command("missing")(review_missing_cmd)
review_app.command("columns")(review_columns_cmd)
review_app.command("export")(review_export_cmd)
review_app.command("meta")(review_meta_cmd)

def main() -> None:
    app()
