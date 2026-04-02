from __future__ import annotations

import typer

from sqlcompare.meta import emit_meta
from sqlcompare.review import review_diff


def review_diff_cmd(
    diff_id: str = typer.Argument(..., help="Diff run ID"),
    column: str | None = typer.Option(
        None, "--column", "-c", help="Filter by specific column name"
    ),
    limit: int = typer.Option(25, "--limit", "-l", help="Limit results to display"),
) -> None:
    """Review the standard diff results."""
    review_diff(diff_id, column=column, limit=limit)


def review_stats_cmd(
    diff_id: str = typer.Argument(..., help="Diff run ID"),
    column: str | None = typer.Option(
        None, "--column", "-c", help="Filter by specific column name"
    ),
    limit: int = typer.Option(25, "--limit", "-l", help="Limit results to display"),
) -> None:
    """Review column-level statistics for a diff."""
    review_diff(diff_id, column=column, limit=limit, stats=True)


def review_missing_cmd(
    diff_id: str = typer.Argument(..., help="Diff run ID"),
    side: str = typer.Option(
        ...,
        "--side",
        help="Which side to show: current or previous",
    ),
    limit: int = typer.Option(25, "--limit", "-l", help="Limit results to display"),
) -> None:
    """Review rows that exist only on one side."""
    side_value = side.lower().strip()
    if side_value not in {"current", "previous"}:
        raise typer.BadParameter("Invalid --side value. Use 'current' or 'previous'.")
    review_diff(
        diff_id,
        limit=limit,
        missing_current=side_value == "current",
        missing_previous=side_value == "previous",
    )


def review_columns_cmd(
    diff_id: str = typer.Argument(..., help="Diff run ID"),
    limit: int = typer.Option(25, "--limit", "-l", help="Limit results to display"),
) -> None:
    """List available columns in the diff data."""
    review_diff(diff_id, limit=limit, list_columns=True)


def review_export_cmd(
    diff_id: str = typer.Argument(..., help="Diff run ID"),
    mode: str = typer.Option(
        "summary",
        "--mode",
        help="Report mode: summary or complete",
    ),
    output: str | None = typer.Option(
        None,
        "--output",
        help="Output path for the saved report (.xlsx). Defaults to current directory.",
    ),
    column: str | None = typer.Option(
        None, "--column", "-c", help="Filter by specific column name"
    ),
) -> None:
    """Export diff results to an XLSX report."""
    save_mode = mode.lower().strip()
    if save_mode not in {"summary", "complete"}:
        raise typer.BadParameter("Invalid --mode. Use 'summary' or 'complete'.")
    review_diff(
        diff_id,
        column=column,
        save=save_mode,
        file_path=output,
    )


def review_meta_cmd(
    diff_id: str = typer.Argument(..., help="Diff run ID"),
) -> None:
    """Emit JSON metadata for AI/LLM analysis."""
    emit_meta(diff_id)
