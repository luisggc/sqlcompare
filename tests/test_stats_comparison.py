from __future__ import annotations

from typer.testing import CliRunner

from sqlcompare.cli import app
from tests.cli_helpers import seed_duckdb, set_cli_env


def test_table_command_with_stats(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "sqlcompare.duckdb"
    seed_duckdb(db_path)
    config_dir = tmp_path / "config"
    set_cli_env(
        monkeypatch,
        config_dir,
        "duckdb_test",
        f"duckdb:///{db_path}",
        schema="analysis_schema",
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "table",
            "previous",
            "current",
            "--connection",
            "duckdb_test",
            "--schema",
            "analysis_schema",
            "--stats",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Table statistics comparison" in result.output

    lines = [line for line in result.output.splitlines() if line.startswith("|")]
    assert lines, result.output

    header = []
    rows = {}
    for line in lines:
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if not cells:
            continue
        if cells[0] == "COLUMN_NAME":
            header = cells
            continue
        rows[cells[0]] = cells

    expected_header = [
        "COLUMN_NAME",
        "PREV_ROWS",
        "NEW_ROWS",
        "PREV_NULLS",
        "NEW_NULLS",
        "PREV_DISTINCT",
        "NEW_DISTINCT",
        "ROWS_DIFF",
        "NULLS_DIFF",
        "DISTINCT_DIFF",
    ]
    assert header == expected_header

    for col_name in ("id", "name", "value"):
        assert col_name in rows, result.output
        assert rows[col_name][1:] == ["2", "2", "0", "0", "2", "2", "0", "0", "0"]
