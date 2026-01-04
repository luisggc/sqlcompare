from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from sqlcompare.cli import app
from sqlcompare.config import load_test_runs
from tests.cli_helpers import seed_duckdb, set_cli_env


def test_table_command_with_options(tmp_path, monkeypatch) -> None:
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
            "id",
            "--connection",
            "duckdb_test",
            "--schema",
            "analysis_schema",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "analyze-diff" in result.output
    runs = load_test_runs()
    assert len(runs) == 1
