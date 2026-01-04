from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from sqlcompare.cli import app
from tests.cli_helpers import seed_duckdb, set_cli_env


def _prepare_db(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "sqlcompare.duckdb"
    seed_duckdb(db_path)
    config_dir = tmp_path / "config"
    set_cli_env(
        monkeypatch,
        config_dir,
        "duckdb_query",
        f"duckdb:///{db_path}",
    )


def test_query_command_terminal_output(tmp_path, monkeypatch) -> None:
    _prepare_db(tmp_path, monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "query",
            "SELECT id, name FROM previous ORDER BY id",
            "--connection",
            "duckdb_query",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Total rows: 2" in result.output


def test_query_command_csv_output(tmp_path, monkeypatch) -> None:
    _prepare_db(tmp_path, monkeypatch)
    runner = CliRunner()

    with runner.isolated_filesystem():
        result = runner.invoke(
            app,
            [
                "query",
                "SELECT id, value FROM current ORDER BY id",
                "--connection",
                "duckdb_query",
                "--output",
                "results.csv",
            ],
        )

        assert result.exit_code == 0, result.output
        assert Path("results.csv").exists()


def test_query_command_rejects_xlsx_output(tmp_path, monkeypatch) -> None:
    _prepare_db(tmp_path, monkeypatch)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "query",
            "SELECT id FROM previous",
            "--connection",
            "duckdb_query",
            "--output",
            "results.xlsx",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "XLSX output is not supported" in result.output
