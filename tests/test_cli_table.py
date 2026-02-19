from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from sqlcompare.cli import app
from sqlcompare.config import load_test_runs
from tests.cli_helpers import seed_duckdb, set_cli_env, setup_duckdb_env


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
    assert "inspect" in result.output
    runs = load_test_runs()
    assert len(runs) == 1


def test_table_command_with_files(tmp_path, monkeypatch) -> None:
    setup_duckdb_env(monkeypatch, tmp_path / "config")
    runner = CliRunner()

    dataset_dir = Path(__file__).parent / "datasets" / "row_compare"
    previous_csv = dataset_dir / "previous.csv"
    current_csv = dataset_dir / "current.csv"

    result = runner.invoke(
        app,
        ["table", str(previous_csv), str(current_csv), "id"],
    )

    assert result.exit_code == 0, result.output
    assert "inspect" in result.output
    runs = load_test_runs()
    assert len(runs) == 1


def test_table_command_ignore_columns_option(tmp_path, monkeypatch) -> None:
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
            "--ignore-columns",
            "value",
        ],
    )

    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1
    run = next(iter(runs.values()))
    assert run["common_cols"] == ["name"]


def test_table_command_columns_option(tmp_path, monkeypatch) -> None:
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
            "--columns",
            "value",
        ],
    )

    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1
    run = next(iter(runs.values()))
    assert run["common_cols"] == ["value"]
