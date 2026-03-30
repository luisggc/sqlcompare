from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from sqlcompare.cli import app
from sqlcompare.config import load_test_runs
from tests.cli_helpers import seed_duckdb, set_cli_env, setup_duckdb_env


def test_run_command_inline_sql(tmp_path, monkeypatch) -> None:
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
            "run",
            "SELECT * FROM previous",
            "SELECT * FROM current",
            "id",
            "--connection",
            "duckdb_test",
        ],
    )

    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1


def test_run_command_sql_files(tmp_path, monkeypatch) -> None:
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

    prev_sql = tmp_path / "previous.sql"
    new_sql = tmp_path / "current.sql"
    prev_sql.write_text("SELECT * FROM previous", encoding="utf-8")
    new_sql.write_text("SELECT * FROM current", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run",
            str(prev_sql),
            str(new_sql),
            "id",
            "--connection",
            "duckdb_test",
        ],
    )

    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1


def test_run_command_sql_file_empty(tmp_path, monkeypatch) -> None:
    setup_duckdb_env(monkeypatch, tmp_path / "config")
    runner = CliRunner()

    empty_sql = tmp_path / "empty.sql"
    empty_sql.write_text("", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "run",
            str(empty_sql),
            "current",
            "id",
        ],
    )

    assert result.exit_code != 0
    assert "SQL file is empty" in result.output


def test_run_command_sql_and_table(tmp_path, monkeypatch) -> None:
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
            "run",
            "SELECT * FROM previous",
            "current",
            "id",
            "--connection",
            "duckdb_test",
        ],
    )

    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1


def test_run_command_with_clause(tmp_path, monkeypatch) -> None:
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
            "run",
            "WITH cte AS (SELECT * FROM previous) SELECT * FROM cte",
            "WITH cte AS (SELECT * FROM current) SELECT * FROM cte",
            "id",
            "--connection",
            "duckdb_test",
        ],
    )

    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1


def test_run_command_inline_sql_with_whitespace(tmp_path, monkeypatch) -> None:
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
            "run",
            "  \n  SELECT * FROM previous;\n",
            "\tWITH cte AS (SELECT * FROM current) SELECT * FROM cte;",
            "id",
            "--connection",
            "duckdb_test",
        ],
    )

    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1


def test_run_command_sql_and_file_disallowed(tmp_path, monkeypatch) -> None:
    setup_duckdb_env(monkeypatch, tmp_path / "config")
    runner = CliRunner()

    dataset_dir = Path(__file__).parent / "datasets" / "row_compare"
    previous_csv = dataset_dir / "previous.csv"

    result = runner.invoke(
        app,
        [
            "run",
            str(previous_csv),
            "SELECT * FROM current",
            "id",
        ],
    )

    assert result.exit_code != 0
    assert "Both table arguments must be file paths" in result.output


def test_run_command_unsupported_file(tmp_path, monkeypatch) -> None:
    setup_duckdb_env(monkeypatch, tmp_path / "config")
    runner = CliRunner()

    bad_path = tmp_path / "bad.txt"
    bad_path.write_text("not a sql file", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "run",
            str(bad_path),
            "current",
            "id",
        ],
    )

    assert result.exit_code != 0
    assert "Unsupported file type" in result.output


def test_run_command_sql_requires_connection(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SQLCOMPARE_CONN_DEFAULT", raising=False)
    monkeypatch.delenv("DTK_CONN_DEFAULT", raising=False)
    setup_duckdb_env(monkeypatch, tmp_path / "config")

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run",
            "SELECT * FROM previous",
            "SELECT * FROM current",
            "id",
        ],
    )

    assert result.exit_code != 0
    assert "No connection specified" in result.output


def test_run_command_default_connection_env(tmp_path, monkeypatch) -> None:
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
            "run",
            "SELECT * FROM previous",
            "SELECT * FROM current",
            "id",
        ],
    )

    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1


def test_run_command_schema_prefix_for_sql(tmp_path, monkeypatch) -> None:
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
            "run",
            "SELECT * FROM previous",
            "SELECT * FROM current",
            "id",
            "--connection",
            "duckdb_test",
            "--schema",
            "analysis_schema",
        ],
    )

    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1
    run = next(iter(runs.values()))
    assert run["tables"]["previous"].startswith("analysis_schema.")
    assert run["tables"]["new"].startswith("analysis_schema.")
