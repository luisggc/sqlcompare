from __future__ import annotations

from pathlib import Path

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
            "run",
            "stats",
            "previous",
            "current",
            "--connection",
            "duckdb_test",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Table statistics comparison" in result.output

    lines = [line for line in result.output.splitlines() if line.strip()]
    assert lines, result.output

    header = []
    rows: dict[str, list[str]] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        cells = stripped.split()
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
        "STATUS_PCT",
    ]
    assert header == expected_header

    for col_name in ("id", "name", "value"):
        assert col_name in rows, result.output
        assert rows[col_name][1:] == [
            "2",
            "2",
            "0",
            "0",
            "2",
            "2",
            "0",
            "0",
            "0",
            "100.0",
        ]


def test_table_command_with_stats_from_files() -> None:
    runner = CliRunner()
    base = Path(__file__).resolve().parent
    prev_path = base / "datasets" / "stats_compare" / "previous.csv"
    curr_path = base / "datasets" / "stats_compare" / "current.csv"

    result = runner.invoke(
        app,
        [
            "run",
            "stats",
            str(prev_path),
            str(curr_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Table statistics comparison" in result.output


def test_stats_command_with_inline_sql(tmp_path, monkeypatch) -> None:
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
            "stats",
            "SELECT * FROM previous",
            "SELECT * FROM current",
            "--connection",
            "duckdb_test",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Table statistics comparison" in result.output


def test_stats_command_with_sql_files(tmp_path, monkeypatch) -> None:
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
            "stats",
            str(prev_sql),
            str(new_sql),
            "--connection",
            "duckdb_test",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Table statistics comparison" in result.output


def test_stats_command_sql_requires_connection(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SQLCOMPARE_CONN_DEFAULT", raising=False)
    monkeypatch.delenv("DTK_CONN_DEFAULT", raising=False)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "run",
            "stats",
            "SELECT * FROM previous",
            "SELECT * FROM current",
        ],
    )

    assert result.exit_code != 0
    assert "No connection specified" in str(result.exception)
