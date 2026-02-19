from __future__ import annotations

import os
from pathlib import Path

import yaml
from typer.testing import CliRunner

from sqlcompare.cli import app
from sqlcompare.config import load_test_runs
from sqlcompare.db import DBConnection
from tests.cli_helpers import setup_duckdb_env


def _seed_duckdb(db_path: Path) -> None:
    with DBConnection(f"duckdb:///{db_path}") as db:
        db.execute(
            "CREATE TABLE previous_table (id INTEGER, name VARCHAR, age INTEGER, bday DATE)"
        )
        db.execute(
            "CREATE TABLE new_table (id INTEGER, name VARCHAR, age INTEGER, bday DATE)"
        )
        db.execute(
            "INSERT INTO previous_table VALUES (1, 'Alice', 30, '1990-01-01'), (2, 'Bob', 40, '1985-05-05'), (3, 'Carol', 25, '1995-07-07'), (4, 'Dan', 50, '1980-12-12')"
        )
        db.execute(
            "INSERT INTO new_table VALUES (1, 'Alice', 30, '1990-01-01'), (2, 'Bobby', 45, '1985-05-05'), (3, 'Caroline', 25, '1995-07-08'), (5, 'Erin', 28, '1992-02-02')"
        )


def _env(config_dir: Path, connection_name: str, connection_url: str) -> dict[str, str]:
    return {
        "SQLCOMPARE_CONFIG_DIR": str(config_dir),
        "SQLCOMPARE_CONN_DEFAULT": connection_name,
        f"SQLCOMPARE_CONN_{connection_name.upper()}": connection_url,
        "SQLCOMPARE_COMPARISON_SCHEMA": "sqlcompare",
    }


def test_dataset_command_select_sql(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "dataset.duckdb"
    _seed_duckdb(db_path)

    env = _env(tmp_path / "config", "duckdb_test", f"duckdb:///{db_path}")
    monkeypatch.setenv("SQLCOMPARE_CONFIG_DIR", env["SQLCOMPARE_CONFIG_DIR"])
    monkeypatch.setenv(
        "SQLCOMPARE_CONN_DUCKDB_TEST", env["SQLCOMPARE_CONN_DUCKDB_TEST"]
    )

    runner = CliRunner()
    dataset_path = Path(__file__).parent / "datasets" / "duckdb_test" / "test.yaml"

    result = runner.invoke(
        app,
        [
            "dataset",
            str(dataset_path),
            "--connection",
            "duckdb_test",
            "--schema",
            "sqlcompare",
        ],
    )
    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1
    run = next(iter(runs.values()))
    assert "sqlcompare." in run["tables"]["previous"]
    assert "sqlcompare." in run["tables"]["new"]


def test_dataset_command_file_name(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "dataset.duckdb"

    env = _env(tmp_path / "config", "duckdb", f"duckdb:///{db_path}")
    monkeypatch.setenv("SQLCOMPARE_CONFIG_DIR", env["SQLCOMPARE_CONFIG_DIR"])
    monkeypatch.setenv("SQLCOMPARE_CONN_DUCKDB", env["SQLCOMPARE_CONN_DUCKDB"])

    runner = CliRunner()
    dataset_path = Path(__file__).parent / "datasets" / "csv_test" / "test.yaml"

    result = runner.invoke(
        app,
        [
            "dataset",
            str(dataset_path),
            "--connection",
            "duckdb",
            "--schema",
            "sqlcompare",
        ],
    )
    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1


def test_dataset_command_with_yaml_dataset(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "dataset.duckdb"

    env = _env(tmp_path / "config", "duckdb", f"duckdb:///{db_path}")
    monkeypatch.setenv("SQLCOMPARE_CONFIG_DIR", env["SQLCOMPARE_CONFIG_DIR"])
    monkeypatch.setenv("SQLCOMPARE_CONN_DUCKDB", env["SQLCOMPARE_CONN_DUCKDB"])

    runner = CliRunner()
    dataset_path = Path(__file__).parent / "datasets" / "row_compare" / "dataset.yaml"

    result = runner.invoke(
        app,
        [
            "dataset",
            str(dataset_path),
            "--connection",
            "duckdb",
            "--schema",
            "sqlcompare",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "inspect" in result.output
    runs = load_test_runs()
    assert len(runs) == 1


def test_dataset_command_file_name_without_connection(
    tmp_path: Path, monkeypatch
) -> None:
    setup_duckdb_env(monkeypatch, tmp_path / "config")

    runner = CliRunner()
    dataset_path = Path(__file__).parent / "datasets" / "row_compare" / "dataset.yaml"

    result = runner.invoke(app, ["dataset", str(dataset_path)])

    assert result.exit_code == 0, result.output
    assert "inspect" in result.output
    runs = load_test_runs()
    assert len(runs) == 1


def test_dataset_command_rejects_mismatched_connectors(
    tmp_path: Path, monkeypatch
) -> None:
    dataset_path = tmp_path / "dataset.yaml"
    dataset_path.write_text(
        yaml.safe_dump(
            {
                "previous": {
                    "conn": "a",
                    "select_sql": "SELECT 1 AS id",
                    "index": ["id"],
                },
                "new": {"conn": "b", "select_sql": "SELECT 1 AS id", "index": ["id"]},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SQLCOMPARE_CONFIG_DIR", str(tmp_path / "config"))
    monkeypatch.setenv("SQLCOMPARE_CONN_A", "duckdb://")
    monkeypatch.setenv("SQLCOMPARE_CONN_B", "duckdb://")

    runner = CliRunner()

    result = runner.invoke(app, ["dataset", str(dataset_path)])

    assert result.exit_code != 0
    assert "connector" in result.output.lower()


def test_dataset_command_rejects_mismatched_indexes(
    tmp_path: Path, monkeypatch
) -> None:
    dataset_path = tmp_path / "dataset.yaml"
    dataset_path.write_text(
        yaml.safe_dump(
            {
                "previous": {"select_sql": "SELECT 1 AS id", "index": ["id"]},
                "new": {"select_sql": "SELECT 1 AS id", "index": ["other_id"]},
            }
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "dataset.duckdb"

    env = _env(tmp_path / "config", "duckdb", f"duckdb:///{db_path}")
    monkeypatch.setenv("SQLCOMPARE_CONFIG_DIR", env["SQLCOMPARE_CONFIG_DIR"])
    monkeypatch.setenv("SQLCOMPARE_CONN_DUCKDB", env["SQLCOMPARE_CONN_DUCKDB"])

    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "dataset",
            str(dataset_path),
            "--connection",
            "duckdb",
            "--schema",
            "sqlcompare",
        ],
    )

    assert result.exit_code != 0
    assert "index" in result.output.lower()
