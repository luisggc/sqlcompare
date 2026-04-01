from __future__ import annotations

from pathlib import Path

import json

from typer.testing import CliRunner

from sqlcompare.cli import app
from sqlcompare.config import load_test_runs
from tests.cli_helpers import seed_duckdb, set_cli_env


def _create_diff(tmp_path: Path, monkeypatch) -> str:
    db_path = tmp_path / "sqlcompare.duckdb"
    seed_duckdb(db_path)
    config_dir = tmp_path / "config"
    set_cli_env(
        monkeypatch,
        config_dir,
        "duckdb_test",
        f"duckdb:///{db_path}",
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
        ],
    )
    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1
    return next(iter(runs.keys()))


def test_diff_queries_lists_queryable_tables(tmp_path, monkeypatch) -> None:
    diff_id = _create_diff(tmp_path, monkeypatch)
    runner = CliRunner()

    result = runner.invoke(app, ["diff-queries", diff_id])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["diff_id"] == diff_id
    assert payload["tables"]
    table_names = [entry["name"] for entry in payload["tables"]]
    assert "previous" in table_names
    assert "current" in table_names
    assert any(name and name.endswith("_join") for name in table_names)
    assert payload["index_columns"] == ["id"]
    assert "name" in payload["common_columns"]
    assert "value" in payload["common_columns"]
    join_entry = next(entry for entry in payload["tables"] if entry["role"] == "join")
    assert "id_previous" in join_entry["columns"]
    assert "value_new" in join_entry["columns"]
    assert payload["queries"]
