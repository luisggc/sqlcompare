from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from sqlcompare.cli import app
from sqlcompare.config import load_test_runs
from tests.cli_helpers import set_cli_env


def test_dataset_command_with_connection_and_schema(tmp_path, monkeypatch) -> None:
    dataset_path = tmp_path / "dataset.yaml"
    dataset_path.write_text(
        yaml.safe_dump(
            {
                "previous": {
                    "select_sql": "SELECT 1 AS id, 'alpha' AS name",
                    "index": ["id"],
                },
                "new": {
                    "select_sql": "SELECT 1 AS id, 'alfa' AS name",
                    "index": ["id"],
                },
            }
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "dataset.duckdb"
    config_dir = tmp_path / "config"
    set_cli_env(
        monkeypatch,
        config_dir,
        "duckdb_dataset",
        f"duckdb:///{db_path}",
        schema="dataset_schema",
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "dataset",
            str(dataset_path),
            "--connection",
            "duckdb_dataset",
            "--schema",
            "dataset_schema",
        ],
    )

    assert result.exit_code == 0, result.output
    runs = load_test_runs()
    assert len(runs) == 1
