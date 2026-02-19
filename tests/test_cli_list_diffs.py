from __future__ import annotations

import yaml
from typer.testing import CliRunner

from sqlcompare.cli import app
from sqlcompare.config import load_test_runs
from tests.cli_helpers import seed_duckdb, set_cli_env


def _prepare_run(tmp_path, monkeypatch) -> str:
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


def test_list_diffs_with_filters(tmp_path, monkeypatch) -> None:
    diff_id = _prepare_run(tmp_path, monkeypatch)
    runner = CliRunner()

    result = runner.invoke(app, ["list-diffs", diff_id[:8], "--test", "duckdb_test"])
    assert result.exit_code == 0, result.output
    assert diff_id[:30] in result.output

    no_match = runner.invoke(app, ["list-diffs", "no_such_diff"])
    assert no_match.exit_code == 0, no_match.output
    assert "No diff data found" in no_match.output


def test_list_diffs_filter_handles_none_conn(tmp_path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    runs_dir = config_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / "manual_run.yaml").write_text(
        yaml.safe_dump(
            {
                "tables": {"previous": "p", "new": "n", "join": "j"},
                "index_cols": ["id"],
                "cols_prev": ["id"],
                "cols_new": ["id"],
                "common_cols": [],
                "conn": None,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SQLCOMPARE_CONFIG_DIR", str(config_dir))

    runner = CliRunner()
    result = runner.invoke(app, ["list-diffs", "--test", "duckdb"])
    assert result.exit_code == 0, result.output
    assert "No diff data found" in result.output
