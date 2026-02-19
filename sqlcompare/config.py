from __future__ import annotations

import os
from pathlib import Path
from typing import Any
import yaml


def _config_dir() -> Path:
    return Path(
        os.getenv("SQLCOMPARE_CONFIG_DIR", Path.home() / ".config" / "sqlcompare")
    )


def get_default_schema() -> str:
    return os.getenv("SQLCOMPARE_COMPARISON_SCHEMA", "sqlcompare")


def load_config() -> dict[str, Any]:
    return {
        "default_connection": os.getenv("SQLCOMPARE_CONN_DEFAULT"),
        "db_test_schema": get_default_schema(),
        "sqlcompare_dir": str(_config_dir().parent),
    }


def _runs_dir() -> Path:
    return _config_dir() / "runs"


def get_tests_folder() -> Path:
    return _config_dir() / "db" / "tests"


DB_TEST_DB = _config_dir() / "db_test.duckdb"


def load_test_runs() -> dict[str, Any]:
    runs_dir = _runs_dir()
    if not runs_dir.exists():
        return {}

    runs = {}
    for yaml_file in runs_dir.glob("*.yaml"):
        run_id = yaml_file.stem  # filename without extension
        payload = _read_yaml(yaml_file)
        if payload:
            runs[run_id] = payload

    return runs


def save_test_runs(runs: dict[str, Any]) -> None:
    runs_dir = _runs_dir()
    runs_dir.mkdir(parents=True, exist_ok=True)

    for run_id, run_data in runs.items():
        file_path = runs_dir / f"{run_id}.yaml"
        _write_yaml(file_path, run_data)


def _read_yaml(path: Path) -> dict[str, Any] | None:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")
