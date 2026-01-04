from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def _config_dir() -> Path:
    return Path(
        os.getenv("SQLCOMPARE_CONFIG_DIR", Path.home() / ".config" / "data-toolkit")
    )


def get_default_schema() -> str:
    return os.getenv("SQLCOMPARE_COMPARISON_SCHEMA", "sqlcompare")


def load_config() -> dict[str, Any]:
    return {
        "default_connection": os.getenv("SQLCOMPARE_CONN_DEFAULT"),
        "db_test_schema": get_default_schema(),
        "sqlcompare_dir": str(_config_dir().parent),
    }


def _runs_file() -> Path:
    return _config_dir() / "db_test_runs.yaml"


def get_tests_folder() -> Path:
    return _config_dir() / "db" / "tests"


DB_TEST_DB = _config_dir() / "db_test.duckdb"


def load_test_runs() -> dict[str, Any]:
    path = _runs_file()
    if not path.exists():
        return {}
    payload = _read_yaml(path)
    if not payload:
        return {}
    if not isinstance(payload, dict):
        raise ValueError("Expected mapping in db_test_runs.yaml")
    return payload


def save_test_runs(runs: dict[str, Any]) -> None:
    path = _runs_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_yaml(path, runs)


def _read_yaml(path: Path) -> dict[str, Any] | None:
    try:
        import yaml
    except ImportError:
        import json

        return json.loads(path.read_text(encoding="utf-8"))
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    try:
        import yaml
    except ImportError:
        import json

        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return
    path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")
