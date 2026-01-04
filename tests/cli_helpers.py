from __future__ import annotations

from pathlib import Path

from sqlcompare.db import DBConnection


def count_rows(db: DBConnection, query: str) -> int:
    result = db.query(f"SELECT COUNT(*) FROM ({query})")
    return result[0][0] if result else 0


def seed_duckdb(db_path: Path) -> None:
    with DBConnection(f"duckdb:///{db_path}") as db:
        db.execute("CREATE TABLE previous (id INTEGER, name VARCHAR, value INTEGER)")
        db.execute("CREATE TABLE current (id INTEGER, name VARCHAR, value INTEGER)")
        db.execute(
            "INSERT INTO previous VALUES (1, 'alpha', 10), (2, 'bravo', 20)"
        )
        db.execute(
            "INSERT INTO current VALUES (1, 'alfa', 11), (3, 'charlie', 30)"
        )


def set_cli_env(
    monkeypatch,
    config_dir: Path,
    connection_name: str,
    connection_url: str,
    schema: str = "sqlcompare",
) -> None:
    monkeypatch.setenv("SQLCOMPARE_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("SQLCOMPARE_CONN_DEFAULT", connection_name)
    monkeypatch.setenv(f"SQLCOMPARE_CONN_{connection_name.upper()}", connection_url)
    monkeypatch.setenv("SQLCOMPARE_COMPARISON_SCHEMA", schema)
