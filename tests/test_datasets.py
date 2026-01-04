from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
import yaml

from sqlcompare.compare.comparator import DatabaseComparator
from sqlcompare.config import load_test_runs
from sqlcompare.db import DBConnection
from tests.cli_helpers import count_rows


def _load_dataset_config(path: Path) -> dict:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    base_dir = path.parent

    def expand(value):
        if isinstance(value, dict):
            return {key: expand(item) for key, item in value.items()}
        if isinstance(value, list):
            return [expand(item) for item in value]
        if isinstance(value, str):
            return value.replace("{{here}}", str(base_dir))
        return value

    return expand(payload)


def _create_table_from_file(db: DBConnection, table: str, file_path: str):
    db.create_table_from_file(table, file_path)


def _stats_to_dict(rows: list[tuple]) -> dict[str, int]:
    return {str(row[0]).upper(): row[1] for row in rows}


@pytest.fixture
def config_dir(tmp_path, monkeypatch) -> Path:
    config_path = tmp_path / "config"
    monkeypatch.setenv("SQLCOMPARE_CONFIG_DIR", str(config_path))
    return config_path


def _assert_common_diffs(comparator: DatabaseComparator, db: DBConnection):
    diff_count = count_rows(db, comparator.get_diff_query())
    assert diff_count == 4

    stats_query = comparator.get_stats_query()
    rows, columns = db.query(stats_query, include_columns=True)
    assert columns == ["COLUMN_NAME", "diff_count"]
    stats = _stats_to_dict(rows)
    assert stats["NAME"] == 2
    assert stats["AGE"] == 1
    assert stats["BDAY"] == 1

    missing_current = count_rows(db, comparator.get_in_current_only_query())
    missing_previous = count_rows(db, comparator.get_in_previous_only_query())
    assert missing_current == 1
    assert missing_previous == 1


def test_csv_dataset_compare(config_dir: Path, tmp_path: Path) -> None:
    dataset_path = Path(__file__).parent / "datasets" / "csv_test" / "test.yaml"
    dataset = _load_dataset_config(dataset_path)

    db_file = tmp_path / "test.duckdb"
    conn_url = f"duckdb:///{db_file}"

    with DBConnection(conn_url) as db:
        _create_table_from_file(db, "previous", dataset["previous"]["file_name"])
        _create_table_from_file(db, "current", dataset["new"]["file_name"])

    comparator = DatabaseComparator(conn_url)
    diff_id = comparator.compare(
        "previous",
        "current",
        dataset["previous"]["index"],
        test_name="compare_csv_test",
        test_schema="sqlcompare",
    )

    runs = load_test_runs()
    assert diff_id in runs

    with DBConnection(conn_url) as db:
        _assert_common_diffs(comparator, db)


def test_xlsx_dataset_compare(config_dir: Path, tmp_path: Path) -> None:
    dataset_path = Path(__file__).parent / "datasets" / "xlsx_test" / "test.yaml"
    dataset = _load_dataset_config(dataset_path)

    db_file = tmp_path / "test.duckdb"
    conn_url = f"duckdb:///{db_file}"

    with DBConnection(conn_url) as db:
        try:
            _create_table_from_file(db, "previous", dataset["previous"]["file_name"])
            _create_table_from_file(db, "current", dataset["new"]["file_name"])
        except (RuntimeError, duckdb.CatalogException, FileNotFoundError) as exc:
            pytest.skip(str(exc))

    comparator = DatabaseComparator(conn_url)
    diff_id = comparator.compare(
        "previous",
        "current",
        dataset["previous"]["index"],
        test_name="compare_xlsx_test",
        test_schema="sqlcompare",
    )

    runs = load_test_runs()
    assert diff_id in runs

    with DBConnection(conn_url) as db:
        _assert_common_diffs(comparator, db)


def test_duckdb_dataset_compare(config_dir: Path, tmp_path: Path) -> None:
    dataset_path = Path(__file__).parent / "datasets" / "duckdb_test" / "test.yaml"
    dataset = _load_dataset_config(dataset_path)

    db_file = tmp_path / "test.duckdb"
    conn_url = f"duckdb:///{db_file}"

    with DBConnection(conn_url) as db:
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

    comparator = DatabaseComparator(conn_url)
    diff_id = comparator.compare(
        "previous_table",
        "new_table",
        dataset["previous"]["index"],
        test_name="compare_duckdb_test",
        test_schema="sqlcompare",
    )

    runs = load_test_runs()
    assert diff_id in runs

    with DBConnection(conn_url) as db:
        _assert_common_diffs(comparator, db)
