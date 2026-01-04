from __future__ import annotations

from pathlib import Path

import pytest

from sqlcompare.compare.comparator import DatabaseComparator
from sqlcompare.db import DBConnection
from tests.cli_helpers import count_rows


@pytest.fixture
def config_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    config_path = tmp_path / "config"
    monkeypatch.setenv("SQLCOMPARE_CONFIG_DIR", str(config_path))
    return config_path


def test_csv_compare_between_two_files(config_dir: Path, tmp_path: Path) -> None:
    dataset_dir = Path(__file__).parent / "datasets" / "csv_compare"
    previous_csv = dataset_dir / "previous.csv"
    current_csv = dataset_dir / "current.csv"

    db_file = tmp_path / "test.duckdb"
    conn_url = f"duckdb:///{db_file}"
    with DBConnection(conn_url) as db:
        db.create_table_from_file("previous", previous_csv)
        db.create_table_from_file("current", current_csv)

    comparator = DatabaseComparator(conn_url)
    comparator.compare(
        "previous",
        "current",
        ["id"],
        test_name="compare_csv_files",
        test_schema="sqlcompare",
    )

    with DBConnection(conn_url) as db:
        diff_count = count_rows(db, comparator.get_diff_query())
        assert diff_count == 2

        stats_rows, _ = db.query(comparator.get_stats_query(), include_columns=True)
        stats = {str(row[0]).upper(): row[1] for row in stats_rows}
        assert stats["NAME"] == 1
        assert stats["AMOUNT"] == 1

        missing_current = count_rows(db, comparator.get_in_current_only_query())
        missing_previous = count_rows(db, comparator.get_in_previous_only_query())
        assert missing_current == 1
        assert missing_previous == 1
