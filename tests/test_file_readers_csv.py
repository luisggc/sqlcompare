from __future__ import annotations

from pathlib import Path

from sqlcompare.db import DBConnection


def _write_csv(path: Path) -> None:
    path.write_text("id,name,amount\n1,alpha,10\n2,bravo,20\n", encoding="utf-8")


def test_csv_reader_returns_query_and_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    _write_csv(csv_path)

    db_path = tmp_path / "test.duckdb"
    conn_url = f"duckdb:///{db_path}"
    with DBConnection(conn_url) as db:
        db.create_table_from_file("sample", csv_path)
        columns = [
            row[1] for row in db.query("PRAGMA table_info('sample')", fetch=True)
        ]
        rows = db.query("SELECT * FROM sample ORDER BY id")

    assert columns == ["id", "name", "amount"]
    assert rows == [(1, "alpha", 10), (2, "bravo", 20)]


def test_csv_reader_prefers_integer_types(tmp_path: Path) -> None:
    csv_path = tmp_path / "numbers.csv"
    csv_path.write_text("id,value\n1,1.5\n2,2.75\n", encoding="utf-8")

    db_path = tmp_path / "test.duckdb"
    conn_url = f"duckdb:///{db_path}"
    with DBConnection(conn_url) as db:
        db.create_table_from_file("numbers", csv_path)
        columns = [
            row[1] for row in db.query("PRAGMA table_info('numbers')", fetch=True)
        ]
        rows = db.query("SELECT * FROM numbers ORDER BY id")

    assert columns == ["id", "value"]
    assert rows == [(1, 2), (2, 3)]
