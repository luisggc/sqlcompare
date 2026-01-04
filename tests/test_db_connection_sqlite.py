from __future__ import annotations

from sqlalchemy import create_engine

from sqlcompare.db import DBConnection


def test_db_connection_fetch_rows_sqlite_memory() -> None:
    with DBConnection("sqlite:///:memory:") as db:
        db.execute("CREATE TABLE items (id INTEGER, name TEXT)")
        db.execute("INSERT INTO items VALUES (1, 'alpha'), (2, 'bravo')")
        rows, columns = db.query("SELECT id, name FROM items ORDER BY id", include_columns=True)

    assert columns == ["id", "name"]
    assert rows == [(1, "alpha"), (2, "bravo")]
