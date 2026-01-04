from sqlcompare.db import DBConnection, DBConnectionError


import tempfile
import pytest


@pytest.fixture(autouse=True)
def _cleanup_engines():
    # important because we use a global Engine cache (Option A)
    yield
    DBConnection.dispose_all_engines()


def test_sqlite_multiple_query_shapes():
    db = DBConnection("sqlite:///:memory:")

    with db as dbc:
        # DDL -> no rows -> returns None (fetch="auto")
        assert dbc.query("CREATE TEMP TABLE t1 (x INTEGER)") is None

    with pytest.raises(DBConnectionError):
        with db as dbc:
            # DML -> no rows -> returns None (fetch="auto")
            assert dbc.query("INSERT INTO t1(x) VALUES (1), (2)") is None
        
        
def test_sqlite_memory_basic_query_shapes():
    db = DBConnection("sqlite:///:memory:")

    with db as dbc:
        # DDL -> no rows -> returns None (fetch="auto")
        assert dbc.query("CREATE TEMP TABLE t1 (x INTEGER)") is None

        # DML -> no rows -> returns None (fetch="auto")
        assert dbc.query("INSERT INTO t1(x) VALUES (1), (2)") is None

        # rows only
        rows = dbc.query("SELECT x FROM t1 ORDER BY x")
        assert rows == [(1,), (2,)]

        # rows + cols
        rows2, cols = dbc.query("SELECT x FROM t1 ORDER BY x", include_columns=True)
        assert rows2 == [(1,), (2,)]
        assert cols == ["x"]

        # list[dict]
        dicts = dbc.query("SELECT x FROM t1 ORDER BY x", as_dict=True)
        assert dicts == [{"x": 1}, {"x": 2}]

        # list[dict] + cols
        dicts2, cols2 = dbc.query(
            "SELECT x FROM t1 ORDER BY x", as_dict=True, include_columns=True
        )
        assert dicts2 == [{"x": 1}, {"x": 2}]
        assert cols2 == ["x"]

        # fetch=False forces None even for SELECT
        assert dbc.query("SELECT x FROM t1", fetch=False) is None


def test_execute_returns_meta():
    db = DBConnection("sqlite:///:memory:")

    with db as dbc:
        meta = dbc.execute("CREATE TABLE t1 (x INTEGER)")
        assert meta.elapsed_ms >= 0
        assert meta.columns is None  # no rows expected
        # rowcount is often -1/None for DDL depending on DB/driver; we just ensure it doesn't crash

        meta2 = dbc.execute("INSERT INTO t1(x) VALUES (1), (2), (3)")
        assert meta2.elapsed_ms >= 0
        # sqlite usually reports rowcount=3 here
        assert meta2.rowcount in (3, None)


def test_query_outside_context_raises():
    db = DBConnection("sqlite:///:memory:")
    with pytest.raises(DBConnectionError):
        db.query("SELECT 1")


def test_transaction_commit_persists_in_file_sqlite():
    with tempfile.NamedTemporaryFile(suffix=".db") as f:
        url = f"sqlite:///{f.name}"
        db = DBConnection(url)

        with db as dbc:
            dbc.query("CREATE TABLE t1 (x INTEGER)")
            dbc.query("INSERT INTO t1(x) VALUES (10), (20)")

        # new context, same file DB -> should see committed rows
        with db as dbc:
            rows = dbc.query("SELECT x FROM t1 ORDER BY x")
            assert rows == [(10,), (20,)]


def test_transaction_rollback_discards_changes_in_file_sqlite():
    with tempfile.NamedTemporaryFile(suffix=".db") as f:
        url = f"sqlite:///{f.name}"
        db = DBConnection(url)

        # create table (committed)
        with db as dbc:
            dbc.query("CREATE TABLE t1 (x INTEGER)")

        # attempt insert then raise -> rollback should remove inserted rows
        with pytest.raises(RuntimeError):
            with db as dbc:
                dbc.query("INSERT INTO t1(x) VALUES (1), (2), (3)")
                raise RuntimeError("boom")

        # verify rollback worked
        with db as dbc:
            rows = dbc.query("SELECT COUNT(*) FROM t1")
            assert rows == [(0,)]


