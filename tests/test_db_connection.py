from __future__ import annotations

from types import SimpleNamespace

from sqlcompare.db import DBConnection
import sqlcompare.db.connection as connection_module


def test_get_table_column_types_snowflake_uses_describe_for_fully_qualified_name(
    monkeypatch,
) -> None:
    db = DBConnection("snowflake://example")
    db._engine = SimpleNamespace(dialect=SimpleNamespace(name="snowflake"))

    captured: dict[str, str] = {}

    def fake_query(sql: str, *args, **kwargs):
        captured["sql"] = sql
        return [("ID", "NUMBER(38,0)"), ("NAME", "VARCHAR(16777216)")]

    monkeypatch.setattr(db, "query", fake_query)

    def fail_inspect(_conn):
        raise AssertionError("inspect() should not be used for Snowflake")

    monkeypatch.setattr(connection_module, "inspect", fail_inspect)

    result = db.get_table_column_types(
        "LH_PROD_DATA_SNOWFLAKE.salesforce_derived.account_augmented"
    )

    assert (
        captured["sql"]
        == "DESCRIBE TABLE LH_PROD_DATA_SNOWFLAKE.salesforce_derived.account_augmented"
    )
    assert result == {"ID": "NUMBER(38,0)", "NAME": "VARCHAR(16777216)"}
