from __future__ import annotations

from pathlib import Path
import threading

from typer.testing import CliRunner

from sqlcompare.cli import app
from sqlcompare.stats.models import CheckDefinition, CheckResult, StatsContext
from sqlcompare.stats.runner import _run_selected_checks
from tests.cli_helpers import seed_duckdb, set_cli_env


def test_table_command_with_stats(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "sqlcompare.duckdb"
    seed_duckdb(db_path)
    config_dir = tmp_path / "config"
    set_cli_env(
        monkeypatch,
        config_dir,
        "duckdb_test",
        f"duckdb:///{db_path}",
        schema="analysis_schema",
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "run",
            "stats",
            "previous",
            "current",
            "--connection",
            "duckdb_test",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Stats comparison: previous -> current" in result.output
    assert "Row counts:" in result.output
    assert "Schema differences:" in result.output
    assert "Column comparison:" in result.output
    assert "STATUS_PCT" not in result.output
    assert "Previous  Current  Diff % Diff" in result.output
    assert "        2        2     0   0.0%" in result.output
    assert "COL" in result.output
    assert "TYPE" in result.output
    assert "MATCH" in result.output
    assert "0 (no change)" in result.output
    assert "Schemas match on column names (3 common columns)." in result.output
    assert "Summary: 0 of 4 selected check(s) found differences." in result.output


def test_table_command_with_stats_from_files() -> None:
    runner = CliRunner()
    base = Path(__file__).resolve().parent
    prev_path = base / "datasets" / "stats_compare" / "previous.csv"
    curr_path = base / "datasets" / "stats_compare" / "current.csv"

    result = runner.invoke(
        app,
        [
            "run",
            "stats",
            str(prev_path),
            str(curr_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Stats comparison:" in result.output
    assert "Row counts:" in result.output
    assert "Schema differences:" in result.output
    assert "Column comparison:" in result.output
    assert "NULL" in result.output
    assert "DUP" in result.output
    assert "TYPE" in result.output
    assert "MATCH" in result.output
    assert "notes" in result.output
    assert "VARCHAR (no change)" in result.output
    assert "2 -> 1 (-50%)" in result.output
    assert "0 (no change)" in result.output


def test_stats_command_with_inline_sql(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "sqlcompare.duckdb"
    seed_duckdb(db_path)
    config_dir = tmp_path / "config"
    set_cli_env(
        monkeypatch,
        config_dir,
        "duckdb_test",
        f"duckdb:///{db_path}",
        schema="analysis_schema",
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "run",
            "stats",
            "SELECT * FROM previous",
            "SELECT * FROM current",
            "--connection",
            "duckdb_test",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Stats comparison:" in result.output


def test_stats_command_with_sql_files(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "sqlcompare.duckdb"
    seed_duckdb(db_path)
    config_dir = tmp_path / "config"
    set_cli_env(
        monkeypatch,
        config_dir,
        "duckdb_test",
        f"duckdb:///{db_path}",
        schema="analysis_schema",
    )
    prev_sql = tmp_path / "previous.sql"
    new_sql = tmp_path / "current.sql"
    prev_sql.write_text("SELECT * FROM previous", encoding="utf-8")
    new_sql.write_text("SELECT * FROM current", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "run",
            "stats",
            str(prev_sql),
            str(new_sql),
            "--connection",
            "duckdb_test",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Stats comparison:" in result.output


def test_stats_command_sql_requires_connection(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SQLCOMPARE_CONN_DEFAULT", raising=False)
    monkeypatch.delenv("DTK_CONN_DEFAULT", raising=False)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "run",
            "stats",
            "SELECT * FROM previous",
            "SELECT * FROM current",
        ],
    )

    assert result.exit_code != 0
    assert "No connection specified" in str(result.exception)


def test_stats_command_supports_check_selection() -> None:
    runner = CliRunner()
    base = Path(__file__).resolve().parent
    prev_path = base / "datasets" / "stats_compare" / "previous.csv"
    curr_path = base / "datasets" / "stats_compare" / "current.csv"

    result = runner.invoke(
        app,
        [
            "run",
            "stats",
            str(prev_path),
            str(curr_path),
            "--checks",
            "row_count,schema",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Row counts:" in result.output
    assert "Schema differences:" in result.output
    assert "Column comparison:" not in result.output
    assert "Summary: 1 of 2 selected check(s) found differences." in result.output


def test_stats_command_rejects_unknown_checks() -> None:
    runner = CliRunner()
    base = Path(__file__).resolve().parent
    prev_path = base / "datasets" / "stats_compare" / "previous.csv"
    curr_path = base / "datasets" / "stats_compare" / "current.csv"

    result = runner.invoke(
        app,
        [
            "run",
            "stats",
            str(prev_path),
            str(curr_path),
            "--checks",
            "row_count,invalid_check",
        ],
    )

    assert result.exit_code != 0
    assert "Unknown check name(s): invalid_check" in result.output


def test_stats_command_reports_schema_differences(tmp_path) -> None:
    previous = tmp_path / "previous.csv"
    current = tmp_path / "current.csv"
    previous.write_text("id,name,legacy_only\n1,alpha,a\n2,bravo,b\n", encoding="utf-8")
    current.write_text("id,name,current_only\n1,alpha,x\n2,bravo,y\n", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(app, ["run", "stats", str(previous), str(current)])

    assert result.exit_code == 0, result.output
    assert "Schema differences:" in result.output
    assert "Only in previous:" in result.output
    assert "legacy_only" in result.output
    assert "Only in current:" in result.output
    assert "current_only" in result.output
    assert "Column comparison:" in result.output


def test_stats_command_reports_type_changes_in_column_comparison(tmp_path) -> None:
    previous = tmp_path / "previous.csv"
    current = tmp_path / "current.csv"
    previous.write_text("id,code\n1,100\n2,200\n", encoding="utf-8")
    current.write_text("id,code\n1,A100\n2,B200\n", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(app, ["run", "stats", str(previous), str(current)])

    assert result.exit_code == 0, result.output
    assert "Schema differences:" in result.output
    assert "Type changes:" in result.output
    assert "code: BIGINT -> VARCHAR" in result.output
    assert "Column comparison:" in result.output
    assert "TYPE" in result.output
    assert "BIGINT -> VARCHAR" in result.output
    assert "BIGINT (no change)" in result.output


def test_stats_command_skips_column_checks_when_no_common_columns(tmp_path) -> None:
    previous = tmp_path / "previous.csv"
    current = tmp_path / "current.csv"
    previous.write_text("legacy_id\n1\n2\n", encoding="utf-8")
    current.write_text("current_id\n1\n2\n", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(app, ["run", "stats", str(previous), str(current)])

    assert result.exit_code == 0, result.output
    assert "Schema differences:" in result.output
    assert "Column comparison:" in result.output
    assert "No common columns available for column comparison." in result.output


def test_run_selected_checks_executes_in_parallel_with_isolated_connections(
    monkeypatch,
) -> None:
    thread_ids: set[int] = set()
    start_barrier = threading.Barrier(2)

    class FakeDBConnection:
        def __init__(self, conn_id) -> None:
            self.conn_id = conn_id

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            return None

    def runner(context: StatsContext, definition: CheckDefinition) -> CheckResult:
        thread_ids.add(threading.get_ident())
        start_barrier.wait(timeout=1)
        return CheckResult(
            name=definition.name,
            label=definition.label,
            has_differences=False,
            summary=str(context.db.conn_id),
        )

    context = StatsContext(
        db=object(),
        previous_name="previous",
        current_name="current",
        previous_columns=[],
        current_columns=[],
        common_columns=[],
        previous_only_columns=[],
        current_only_columns=[],
        previous_column_types={},
        current_column_types={},
    )
    check_map = {
        "nulls": CheckDefinition("nulls", "Null differences", runner),
        "duplicates": CheckDefinition("duplicates", "Duplicate differences", runner),
    }
    monkeypatch.setattr("sqlcompare.stats.runner.DBConnection", FakeDBConnection)

    results = _run_selected_checks(
        context=context,
        selected_check_names=["nulls", "duplicates"],
        check_map=check_map,
        connection_id="duckdb_test",
        parallel_safe=True,
    )

    assert [result.name for result in results] == ["nulls", "duplicates"]
    assert [result.summary for result in results] == ["duckdb_test", "duckdb_test"]
    assert len(thread_ids) == 2
