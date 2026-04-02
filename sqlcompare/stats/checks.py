from __future__ import annotations

from sqlcompare.stats.models import CheckDefinition, CheckResult, StatsContext


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _format_percent_diff(previous: int, current: int) -> str:
    if previous == 0:
        return "0.0%" if current == 0 else "n/a"
    return f"{((current - previous) / previous) * 100:.1f}%"


def _calculate_similarity(previous: int, current: int) -> float:
    base = max(previous, current, 1)
    return max(0.0, min(1.0, 1 - (abs(current - previous) / base)))


def _build_empty_column_result(definition: CheckDefinition) -> CheckResult:
    return CheckResult(
        name=definition.name,
        label=definition.label,
        has_differences=False,
        skipped_reason="no common columns",
    )


def _collect_null_counts(
    context: StatsContext, table_name: str, use_previous_names: bool
) -> dict[str, int]:
    if not context.common_columns:
        return {}

    select_parts = []
    aliases: dict[str, str] = {}
    for idx, pair in enumerate(context.common_columns):
        column_name = pair.previous_name if use_previous_names else pair.current_name
        alias = f"c{idx}_nulls"
        select_parts.append(
            f"SUM(CASE WHEN {_quote_ident(column_name)} IS NULL THEN 1 ELSE 0 END) AS {alias}"
        )
        aliases[pair.key] = alias

    rows, cols = context.db.query(
        f"SELECT {', '.join(select_parts)} FROM {table_name}",
        include_columns=True,
    )
    if not rows:
        return {}

    row = rows[0]
    col_index = {col.upper(): idx for idx, col in enumerate(cols)}
    return {
        pair.key: int(row[col_index[aliases[pair.key].upper()]])
        for pair in context.common_columns
    }


def _duplicate_count(context: StatsContext, table_name: str, column_name: str) -> int:
    query = (
        "SELECT COALESCE(SUM(group_count), 0) "
        "FROM ("
        f"SELECT COUNT(*) AS group_count FROM {table_name} "
        f"GROUP BY {_quote_ident(column_name)} "
        "HAVING COUNT(*) > 1"
        ") duplicate_groups"
    )
    return int(context.db.query(query)[0][0])


def _build_column_metric_result(
    *,
    context: StatsContext,
    definition: CheckDefinition,
    metric_name: str,
    previous_counts: dict[str, int],
    current_counts: dict[str, int],
) -> CheckResult:
    rows = []
    metrics: dict[str, dict[str, int | float | str]] = {}

    for pair in context.common_columns:
        previous = previous_counts.get(pair.key, 0)
        current = current_counts.get(pair.key, 0)
        diff = current - previous
        metrics[pair.key] = {
            "previous": previous,
            "current": current,
            "diff": diff,
            "pct_diff": _format_percent_diff(previous, current),
            "similarity": _calculate_similarity(previous, current),
        }
        rows.append(
            (
                pair.previous_name,
                previous,
                current,
                diff,
                _format_percent_diff(previous, current),
            )
        )

    diff_rows = [row for row in rows if row[3] != 0]
    diff_rows.sort(key=lambda row: (-abs(row[3]), row[0].lower()))

    return CheckResult(
        name=definition.name,
        label=definition.label,
        has_differences=bool(diff_rows),
        columns=["Column", "Previous", "Current", "Diff", "% Diff"],
        rows=diff_rows,
        summary=(
            f"{len(diff_rows)} column(s) with {metric_name} differences."
            if diff_rows
            else f"No {metric_name} differences found."
        ),
        metadata={"column_metrics": metrics},
    )


def run_row_count_check(context: StatsContext, definition: CheckDefinition) -> CheckResult:
    previous = context.db.query(f"SELECT COUNT(*) FROM {context.previous_name}")[0][0]
    current = context.db.query(f"SELECT COUNT(*) FROM {context.current_name}")[0][0]
    diff = current - previous
    pct_diff = _format_percent_diff(previous, current)
    return CheckResult(
        name=definition.name,
        label=definition.label,
        has_differences=diff != 0,
        columns=["Previous", "Current", "Diff", "% Diff"],
        rows=[(previous, current, diff, pct_diff)],
        summary=f"Previous={previous}, Current={current}, Diff={diff}, % Diff={pct_diff}",
        metadata={"similarity": _calculate_similarity(previous, current)},
    )


def run_schema_check(context: StatsContext, definition: CheckDefinition) -> CheckResult:
    previous_only = context.previous_only_columns
    current_only = context.current_only_columns
    type_changes = [
        (
            pair.previous_name,
            pair.previous_type or "unknown",
            pair.current_type or "unknown",
        )
        for pair in context.common_columns
        if (pair.previous_type or "unknown").upper() != (pair.current_type or "unknown").upper()
    ]
    has_differences = bool(previous_only or current_only or type_changes)
    common_count = len(context.common_columns)
    if has_differences:
        summary = (
            f"{len(previous_only)} only in previous, "
            f"{len(current_only)} only in current, "
            f"{len(type_changes)} type changes, "
            f"{common_count} common columns."
        )
    else:
        summary = f"Schemas match on column names ({common_count} common columns)."
    return CheckResult(
        name=definition.name,
        label=definition.label,
        has_differences=has_differences,
        summary=summary,
        metadata={
            "previous_only_columns": previous_only,
            "current_only_columns": current_only,
            "type_changes": type_changes,
            "common_count": common_count,
        },
    )


def run_nulls_check(context: StatsContext, definition: CheckDefinition) -> CheckResult:
    if not context.common_columns:
        return _build_empty_column_result(definition)

    previous_counts = _collect_null_counts(context, context.previous_name, True)
    current_counts = _collect_null_counts(context, context.current_name, False)
    return _build_column_metric_result(
        context=context,
        definition=definition,
        metric_name="null-count",
        previous_counts=previous_counts,
        current_counts=current_counts,
    )


def run_duplicates_check(context: StatsContext, definition: CheckDefinition) -> CheckResult:
    if not context.common_columns:
        return _build_empty_column_result(definition)

    previous_counts = {
        pair.key: _duplicate_count(context, context.previous_name, pair.previous_name)
        for pair in context.common_columns
    }
    current_counts = {
        pair.key: _duplicate_count(context, context.current_name, pair.current_name)
        for pair in context.common_columns
    }
    return _build_column_metric_result(
        context=context,
        definition=definition,
        metric_name="duplicate-count",
        previous_counts=previous_counts,
        current_counts=current_counts,
    )


def get_checks() -> list[CheckDefinition]:
    return [
        CheckDefinition("row_count", "Row counts", run_row_count_check),
        CheckDefinition("schema", "Schema differences", run_schema_check),
        CheckDefinition("nulls", "Null differences", run_nulls_check),
        CheckDefinition("duplicates", "Duplicate differences", run_duplicates_check),
    ]


def get_check_map() -> dict[str, CheckDefinition]:
    return {check.name: check for check in get_checks()}
