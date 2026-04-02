from __future__ import annotations

from sqlcompare.stats.models import CheckResult, StatsContext
from sqlcompare.utils.format import format_table


def _compact_percent(value: str) -> str:
    if value == "n/a":
        return value
    if value.endswith(".0%"):
        return value.replace(".0%", "%")
    return value


def _status_cell(metric: dict) -> str:
    previous = metric.get("previous", 0)
    current = metric.get("current", 0)
    diff = metric.get("diff", 0)
    pct_diff = _compact_percent(str(metric.get("pct_diff", "0.0%")))

    if diff == 0:
        return f"{current} (no change)"

    return f"{previous} -> {current} ({pct_diff})"


def _type_status_cell(previous_type: str | None, current_type: str | None) -> str:
    previous = previous_type or "unknown"
    current = current_type or "unknown"

    if previous.upper() == current.upper():
        return f"{current} (no change)"

    return f"{previous} -> {current}"


def _build_column_comparison_table(
    context: StatsContext,
    selected_check_names: list[str],
    results_by_name: dict[str, CheckResult],
) -> tuple[list[str], list[tuple]]:
    columns = ["COL"]
    columns.append("TYPE")
    rows: list[tuple] = []

    include_nulls = "nulls" in selected_check_names and "nulls" in results_by_name
    include_duplicates = (
        "duplicates" in selected_check_names and "duplicates" in results_by_name
    )
    row_count_similarity = None
    if "row_count" in selected_check_names and "row_count" in results_by_name:
        row_count_similarity = results_by_name["row_count"].metadata.get("similarity")

    if include_nulls:
        columns.append("NULL")
    if include_duplicates:
        columns.append("DUP")
    columns.append("MATCH")

    null_metrics = (
        results_by_name["nulls"].metadata.get("column_metrics", {}) if include_nulls else {}
    )
    duplicate_metrics = (
        results_by_name["duplicates"].metadata.get("column_metrics", {})
        if include_duplicates
        else {}
    )

    for pair in context.common_columns:
        row = [pair.previous_name]
        row.append(_type_status_cell(pair.previous_type, pair.current_type))
        similarities: list[float] = []

        if row_count_similarity is not None:
            similarities.append(float(row_count_similarity))

        if include_nulls:
            metric = null_metrics.get(pair.key, {})
            row.append(_status_cell(metric))
            similarities.append(float(metric.get("similarity", 1.0)))

        if include_duplicates:
            metric = duplicate_metrics.get(pair.key, {})
            row.append(_status_cell(metric))
            similarities.append(float(metric.get("similarity", 1.0)))

        match_score = (
            100.0
            if not similarities
            else round(sum(similarities) / len(similarities) * 100, 1)
        )
        row.append(match_score)
        rows.append(tuple(row))

    rows.sort(key=lambda row: (row[-1], row[0].lower()))
    return columns, rows


def render_report(
    context: StatsContext,
    selected_check_names: list[str],
    results: list[CheckResult],
) -> str:
    lines: list[str] = []
    results_by_name = {result.name: result for result in results}
    lines.append(f"📊 Stats comparison: {context.previous_name} -> {context.current_name}")
    lines.append("Inputs:")
    lines.append(f"  Previous: {context.previous_name}")
    lines.append(f"  Current: {context.current_name}")

    for result in results:
        if result.name in {"nulls", "duplicates"}:
            continue

        lines.append("")
        lines.append(f"{result.label}:")

        if result.name == "schema":
            lines.append(result.summary or "")
            prev_only = result.metadata.get("previous_only_columns", [])
            curr_only = result.metadata.get("current_only_columns", [])
            type_changes = result.metadata.get("type_changes", [])
            if prev_only:
                lines.append("Only in previous:")
                lines.extend(f"  - {col}" for col in prev_only)
            if curr_only:
                lines.append("Only in current:")
                lines.extend(f"  - {col}" for col in curr_only)
            if type_changes:
                lines.append("Type changes:")
                lines.extend(
                    f"  - {column}: {previous_type} -> {current_type}"
                    for column, previous_type, current_type in type_changes
                )
            continue

        if result.skipped_reason:
            lines.append(f"Skipped: {result.skipped_reason}.")
            continue

        if result.rows:
            lines.append(
                format_table(
                    result.columns,
                    result.rows,
                    max_rows=len(result.rows),
                    max_cols=len(result.columns),
                )
            )
        elif result.summary:
            lines.append(result.summary)

    if any(name in selected_check_names for name in ("nulls", "duplicates")):
        lines.append("")
        lines.append("Column comparison:")
        if not context.common_columns:
            lines.append("No common columns available for column comparison.")
        else:
            column_headers, column_rows = _build_column_comparison_table(
                context,
                selected_check_names,
                results_by_name,
            )
            lines.append(
                format_table(
                    column_headers,
                    column_rows,
                    max_rows=len(column_rows),
                    max_cols=len(column_headers),
                )
            )

    difference_count = sum(1 for result in results if result.has_differences)
    lines.append("")
    lines.append(
        f"Summary: {difference_count} of {len(selected_check_names)} selected check(s) found differences."
    )
    return "\n".join(lines)
