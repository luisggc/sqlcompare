import csv
import glob
import os
from datetime import datetime

from sqlcompare.config import get_tests_folder, load_test_runs
from sqlcompare.log import log
from sqlcompare.utils.format import format_table


def find_diff_file(diff_id: str) -> str | None:
    tests_folder = get_tests_folder()
    pattern = f"{tests_folder}/*/diffs/{diff_id}.pkl"
    matches = glob.glob(pattern)
    if matches:
        return matches[0]
    pattern = f"{tests_folder}/*/diffs/*{diff_id}*.pkl"
    matches = glob.glob(pattern)
    if matches:
        if len(matches) == 1:
            return matches[0]
        log.warning(f"⚠️  Multiple files match '{diff_id}':")
        for match in matches:
            file_id = os.path.basename(match).replace(".pkl", "")
            log.info(f"   {file_id}")
        log.info("💡 Please use a more specific ID")
    return None


def find_diff_run(diff_id: str):
    runs = load_test_runs()
    if diff_id in runs:
        run = runs[diff_id]
        run["id"] = diff_id
        return run
    for key, value in runs.items():
        if diff_id in key:
            value["id"] = key
            return value
    return None


def _display(
    columns: list[str],
    rows: list[tuple],
    column: str | None,
    limit: int,
    save: bool,
    diff_id: str,
    *,
    is_stats: bool = False,
    list_columns: bool = False,
):
    if not columns:
        log.info("No results returned.")
        return

    column_idx = _column_index(columns, "Column")
    column_name_idx = _column_index(columns, "COLUMN_NAME")
    if column_idx is None and column_name_idx is not None:
        column_idx = column_name_idx

    if is_stats:
        log.info(f"\U0001f4ca Statistics for diff ID: {diff_id}")
        filtered_rows = rows
        if column and column_idx is not None:
            column_upper = column.upper()
            filtered_rows = [
                row for row in rows if column_upper in str(row[column_idx]).upper()
            ]
            if not filtered_rows:
                log.info(f"❌ No statistics found for column containing '{column}'")
                return
        log.info(format_table(columns, filtered_rows[:limit]))
        return

    log.info(f"📂 Loaded diff data: {len(rows)} total differences")
    if list_columns:
        if column_idx is None:
            log.warning("⚠️  Column metadata not available.")
            return
        counts: dict[str, int] = {}
        for row in rows:
            col_name = str(row[column_idx])
            counts[col_name] = counts.get(col_name, 0) + 1
        log.info(f"📋 Available columns ({len(counts)}):")
        for col_name in sorted(counts.keys()):
            log.info(f"   {col_name}: {counts[col_name]} differences")
        return

    filtered_rows = rows
    if column and column_idx is not None:
        column_upper = column.upper()
        filtered_rows = [
            row for row in rows if str(row[column_idx]).upper() == column_upper
        ]
        if not filtered_rows:
            log.warning(f"⚠️  No differences found for column '{column}'")
            return
        log.info(
            f"🔍 Filtered to {len(filtered_rows)} differences for column '{column}'"
        )

    display_rows = filtered_rows[:limit]
    log.info(f"📊 Showing {len(display_rows)} of {len(filtered_rows)} differences:")
    log.info(format_table(columns, display_rows))
    if len(filtered_rows) > limit:
        log.info(f"💡 Use --limit {len(filtered_rows)} to see all results")
    if save:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        column_suffix = f"_{column}" if column else ""
        output_file = f"analysis_{diff_id}{column_suffix}_{timestamp}.csv"
        _write_csv(output_file, columns, filtered_rows)
        log.info(f"💾 Results saved to: {output_file}")


def _column_index(columns: list[str], name: str) -> int | None:
    name_upper = name.upper()
    for idx, col in enumerate(columns):
        if col.upper() == name_upper:
            return idx
    return None


def _write_csv(path: str, columns: list[str], rows: list[tuple]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def list_available_diffs():
    tests_folder = get_tests_folder()
    pattern = f"{tests_folder}/*/diffs/*.pkl"
    matches = glob.glob(pattern)
    diff_ids = []
    for match in matches:
        diff_id = os.path.basename(match).replace(".pkl", "")
        if (
            "_stats" in diff_id
            or "_in_current_only" in diff_id
            or "_in_previous_only" in diff_id
        ):
            continue
        test_name = match.split("/")[-3]
        file_size_mb = os.path.getsize(match) / (1024 * 1024)
        diff_ids.append((diff_id, test_name, file_size_mb))
    runs = load_test_runs()
    for run_id, run in runs.items():
        diff_ids.append((run_id, run.get("conn", "db"), 0))
    diff_ids.sort(reverse=True)
    if not diff_ids:
        log.info("   No saved diff data found.")
        return
    log.info("   Recent diff IDs:")
    for diff_id, test_name, size_mb in diff_ids[:10]:
        size_display = f"{size_mb:.1f}MB" if size_mb else "db"
        log.info(f"   {diff_id} (test: {test_name}, {size_display})")
    if len(diff_ids) > 10:
        log.info(f"   ... and {len(diff_ids) - 10} more")
