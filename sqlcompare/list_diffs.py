from __future__ import annotations

import glob
import os
from datetime import datetime

import typer

from sqlcompare.config import get_tests_folder, load_test_runs
from sqlcompare.log import log


def list_diffs(pattern: str | None, test: str | None) -> None:
    """List available diff data files and runs."""
    tests_folder = get_tests_folder()
    search_pattern = f"{tests_folder}/*/diffs/*.pkl"
    matches = glob.glob(search_pattern)
    filtered_matches = []
    runs = load_test_runs()

    for match in matches:
        diff_id = os.path.basename(match).replace(".pkl", "")
        if (
            "_stats" in diff_id
            or "_in_current_only" in diff_id
            or "_in_previous_only" in diff_id
        ):
            continue
        test_name = match.split("/")[-3]
        if pattern and pattern.lower() not in diff_id.lower():
            continue
        if test and test.lower() not in test_name.lower():
            continue
        file_size = os.path.getsize(match)
        file_size_mb = file_size / (1024 * 1024)
        file_time = datetime.fromtimestamp(os.path.getmtime(match))
        filtered_matches.append((diff_id, test_name, file_size_mb, file_time))

    for run_id, run in runs.items():
        if pattern and pattern.lower() not in run_id.lower():
            continue
        if test and test.lower() not in run.get("conn", "").lower():
            continue
        filtered_matches.append((run_id, run.get("conn", "db"), 0, datetime.now()))

    if not filtered_matches:
        log.info("📭 No diff data found matching your criteria.")
        return

    filtered_matches.sort(key=lambda x: x[3], reverse=True)
    log.info(f"📊 Found {len(filtered_matches)} diff data files:")
    log.info("ID | Test | Size | Created")
    log.info("-" * 80)
    for diff_id, test_name, size_mb, file_time in filtered_matches:
        time_str = file_time.strftime("%Y-%m-%d %H:%M")
        log.info(
            f"{diff_id[:30]:<30} | {test_name:<10} | {size_mb:>5.1f}MB | {time_str}"
        )


def list_diffs_cmd(
    pattern: str | None = typer.Argument(None, help="Match diff IDs"),
    test: str | None = typer.Option(None, "--test", help="Filter by test name"),
) -> None:
    """List all available diff data files."""
    list_diffs(pattern, test)
