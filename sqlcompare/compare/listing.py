import click
import glob
import os
import pandas as pd
from data_toolkit.core.config import load_test_runs
from data_toolkit.core.log import log
from ..analysis.utils import tests_folder


@click.argument("pattern", type=str, required=False)
@click.option("--test", type=str, help="Filter by test name")
@click.command()
def list_diffs(pattern, test):
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
        file_time = pd.to_datetime(os.path.getmtime(match), unit="s")
        filtered_matches.append((diff_id, test_name, file_size_mb, file_time))

    for run_id, run in runs.items():
        if pattern and pattern.lower() not in run_id.lower():
            continue
        if test and test.lower() not in run.get("conn", "").lower():
            continue
        filtered_matches.append((run_id, run.get("conn", "db"), 0, pd.Timestamp.now()))

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
