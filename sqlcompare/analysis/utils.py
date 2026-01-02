import os
import glob
import pandas as pd
from data_toolkit.core.config import load_config, load_test_runs
from data_toolkit.core.log import log
from ..db_connection import DBConnection

config = load_config()
tests_folder = f"{config['dtk_dir']}/data_toolkit/db/tests"


def find_diff_file(diff_id: str) -> str | None:
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
    diff_df: pd.DataFrame,
    column: str | None,
    limit: int,
    save: bool,
    diff_id: str,
    *,
    is_stats: bool = False,
    list_columns: bool = False,
):
    # Normalise column naming for easier handling
    if "Column" not in diff_df.columns:
        rename_candidates = {}
        if "COLUMN_NAME" in diff_df.columns:
            rename_candidates["COLUMN_NAME"] = "Column"
        if "COLUMN" in diff_df.columns:
            rename_candidates["COLUMN"] = "Column"
        if rename_candidates:
            diff_df = diff_df.rename(columns=rename_candidates)

    column_field = "Column"

    if is_stats:
        log.info(f"\U0001f4ca Statistics for diff ID: {diff_id}")
        if column:
            filtered_stats = diff_df[
                diff_df[column_field].str.contains(column, case=False, na=False)
            ]
            if filtered_stats.empty:
                log.info(f"❌ No statistics found for column containing '{column}'")
                return
            log.info(filtered_stats.head(limit))
        else:
            log.info(diff_df.head(limit))
        return

    log.info(f"📂 Loaded diff data: {len(diff_df)} total differences")
    if list_columns:
        columns = diff_df[column_field].unique()
        log.info(f"📋 Available columns ({len(columns)}):")
        for col in sorted(columns):
            count = len(diff_df[diff_df[column_field] == col])
            log.info(f"   {col}: {count} differences")
        return

    if column:
        column_upper = column.upper()
        filtered_df = diff_df[diff_df[column_field].str.upper() == column_upper]
        if len(filtered_df) == 0:
            log.warning(f"⚠️  No differences found for column '{column}'")
            return
        log.info(f"🔍 Filtered to {len(filtered_df)} differences for column '{column}'")
    else:
        filtered_df = diff_df

    display_df = filtered_df.head(limit)
    log.info(f"📊 Showing {len(display_df)} of {len(filtered_df)} differences:")

    # Limit columns to display to avoid bad formatting
    if len(display_df.columns) > 10:
        display_cols = list(display_df.columns[:10])
        log.info(
            display_df[display_cols].to_string(index=False)
            + f"\n... and {len(display_df.columns) - 10} more columns"
        )
    else:
        log.info(display_df.to_string(index=False))
    if len(filtered_df) > limit:
        log.info(f"💡 Use --limit {len(filtered_df)} to see all results")
    if save:
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        column_suffix = f"_{column}" if column else ""
        output_file = f"analysis_{diff_id}{column_suffix}_{timestamp}.xlsx"
        filtered_df.to_excel(output_file, index=False)
        log.info(f"💾 Results saved to: {output_file}")


def list_available_diffs():
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
