import os

from sqlcompare.log import log

possible_stats = {"diff": lambda x, y: y - x, "diff%": lambda x, y: 100 * (y - x) / x}


def test_stats(df_previous, df_current, test_config, save):
    try:
        import pandas as pd
    except ImportError:
        log.error(
            "❌ pandas is required for stats tests. Install it to use this feature."
        )
        return
    common_columns = df_current.columns.intersection(df_previous.columns)
    index_cols = [*df_previous.attrs["index_cols"], *df_current.attrs["index_cols"]]
    common_columns = [column for column in common_columns if column not in index_cols]
    # merge both dataframes
    df_merged = pd.merge(
        df_previous[common_columns],
        df_current[common_columns],
        left_index=True,
        right_index=True,
        suffixes=("_p", "_n"),
    )
    for common_column in common_columns:
        for stat_name, stat_func in possible_stats.items():
            try:
                df_merged[common_column + "_" + stat_name] = stat_func(
                    df_merged[f"{common_column}_p"], df_merged[f"{common_column}_n"]
                )
            except Exception:
                log.info(f"Error calculating {stat_name} for column {common_column}")

    common_columns_with_new_ones = [
        item
        for common_column in common_columns
        for item in (
            [f"{common_column}_p", f"{common_column}_n"]
            + [f"{common_column}_{stat}" for stat in possible_stats]
        )
    ]
    df_merged = df_merged[common_columns_with_new_ones]

    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", 150)
    log.info(df_merged)
    if save:
        dt_suffix = pd.Timestamp.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"{test_config['test_folder']}/extracts/{test_config['test_name']}-{dt_suffix}.csv"
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        df_merged.to_csv(file_name, index=True)
        log.info("--------------------")
        log.info(f"File saved at {file_name}")
    log.info("--------------------")
    log.info("Summary of the % of differences:")
    for common_column in common_columns:
        sum_n = sum(df_merged[common_column + "_n"])
        sum_p = sum(df_merged[common_column + "_p"])
        perc_diff = 100 * (sum_n - sum_p) / sum_p if sum_p != 0 else pd.NA
        log.info(f"{common_column}: {perc_diff:.2f}%")
