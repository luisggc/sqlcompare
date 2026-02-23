from __future__ import annotations

from typing import Any

import pandas as pd


def format_table(
    columns: list[str],
    rows: list[tuple],
    *,
    max_rows: int = 20,
    max_cols: int = 12,
    max_cell_width: int | None = 60,
) -> str:
    """Render a dataframe-like table using pandas output formatting."""
    if not columns:
        return ""

    df = pd.DataFrame(list(rows), columns=columns)

    display_max_rows = None if max_rows is None or max_rows <= 0 else max_rows
    display_max_cols = None if max_cols is None or max_cols <= 0 else max_cols

    option_kwargs: dict[str, Any] = {
        "display.max_rows": display_max_rows,
        "display.max_columns": display_max_cols,
        "display.width": 0,
        "display.expand_frame_repr": False,
    }
    if max_cell_width is not None:
        option_kwargs["display.max_colwidth"] = max_cell_width

    with pd.option_context(*sum(option_kwargs.items(), ())):
        return df.to_string(index=False)
