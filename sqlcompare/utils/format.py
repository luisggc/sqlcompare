from __future__ import annotations

from typing import Iterable, Sequence, Any
from tabulate import tabulate


def _trim_cell(x: Any, max_width: int | None) -> Any:
    """Truncate long cell strings with … (does not wrap)."""
    if max_width is None:
        return x
    s = "" if x is None else str(x)
    if len(s) <= max_width:
        return s
    if max_width <= 1:
        return "…"
    return s[: max_width - 1] + "…"


def format_table(
    columns: list[str],
    rows: list[tuple],
    *,
    tablefmt: str = "pretty",
    max_rows: int = 20,
    max_cols: int = 12,
    max_cell_width: int | None = 60,
    **tabulate_kwargs,
) -> str:
    """
    pandas-like summary output for tabulate:
      - If too many columns: show left + '…' + right (inserts an ellipsis column)
      - If too many rows: show head + ellipsis row + tail
    """
    n_cols = len(columns)
    if n_cols == 0:
        return tabulate([], headers=[], tablefmt=tablefmt, **tabulate_kwargs)

    # ---- column trimming ----
    use_col_ellipsis = n_cols > max_cols and max_cols >= 2
    if use_col_ellipsis:
        left = max_cols // 2
        right = max_cols - left
        left_idx = list(range(left))
        right_idx = list(range(n_cols - right, n_cols))
        keep_idx = left_idx + right_idx

        out_columns = columns[:left] + ["…"] + columns[-right:]

        out_rows = []
        for r in rows:
            r_list = list(r)
            kept = [r_list[i] for i in keep_idx[:left]] + ["…"] + [r_list[i] for i in keep_idx[left:]]
            out_rows.append(tuple(kept))
    else:
        out_columns = columns[: max_cols] if (n_cols > max_cols and max_cols >= 1) else columns
        keep_idx = list(range(len(out_columns)))
        out_rows = [tuple(r[i] for i in keep_idx) for r in rows]

    # ---- row trimming ----
    n_rows = len(out_rows)
    use_row_ellipsis = n_rows > max_rows and max_rows >= 2
    if use_row_ellipsis:
        top = max_rows // 2
        bottom = max_rows - top
        head = out_rows[:top]
        tail = out_rows[-bottom:]
        ellipsis_row = tuple("…" for _ in out_columns)
        out_rows = head + [ellipsis_row] + tail
    else:
        out_rows = out_rows[:max_rows]

    # ---- cell trimming ----
    if max_cell_width is not None:
        out_rows = [
            tuple(_trim_cell(v, max_cell_width) for v in r)
            for r in out_rows
        ]

    return tabulate(out_rows, headers=out_columns, tablefmt=tablefmt, **tabulate_kwargs)
