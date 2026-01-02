from __future__ import annotations

from typing import Iterable, Mapping


def _build_expr(column: str, cfg: Mapping[str, str], alias: str) -> str:
    col_ref = f'{alias}."{column}"'
    if not cfg:
        return col_ref
    t = cfg.get("type", "").lower()
    if t == "round":
        decimals = int(cfg.get("decimals", 2))
        return f"ROUND(CAST({col_ref} AS DOUBLE), {decimals})"
    if t == "datetime":
        fmt = cfg.get("format")
        if fmt:
            return f"STRPTIME({col_ref}, '{fmt}')"
        return f"CAST({col_ref} AS TIMESTAMP)"
    if t == "date":
        fmt = cfg.get("format")
        if fmt:
            return f"CAST(STRPTIME({col_ref}, '{fmt}') AS DATE)"
        return f"CASE WHEN TRY_CAST({col_ref} AS DOUBLE) IS NOT NULL THEN CAST(from_days(CAST({col_ref} AS DOUBLE)) AS DATE) ELSE CAST({col_ref} AS DATE) END"
    if t == "string":
        op = cfg.get("operation", "").lower()
        if op == "upper":
            return f"UPPER({col_ref})"
        if op == "lower":
            return f"LOWER({col_ref})"
        if op == "strip":
            return f"TRIM({col_ref})"
        return f"CAST({col_ref} AS VARCHAR)"
    if t == "numeric":
        expr = f"CAST({col_ref} AS DOUBLE)"
        if "fill_na" in cfg:
            return f"COALESCE({expr}, {cfg['fill_na']})"
        return expr
    if t in {"int", "integer"}:
        return f"CAST({col_ref} AS BIGINT)"
    if t in {"float", "double"}:
        return f"CAST({col_ref} AS DOUBLE)"
    if t == "fill_na":
        value = cfg.get("value", "''")
        return f"COALESCE({col_ref}, {value})"
    if t == "abs":
        return f"ABS(CAST({col_ref} AS DOUBLE))"
    return col_ref


def apply_column_transformations(
    columns: Iterable[str],
    transformations: Mapping[str, Mapping[str, str]],
    alias: str = "t",
) -> str:
    """Return SQL select list applying transformations to columns."""
    select_parts = []
    for col in columns:
        cfg = transformations.get(col.upper(), {}) if transformations else {}
        expr = _build_expr(col, cfg, alias)
        select_parts.append(f'{expr} AS "{col}"')
    return ", ".join(select_parts)
