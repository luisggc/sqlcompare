import uuid
from typing import Iterable, Tuple

import duckdb
import pandas as pd
import datetime


class BaseFileReader:
    """Base class for reading files into DuckDB queries."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def _prepare_df(self, df: pd.DataFrame) -> Tuple[str, Iterable[str]]:
        for c in df.columns:
            series = df[c]
            if series.dtype == object and not series.empty:
                first = series.dropna().iloc[0]
                if isinstance(first, datetime.time):
                    df[c] = series.astype(str)
        cols = list(df.columns)
        tmp_view = f"tmp_{uuid.uuid4().hex}_view"
        tmp = f"tmp_{uuid.uuid4().hex}"
        self.con.register(tmp_view, df)
        self.con.execute(f"CREATE TABLE {tmp} AS SELECT * FROM {tmp_view}")
        self.con.unregister(tmp_view)
        exprs = []
        for c in cols:
            t = self._infer_column_type(tmp, c)
            if t == "VARCHAR":
                exprs.append(f'"{c}"')
            else:
                exprs.append(f'CAST("{c}" AS {t}) AS "{c}"')
        query = f"SELECT {', '.join(exprs)} FROM {tmp}"
        return query, cols

    def _infer_column_type(self, table: str, column: str) -> str:
        for t in ["BIGINT", "DOUBLE", "TIMESTAMP", "DATE"]:
            q = (
                f"SELECT COUNT(*) FROM {table} "
                f'WHERE TRY_CAST("{column}" AS {t}) IS NULL AND "{column}" IS NOT NULL'
            )
            if self.con.execute(q).fetchone()[0] == 0:
                return t
        return "VARCHAR"

    def read(
        self, file_name: str
    ) -> Tuple[str, Iterable[str]]:  # pragma: no cover - base
        raise NotImplementedError


class CsvReader(BaseFileReader):
    """Prepare a SELECT statement for a CSV file."""

    def read(self, file_name: str) -> Tuple[str, Iterable[str]]:
        return self._prepare_df(pd.read_csv(file_name))


class XlsxReader(BaseFileReader):
    """Prepare a SELECT statement for an XLSX file."""

    def read(self, file_name: str) -> Tuple[str, Iterable[str]]:
        return self._prepare_df(pd.read_excel(file_name))
