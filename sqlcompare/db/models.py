from dataclasses import dataclass, field
from typing import Any, Literal, Optional


@dataclass(frozen=True)
class ExecMeta:
    elapsed_ms: int
    rowcount: int | None
    columns: list[str] | None


# ----------------------------
# DBConnection
# ----------------------------

FetchMode = Literal["auto", True, False]
