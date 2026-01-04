from __future__ import annotations

import os
import sys
from typing import Any


class _Logger:
    def _emit(self, message: Any) -> None:
        sys.stdout.write(f"{message}\n")
        sys.stdout.flush()

    def info(self, message: Any) -> None:
        self._emit(message)

    def warning(self, message: Any) -> None:
        self._emit(message)

    def error(self, message: Any) -> None:
        self._emit(message)

    def debug(self, message: Any) -> None:
        if os.getenv("SQLCOMPARE_DEBUG"):
            self._emit(message)


log = _Logger()
