from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Sequence, TypeVar

T = TypeVar("T")
R = TypeVar("R")


def run_ordered(
    items: Sequence[T],
    worker: Callable[[T], R],
    *,
    enabled: bool = True,
    max_workers: int | None = None,
) -> list[R]:
    """Run work items concurrently while preserving the input order in the results."""
    if len(items) <= 1 or not enabled:
        return [worker(item) for item in items]

    worker_count = max_workers or len(items)
    with ThreadPoolExecutor(max_workers=min(len(items), worker_count)) as executor:
        futures = [executor.submit(worker, item) for item in items]
        return [future.result() for future in futures]
