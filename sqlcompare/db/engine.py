import threading
from typing import Any, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

# Cache key includes URL + poolclass so we never mix pooled vs NullPool engines
_ENGINE_CACHE: dict[tuple[str, type | None], Engine] = {}
_ENGINE_CACHE_LOCK = threading.Lock()


def dispose_all_engines() -> None:
    """
    Force-close pooled connections and clear the engine cache.
    Useful in tests.
    """
    with _ENGINE_CACHE_LOCK:
        for eng in _ENGINE_CACHE.values():
            try:
                eng.dispose()
            except Exception:
                pass
        _ENGINE_CACHE.clear()


def get_engine(url: str, *, engine_kwargs: Optional[dict[str, Any]] = None) -> Engine:
    """
    Return a cached Engine for this URL (thread-safe).

    Default behavior:
    - Reuse engines (and their pools) across DBConnection instances for the same URL.
    - For SQLite in-memory URLs, auto-inject NullPool unless user overrides poolclass,
        so each `with DBConnection(...):` gets a truly fresh in-memory database.
    """
    engine_kwargs = dict(engine_kwargs or {})  # copy so we can safely mutate

    # Auto-disable pooling for in-memory SQLite unless user explicitly sets poolclass.
    if ":memory:" in url and "poolclass" not in engine_kwargs:
        engine_kwargs["poolclass"] = NullPool

    poolclass = engine_kwargs.get("poolclass")  # might be None
    cache_key = (url, poolclass)

    with _ENGINE_CACHE_LOCK:
        engine = _ENGINE_CACHE.get(cache_key)
        if engine is None:
            # pool_pre_ping is useful for long-lived CLIs; sqlite will ignore it.
            engine = create_engine(url, pool_pre_ping=True, **engine_kwargs)
            _ENGINE_CACHE[cache_key] = engine
        return engine
