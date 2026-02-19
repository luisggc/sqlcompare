import os
import re
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy.engine import URL

from .constants import DEFAULT_CONN_IDS, LIBRARY_CONNECTIONS, ENV_PREFIXS


def normalize_connection_name(name: str) -> str:
    """
    Normalize connection IDs so env var names are stable:
    - uppercase
    - non-alphanumeric -> underscore
    - collapse underscores
    - trim underscores
    """
    s = name.strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _expand_env_vars(value: Any) -> Any:
    """
    Recursively expand environment variables in strings, dicts, and lists.

    Expands ${VAR_NAME} and $VAR_NAME patterns in strings.
    """
    if isinstance(value, str):
        return os.path.expandvars(value)
    elif isinstance(value, dict):
        return {k: _expand_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_expand_env_vars(item) for item in value]
    else:
        return value


def _load_connections_from_yaml(yaml_path: str) -> dict[str, dict[str, Any]]:
    """
    Load connection definitions from YAML file.
    Returns a dict mapping conn_id -> URL.create() parameters.
    Expands environment variables in all string values.
    """
    expanded_path = Path(yaml_path).expanduser()
    if not expanded_path.exists():
        return {}

    try:
        with open(expanded_path, "r") as f:
            data = yaml.safe_load(f)
            if isinstance(data, dict):
                # Expand environment variables in all values
                return _expand_env_vars(data)
            return {}
    except Exception:
        return {}


def _build_not_found_error(conn_id: str, normalized_name: str) -> str:
    """
    Build a comprehensive error message listing all search locations.

    Args:
        conn_id: Original connection ID
        normalized_name: Normalized version used for env var lookups

    Returns:
        Multi-line error message showing where we looked
    """
    env_vars_checked = [f"{prefix}{normalized_name}" for prefix in ENV_PREFIXS]

    message_parts = [
        f"No connection URL found for conn_id={conn_id!r}.",
        "",
        "Searched environment variables:",
    ]
    for env_var in env_vars_checked:
        message_parts.append(f"  - {env_var}")

    message_parts.append("")
    message_parts.append("Searched YAML files:")
    for yaml_file in LIBRARY_CONNECTIONS:
        expanded = Path(yaml_file).expanduser()
        exists = " (found)" if expanded.exists() else " (not found)"
        message_parts.append(f"  - {yaml_file}{exists}")

    return "\n".join(message_parts)


def resolve_connection_url(conn_id_or_url: str | None) -> str:
    """
    Resolve a connection ID to a database URL.

    Resolution order (first match wins):
    1. Use DEFAULT_CONN_IDS if conn_id_or_url is None
    2. Direct URL (if contains "://")
    3. File path (.csv or .xlsx) - returns DuckDB with special marker
    4. Environment variables (all ENV_PREFIXS in order)
    5. YAML files (all LIBRARY_CONNECTIONS in order)

    Args:
        conn_id_or_url: Connection ID to resolve, a direct database URL, file path, or None

    Returns:
        Database URL string (for file paths, returns "duckdb:///:memory:?file_path=<path>")

    Raises:
        ValueError: If connection ID cannot be resolved

    Example:
        # Direct URL passthrough
        url = resolve_connection_url("postgresql://localhost/db")

        # Environment variable lookup
        os.environ["SQLCOMPARE_CONN_PROD"] = "postgresql://prod/db"
        url = resolve_connection_url("prod")

        # Use default connection if None
        os.environ["SQLCOMPARE_CONN_DEFAULT"] = "my_conn"
        url = resolve_connection_url(None)

        # File path (CSV or Excel)
        url = resolve_connection_url("/path/to/data.csv")

        # YAML file lookup (if not in env)
        # Searches ~/.sqlcompare/connections.yml, then ~/.dtk/connections.yml
    """
    # Step 0: Use default connection if None (search all DEFAULT_CONN_IDS)
    if conn_id_or_url is None:
        for default_id in DEFAULT_CONN_IDS:
            if default_id:
                conn_id_or_url = default_id
                break

        if conn_id_or_url is None:
            raise ValueError(
                "No connection ID provided and no default connection found. "
                "Set SQLCOMPARE_CONN_DEFAULT or DTK_CONN_DEFAULT"
            )

    # Step 1: Direct URL passthrough
    if "://" in conn_id_or_url:
        return conn_id_or_url

    # Step 2: Check if it's a file path (.csv or .xlsx)
    file_path = Path(conn_id_or_url).expanduser()
    if file_path.exists() and file_path.suffix.lower() in (".csv", ".xlsx"):
        # Return DuckDB URL with file path marker
        return f"duckdb:///:memory:?_file_path={file_path.absolute()}"

    normalized_name = normalize_connection_name(conn_id_or_url)

    # Step 3: Search ALL environment prefixes (in order)
    for env_prefix in ENV_PREFIXS:
        env_key = f"{env_prefix}{normalized_name}"
        url_str = os.getenv(env_key)
        if url_str:
            return url_str

    # Step 4: Search ALL YAML files (in order)
    for yaml_path in LIBRARY_CONNECTIONS:
        connections = _load_connections_from_yaml(yaml_path)

        # YAML uses raw conn_id (not normalized) for matching
        if conn_id_or_url in connections:
            url_params = connections[conn_id_or_url]
            try:
                url = URL.create(**url_params)
                return str(url)
            except Exception as e:
                raise ValueError(
                    f"Invalid connection parameters for conn_id={conn_id_or_url!r} "
                    f"in {yaml_path}: {e}"
                ) from e

    # Step 5: Not found anywhere - build helpful error message
    raise ValueError(_build_not_found_error(conn_id_or_url, normalized_name))
