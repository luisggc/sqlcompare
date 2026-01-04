"""Default configuration values for the db module."""

import os
from pathlib import Path

lib_name = "sqlcompare"
lib_config_folder = Path.home() / f".{lib_name}"

LIBRARY_CONNECTIONS = [
    os.getenv(
        "SQLCOMPARE_CONNECTIONS_FILE", str(lib_config_folder / "connections.yml")
    ),
    os.getenv("DTK_CONNECTIONS_FILE", str(Path.home() / ".dtk" / "connections.yml")),
]

ENV_PREFIXS = [
    f"{lib_name.upper()}_CONN_",
    "DTK_CONN_",
]

# Default connection name used when conn_id is None
DEFAULT_CONN_IDS = [
    os.getenv(f"{lib_name.upper()}_CONN_DEFAULT"),
    os.getenv("DTK_CONN_DEFAULT"),
]
