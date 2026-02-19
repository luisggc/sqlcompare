from __future__ import annotations

import yaml

from sqlcompare.db import resolver


def test_resolve_connection_url_falls_back_from_yaml_to_yml(tmp_path, monkeypatch) -> None:
    configured_path = tmp_path / "connections.yaml"
    fallback_path = tmp_path / "connections.yml"
    fallback_path.write_text(
        yaml.safe_dump(
            {
                "yaml_fallback_conn": {
                    "drivername": "sqlite",
                    "database": ":memory:",
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(resolver, "ENV_PREFIXS", [])
    monkeypatch.setattr(resolver, "LIBRARY_CONNECTIONS", [str(configured_path)])

    resolved = resolver.resolve_connection_url("yaml_fallback_conn")
    assert resolved == "sqlite:///:memory:"


def test_resolve_connection_url_falls_back_from_yml_to_yaml(tmp_path, monkeypatch) -> None:
    configured_path = tmp_path / "connections.yml"
    fallback_path = tmp_path / "connections.yaml"
    fallback_path.write_text(
        yaml.safe_dump(
            {
                "yml_fallback_conn": {
                    "drivername": "sqlite",
                    "database": ":memory:",
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(resolver, "ENV_PREFIXS", [])
    monkeypatch.setattr(resolver, "LIBRARY_CONNECTIONS", [str(configured_path)])

    resolved = resolver.resolve_connection_url("yml_fallback_conn")
    assert resolved == "sqlite:///:memory:"
