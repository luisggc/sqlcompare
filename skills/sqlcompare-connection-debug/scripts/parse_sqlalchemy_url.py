#!/usr/bin/env python3
"""Parse a SQLAlchemy URL and highlight encoding issues in credentials."""

from __future__ import annotations

import argparse
import sys
from urllib.parse import parse_qs, unquote, urlparse

try:
    from sqlalchemy.engine import make_url
except Exception:  # pragma: no cover - optional dependency
    make_url = None


RESERVED = set("@:/?#&=%+ ")


def _raw_userinfo(netloc: str) -> str | None:
    if "@" not in netloc:
        return None
    return netloc.split("@", 1)[0]


def _split_user_pass(userinfo: str) -> tuple[str | None, str | None]:
    if ":" in userinfo:
        user, pwd = userinfo.split(":", 1)
        return user, pwd
    return userinfo, None


def _parse_with_sqlalchemy(url: str) -> tuple[object, dict[str, list[str]]]:
    parsed = make_url(url)
    query = parsed.query or {}
    return parsed, {k: [str(v)] if not isinstance(v, list) else [str(x) for x in v] for k, v in query.items()}


def _parse_with_stdlib(url: str) -> tuple[object, dict[str, list[str]]]:
    parsed = urlparse(url)
    return parsed, parse_qs(parsed.query)


def parse_sqlalchemy_url(url: str) -> int:
    if make_url is not None:
        try:
            parsed, query = _parse_with_sqlalchemy(url)
        except Exception as exc:
            print(f\"ERROR: SQLAlchemy failed to parse URL: {exc}\")\n            return 1
    else:
        parsed, query = _parse_with_stdlib(url)

    scheme = getattr(parsed, \"drivername\", None) or getattr(parsed, \"scheme\", \"\")
    if not scheme:
        print("ERROR: Missing scheme (for example, postgresql://, snowflake://, duckdb://)")
        return 1

    netloc = getattr(parsed, \"host\", None)
    if netloc is None and hasattr(parsed, \"netloc\"):
        netloc = parsed.netloc

    raw_userinfo = _raw_userinfo(netloc or \"\") if isinstance(netloc, str) else None
    raw_user, raw_pass = (None, None)
    if raw_userinfo:
        raw_user, raw_pass = _split_user_pass(raw_userinfo)

    if make_url is not None and hasattr(parsed, \"username\"):
        decoded_user = parsed.username
        decoded_pass = parsed.password
    else:
        decoded_user = unquote(raw_user) if raw_user is not None else None
        decoded_pass = unquote(raw_pass) if raw_pass is not None else None

    print("Parsed URL")
    print(f"scheme: {scheme}")
    print(f"username: {decoded_user}")
    print(f"password: {decoded_pass}")
    host = getattr(parsed, \"host\", None) or getattr(parsed, \"hostname\", None)
    port = getattr(parsed, \"port\", None)
    database = getattr(parsed, \"database\", None)
    path = getattr(parsed, \"path\", None)
    print(f\"host: {host}\")
    print(f\"port: {port}\")
    print(f\"database/path: {database or path or None}\")

    if query:
        print("query parameters:")
        for key in sorted(query):
            print(f"  {key}: {query[key]}")
    else:
        print("query parameters: none")

    warnings = []
    if raw_userinfo:
        if raw_user and any(ch in RESERVED for ch in raw_user):
            warnings.append("Username contains reserved characters; URL-encode it.")
        if raw_pass and any(ch in RESERVED for ch in raw_pass):
            warnings.append("Password contains reserved characters; URL-encode it.")
    if warnings:
        print("warnings:")
        for warn in warnings:
            print(f"  - {warn}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse a SQLAlchemy URL")
    parser.add_argument("url", help="SQLAlchemy URL to parse")
    args = parser.parse_args()
    return parse_sqlalchemy_url(args.url)


if __name__ == "__main__":
    sys.exit(main())
