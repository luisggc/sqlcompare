---
name: sqlcompare-connection-debug
description: Debug SQLCompare connection and credential problems, especially connector resolution, environment variables, YAML configs, and permission issues. Use when a run fails to connect or create comparison tables.
---

# SQLCompare Connection Debug

## Overview
Diagnose connection failures by validating connector resolution, credentials, and required privileges. Focus on issues around `-c` selectors, environment variables, and YAML config files.

## Quick Triage
1. Capture the exact error text from the failing command.
2. Verify the command and connector name used (for example, `-c snowflake_prod`).
3. Run a simple connectivity check with `sqlcompare query` if possible.

## Connector Resolution Order
SQLCompare resolves connections in this order.
1. Default connector (if `-c` is omitted).
2. Direct URL passed in the command.
3. Environment variables: `SQLCOMPARE_CONN_DEFAULT`, `SQLCOMPARE_CONN_<NAME>`.
4. YAML file: `~/.sqlcompare/connections.yml`.

## Environment Variable Checklist
1. Confirm `SQLCOMPARE_CONN_DEFAULT` when no `-c` is supplied.
2. Confirm `SQLCOMPARE_CONN_<NAME>` matches the `-c <name>` value.
3. Ensure values are valid SQLAlchemy URLs and include driver, credentials, host, and database.
4. Check for common format issues: missing scheme, extra quotes, unescaped special characters in passwords, or a missing database name.

For URL parsing and credential encoding warnings, run:\n`python3 skills/sqlcompare-connection-debug/scripts/parse_sqlalchemy_url.py \"<sqlalchemy_url>\"`

### SQLAlchemy URL Formats (Examples)
1. Postgres: `postgresql://user:pass@host:5432/dbname`
2. DuckDB file: `duckdb:////absolute/path/to/db.duckdb`
3. DuckDB in-memory: `duckdb:///:memory:`
4. Snowflake (password): `snowflake://user:pass@account/db/schema?warehouse=WH&role=ROLE`
5. Snowflake (private key): `snowflake://user@account/db/schema?warehouse=WH&role=ROLE&private_key_file=/absolute/path/key.p8&private_key_file_pwd=YOUR_PASSPHRASE`

If a password includes `@`, `:`, or `/`, URL-encode it before using it in the connection string.

## YAML Connection File Checklist
1. Check `~/.sqlcompare/connections.yml` exists and is readable.
2. Ensure the top-level key matches the `-c` name.
3. Validate required fields per connector (drivername, username, password, host, database, schema).
4. If you use YAML, prefer it when URL encoding becomes error-prone.

## Permission And Schema Issues
1. SQLCompare creates a physical join table in the comparison schema.
2. Ensure the connector user has `CREATE SCHEMA` and `CREATE TABLE` privileges.
3. Verify `SQLCOMPARE_COMPARISON_SCHEMA` if a non-default schema is required.

## Debug Logging
1. Enable verbose logging by setting `SQLCOMPARE_DEBUG=1`.
2. Re-run the failing command and capture the expanded logs.

## Connection Format Troubleshooting
1. If the error mentions an unknown dialect or driver, confirm the URL scheme (for example `postgresql://`, `snowflake://`, `duckdb://`).
2. If the error mentions authentication, verify username/password and URL-encode special characters.
3. If the error mentions database or schema not found, confirm the database and schema names in the URL or YAML config.

## Connectivity Smoke Tests
Use these to isolate authentication vs query issues.
1. Simple query against the target connector.
`sqlcompare query "SELECT 1" -c <name>`
2. List available diffs to confirm local metadata access.
`sqlcompare list-diffs`

## Common Fixes
1. Use `-c <name>` explicitly to avoid accidental default selection.
2. Correct the connection name casing to match env vars and YAML keys.
3. Quote or fully qualify table names if your database requires it.
