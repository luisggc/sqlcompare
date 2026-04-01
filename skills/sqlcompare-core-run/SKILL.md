---
name: sqlcompare-core-run
description: Run SQLCompare core comparisons across tables, SQL queries, and files to produce a diff_id. Use when the user wants to compare two datasets, choose index keys, set connectors, or run dataset configs via sqlcompare run/table/dataset/stats/query.
---

# SQLCompare Core Run

## Overview
Run the primary SQLCompare commands that create comparison artifacts and return a diff_id for later analysis. Focus on selecting inputs, defining index keys, and choosing the right connector.

## Quick Start
1. Set a default connection or pass one explicitly.
2. Run a comparison command and capture the diff_id from output.
3. Hand the diff_id to output analysis for inspection and reporting.

## Choose The Comparison Type
Use these patterns to pick the right entry point.

1. Compare two tables or views.
`sqlcompare run analytics.fact_sales analytics.fact_sales_new id`
2. Compare two SQL queries (inline or .sql files).
`sqlcompare run "SELECT ..." "SELECT ..." id -c snowflake_prod`
`sqlcompare run queries/previous.sql queries/current.sql id -c snowflake_prod`
3. Compare local files (CSV/XLSX) with DuckDB.
`sqlcompare run path/to/previous.csv path/to/current.xlsx id`
4. Use a dataset config for repeatable runs.
`sqlcompare dataset path/to/dataset.yaml`
5. Run a fast statistical comparison (no diff_id needed for row-level drilldown).
`sqlcompare stats analytics.users analytics.users_new -c snowflake_prod`
6. Run a quick sanity query against a connector.
`sqlcompare query "SELECT COUNT(*) FROM analytics.users" -c snowflake_prod`

## Inputs And Keys
1. Provide index columns that exist in both datasets.
2. Use comma-separated keys for composite indexes.
`sqlcompare run analytics.users analytics.users_new user_id,tenant_id`
3. Fully qualify or quote identifiers when required by your database.

## Connections And Defaults
1. Prefer `-c <name>` to select a named connection for a run.
2. Set `SQLCOMPARE_CONN_DEFAULT` for the default connector.
3. Set `SQLCOMPARE_CONN_<NAME>` for named connectors.
4. Use `~/.sqlcompare/connections.yml` for YAML-based connection configs.

## Expected Output
1. A successful compare prints a diff_id.
2. Use that diff_id with `sqlcompare-output-analysis` for inspection, exports, and queries.

## Notes
1. SQLCompare creates comparison tables in the `SQLCOMPARE_COMPARISON_SCHEMA` (default `sqlcompare`).
2. Ensure the connector has `CREATE SCHEMA` and `CREATE TABLE` privileges.
