# SQLCompare

Professional CLI for comparing datasets across database tables.

SQLCompare performs row-level comparisons between a "previous" and "current" dataset using a composite key, materializes a join table in a comparison schema, computes column-level differences, and stores a diff run ID for repeatable analysis.

## Key Features
- Compare tables or dataset queries using composite keys.
- Persist comparison runs with metadata for later analysis.
- Generate per-column stats, missing-row checks, and samples of differences.
- Designed for repeatable CLI workflows in data engineering and QA.

## Installation
Use your project setup and dependency manager of choice. For local development:

```bash
uv sync --extra dev
```

## Quick Start
Compare two tables on a single key:

```bash
sqlcompare table analytics.fact_sales analytics.fact_sales_new id
```

Compare using a composite key:

```bash
sqlcompare table analytics.users analytics.users_new user_id,tenant_id
```

Use a named connector and custom schema:

```bash
sqlcompare table public.orders public.orders_latest order_id -c prod --schema sqlcompare
```

Follow up with analysis:

```bash
sqlcompare report <diff_id> --stats
sqlcompare report <diff_id> --column revenue --limit 100
sqlcompare report <diff_id> --missing-current
```

## Command Reference

### Compare tables

```bash
sqlcompare table TABLE1 TABLE2 IDS [--connection/-c CONNECTION] [--schema SCHEMA]
```

#### Arguments
- `TABLE1`: Fully qualified name of the "previous" table to compare. Passed directly to SQL (example: `analytics.schema.table`).
- `TABLE2`: Fully qualified name of the "current" table to compare. Passed directly to SQL.
- `IDS`: Comma-separated list of columns that uniquely identify a row (example: `id` or `id,sub_id`). Whitespace around commas is stripped.

#### Options
- `--connection`, `-c`: Name of a connector profile to use. If omitted, SQLCompare reads the default connector from configuration.
- `--schema`: Schema used to store the join table created for the comparison. If omitted, it uses the configured comparison schema.

## Configuration
SQLCompare is configured through environment variables:

- `SQLCOMPARE_COMPARISON_SCHEMA`:
  Schema used for storing comparison tables.
- `SQLCOMPARE_CONN_DEFAULT`:
  Name of the default connector profile.
- `SQLCOMPARE_CONN_{NAME}`:
  SQLAlchemy URL for a named connector profile. Use with `-c {NAME}`.

## How It Works

1) Resolve configuration, connector, and comparison schema.
2) Parse `IDS` into a list of index columns.
3) Create a unique comparison name and diff ID.
4) Use `DatabaseComparator` to:
   - Verify index columns exist in both tables (case-insensitive match).
   - Create a `FULL OUTER JOIN` table in the comparison schema.
   - Count rows present only in either table.
   - Compute per-column differences for shared columns.
   - Persist metadata for later analysis.
5) Emit a summary and a follow-up report command.

## Diff IDs and Metadata
Each run is recorded in:

```
~/.config/sqlcompare/db_test_runs.yaml
```

The record includes:
- `tables`: mapping of previous, current, and join table names
- `index_cols`: resolved index columns (as found in the database)
- `cols_prev` / `cols_new`: column lists for each table
- `conn`: connector name (or `duckdb` for file-based runs)

Diff ID format:

```
compare_{table1}_{table2}_{timestamp}_{random}
```

Table names are sanitized to alphanumeric plus underscores.

## Output Summary
The CLI prints a concise report to stdout (via `data_toolkit.core.log`), including:
- Missing rows in either table (based on null key columns in the join table).
- Total number of value differences across common rows.
- Per-column difference counts (when common columns exist).
- A sample of the first 10 differences.
- A follow-up command to inspect the diff (`sqlcompare report <diff_id>`).

## Difference Detection

`DatabaseComparator` builds a join table with the following shape:
- For every column in `TABLE1`: `column_previous`
- For every column in `TABLE2`: `column_new`

It uses a `FULL OUTER JOIN` on the provided `IDS`.

Differences are detected when:
- The row exists on both sides (key columns are not null in both).
- A common column does not match (or one side is `NULL` and the other is not).

Analysis queries are generated on-demand:
- `get_diff_query`: per-row differences across all common columns
- `get_stats_query`: counts of differences per column
- `get_in_current_only_query` / `get_in_previous_only_query`: missing-row checks

## Operational Notes
- The command creates a physical join table in the comparison schema. Ensure the connector has `CREATE SCHEMA` and `CREATE TABLE` privileges.
- Table names are passed through directly to SQL. Provide fully-qualified or quoted names when required by your database.
- Index columns must exist in both tables; otherwise the command fails with a clear error that includes a sample of available columns.

## Development

Run tests:

```bash
uv run pytest
```
