README

Compare CLI

Overview
The compare CLI command compares two database tables row-by-row using one or more
identifier columns as a composite key. It creates a join table in a test schema,
computes differences across common columns, and stores a diff run ID you can
inspect later with the analyze-diff command.

sqlcompare/compare/comparator.py

Command signature
sqlcompare table TABLE1 TABLE2 IDS [--connection/-c CONNECTION] [--schema SCHEMA]

Arguments
- TABLE1
  Name of the "previous" table to compare. This is passed directly to SQL.
  You can supply a fully qualified name, for example: analytics.schema.table.
- TABLE2
  Name of the "current" table to compare. This is passed directly to SQL.
- IDS
  Comma-separated list of column names used as the unique key. Example: id
  or id,sub_id. Whitespace around commas is stripped.

Options
- --connection, -c
  Name of a connection profile to use. If omitted, the command reads
  default_connection from the main config file.
- --schema
  Schema used to store the join table created for the comparison. If omitted,
  it uses db_test_schema from the config (default: dtk_tests).

Configuration defaults
we can get the default value for the comparison schema in SQLCOMPARE_COMPARISON_SCHEMA. This is used to store the comparison table.
connection default should be SQLCOMPARE_CONN_DEFAULT.
Any conn we get from SQLCOMPARE_CONN_{NAME} . So we can use -c {NAME}.
This conn varibale contains the proper sql alchemy url that we are going to use behid the scenes to perform the queries.

What the command does (high level)
1) Load configuration and resolve connection and schema.
2) Parse IDS into a list of index columns.
3) Create a unique test name and diff ID.
4) Use DatabaseComparator to:
   - Verify index columns exist in both tables (case-insensitive match).
   - Create a join table in the target schema using a FULL OUTER JOIN.
   - Count rows present only in the current or previous table.
   - Compute per-column differences on common columns.
   - Store metadata for later analysis (diff ID).
5) Print a brief summary and the command to analyze the diff.

Diff IDs and persisted metadata
The comparator stores each run in ~/.config/data-toolkit/db_test_runs.yaml with:
- tables: mapping of previous, new, and join table names
- index_cols: resolved index columns (casing as found in the DB)
- cols_prev / cols_new: column lists for each table
- conn: connection name (or duckdb for file-based runs)

The diff ID format is:
compare_{table1}_{table2}_{timestamp}_{random}
where table names are sanitized to alphanumeric plus underscores for safety.

Output and logs
The CLI logs a small report to stdout (via data_toolkit.core.log), including:
- Missing rows in either table (based on null key columns in the join table).
- Total number of value differences across common rows.
- Per-column difference counts (if common columns exist).
- A sample of the first 10 differences.
- A follow-up command to inspect the diff:
  dtk report <diff_id>

How differences are computed (details)
DatabaseComparator builds a join table with the following shape:
- For every column in TABLE1: column_previous
- For every column in TABLE2: column_new
It uses a FULL OUTER JOIN on the provided IDS.

Differences are detected when:
- The row exists on both sides (key columns are not null in both).
- A common column does not match (or one side is NULL and the other is not).

Queries used for analysis are generated on-demand by DatabaseComparator:
- get_diff_query: per-row differences across all common columns
- get_stats_query: counts of differences per column
- get_in_current_only_query / get_in_previous_only_query: missing-row checks

Examples
Compare two tables on a single key:
sqlcompare table analytics.fact_sales analytics.fact_sales_new id

Compare using a composite key:
sqlcompare table analytics.users analytics.users_new user_id,tenant_id

Use an explicit connection and custom schema:
sqlcompare table public.orders public.orders_latest order_id -c prod --schema dtk_tmp

Follow up with analysis:
dtk report <diff_id> --stats
dtk report <diff_id> --column revenue --limit 100
dtk report <diff_id> --missing-current

file in sqlcompare/compare/analyze.py

Operational notes
- The command creates a physical join table in the configured schema. Ensure
  the connection has CREATE SCHEMA and CREATE TABLE privileges as needed.
- Table names are passed through directly to SQL. Provide fully-qualified or
  quoted names when required by your database.
- Index columns must exist in both tables; otherwise the command fails with a
  clear error that includes a sample of available columns.

