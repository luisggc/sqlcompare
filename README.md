# SQLCompare

SQLCompare helps you understand how a change impacted your data.
When you modify logic, filters, or inputs, it lets you compare the previous and current versions of a dataset—whether they come from tables, SQL queries, or files.

You can compare datasets in two complementary ways:

1) Row-by-row comparison (with an ID): detect missing rows on either side, identify the columns with the most changes, and inspect before/after values for any record.
2) Statistical comparison: compare column-level statistics such as null counts, distinct counts, and other aggregates to quickly understand overall impact.

---

## What you get

- **Repeatable checks** for releases, backfills, migrations, and vendor drops
- **Clear summaries**: missing-row detection + per-column change counts
- **One workflow** for warehouses *and* local files (DuckDB-powered)

---

## Install

Recommended:

```bash
uv tool install sqlcompare
```

Install optional connector extras as needed:

```bash
uv tool install "sqlcompare[<connector>]"
```

Examples:

```bash
uv tool install "sqlcompare[snowflake]"
uv tool install "sqlcompare[databricks]"
```

---

## Quick start (tables)

Compare two tables on a key:

```bash
export SQLCOMPARE_CONN_DEFAULT="postgresql://<user>:<pass>@<host>/<db>"
sqlcompare table analytics.fact_sales analytics.fact_sales_new id
```

That command prints a **diff_id**. Use it for follow-up analysis:

```bash
sqlcompare inspect <diff_id> --stats
sqlcompare inspect <diff_id> --column revenue --limit 100
sqlcompare inspect <diff_id> --missing-current
sqlcompare inspect <diff_id> --save summary
sqlcompare inspect <diff_id> --save complete --file-path ./reports/full_diff.xlsx
```

---

## Inspect report export (XLSX)

You can export inspect results as a multi-tab Excel report using `--save`.

Modes:

* `--save none` (default): no file output
* `--save summary`: creates `Overview` + per-column tabs (top 200 rows each) + `SQL Reference`
* `--save complete`: same tabs, but without the 200-row cap (limited only by XLSX limits)

Examples:

```bash
# Save summary report in current directory with generated timestamped filename
sqlcompare inspect <diff_id> --save summary

# Save full report to a specific location
sqlcompare inspect <diff_id> --save complete --file-path ./reports/full_diff.xlsx

# Save a single-column summary report
sqlcompare inspect <diff_id> --column revenue --save summary --file-path ./reports/revenue_diff.xlsx
```

Notes:

* `--file-path` is optional; if omitted, SQLCompare generates a readable timestamped filename.
* `--save summary|complete` is for the standard diff view and should not be combined with `--stats`, `--missing-current`, `--missing-previous`, or `--list-columns`.

---

## Example outputs

See [`examples/`](examples/) for datasets, commands, and captured outputs.

---

## Core idea: compare once, analyze many times

SQLCompare does two things:

1. **Compare** two datasets using an index (single or composite key)
2. Persist comparison results and return a **diff_id** you can use to:

* get overall stats
* drill into a specific column’s changes
* list missing rows (previous-only / current-only)
* pull samples for debugging

---

## Usage by use case

### 1) Compare two tables

Best for production validation and regression checks across supported connectors.

```bash
sqlcompare table analytics.users analytics.users_new user_id,tenant_id
```

Why it’s useful:

* Handles **composite keys**
* Produces a **saved diff ID** for repeatable review

---

### 2) Compare SQL query results

Use this when tables aren’t materialized yet or you want a filtered slice.

Create a dataset config:

```yaml
previous:
  select_sql: "SELECT * FROM analytics.orders WHERE order_date < '2024-01-01'"
  index:
    - ORDER_ID

new:
  select_sql: "SELECT * FROM analytics.orders WHERE order_date >= '2024-01-01'"
  index:
    - ORDER_ID
```

Run the compare:

```bash
sqlcompare dataset path/to/dataset.yaml
```

Why it’s useful:

* Compare slices without touching production tables
* Keep an auditable **diff_id**

---

### 3) Compare local CSV / XLSX files (DuckDB)

Great for ad hoc QA, one-off deliveries, or vendor drops.
SQLCompare uses DuckDB under the hood — no DB server required.

Create a dataset config (supports `{{here}}` for relative paths):

```yaml
previous:
  file_name: "{{here}}/previous.csv"
  index:
    - id

new:
  file_name: "{{here}}/current.xlsx"
  index:
    - id
```

Set a local default connector and run:

```bash
export SQLCOMPARE_CONN_DEFAULT="duckdb:///:memory:"
sqlcompare dataset path/to/dataset.yaml
```

Why it’s useful:

* Same diff workflow as warehouses
* Fast local comparisons, zero infra

---

### 4) Compare tables inside a DuckDB file

Use this when your data lives in a local `.duckdb` file.

```bash
export SQLCOMPARE_CONN_LOCAL="duckdb:////absolute/path/to/warehouse.duckdb"
sqlcompare table raw.customers staged.customers id -c local
```

Why it’s useful:

* Local + fast, without Snowflake/warehouse costs
* Easy to integrate into lightweight pipelines

---

## Configuration

SQLCompare resolves connectors in this order:

1. default connector
2. direct URL
3. environment variables
4. YAML files

### Environment variables

* `SQLCOMPARE_CONN_DEFAULT` — default connector URL (used when `-c` is omitted)
* `SQLCOMPARE_CONN_{NAME}` — SQLAlchemy URL for a named connector
* `SQLCOMPARE_COMPARISON_SCHEMA` — schema for comparison tables (default: `sqlcompare`)

Example:

```bash
export SQLCOMPARE_CONN_DEFAULT="postgresql://..."
export SQLCOMPARE_CONN_LOCAL="duckdb:////abs/path/to/db.duckdb"
```

### YAML connections file (optional)

Location:

```
~/.sqlcompare/connections.yml
```

Example:

```yaml
snowflake:
  drivername: snowflake
  username: my_user
  password: my_password
  host: my_account
  database: ANALYTICS
  schema: PUBLIC
  query:
    warehouse: COMPUTE_WH
```

---

## Operational notes

* SQLCompare creates a **physical join table** in the comparison schema. Ensure your connector has `CREATE SCHEMA` and `CREATE TABLE` privileges.
* Table names are passed through directly to SQL. Provide fully-qualified or quoted names when required by your database.
* Index columns must exist in both datasets; otherwise the command fails with a clear error.

---

## Development

Run tests:

```bash
uv run pytest
```
