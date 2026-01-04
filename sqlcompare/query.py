import csv
import re

from sqlcompare.db import DBConnection
from sqlcompare.log import log


def query(q: str, connection: str | None, output: str) -> None:
    """
    Execute a query against a specified connection.

    Examples:
        # Query Snowflake dev environment and display in terminal
        sqlcompare query "SELECT * FROM my_table LIMIT 10"

        # Query Databricks prod and save as CSV
        sqlcompare query "SELECT * FROM users WHERE id > 100" -c snowflake_dev --output=results.csv

        # Multiline queries
        sqlcompare query -c databricks_dev "
        PASTE YOUR QUERY"

        # Postgres example
        sqlcompare query -c postgres_prod "SELECT id, number FROM public.loans LIMIT 10"
    """
    log.info(f"Connection: {connection}")
    log.info(f"Query: {q}")

    with DBConnection(connection) as db_conn:
        rows, columns = db_conn.query(q, include_columns=True)

    if output == "terminal":
        if rows:
            from tabulate import tabulate

            print(tabulate(rows, headers=columns))
            print(f"\nTotal rows: {len(rows)}")
        else:
            print("Query executed successfully. No rows returned.")
        return

    output = _resolve_output_filename(output, q)
    if not output:
        return

    with open(output, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)
    log.info(f"Results saved to CSV file: {output}")


def _resolve_output_filename(output: str, query: str) -> str | None:
    if output.lower().endswith(".xlsx"):
        log.error("❌ XLSX output is not supported without pandas. Use CSV instead.")
        return None

    if output.lower().endswith(".csv"):
        return output

    match = re.search(r"(?:SELECT\s+)(\w+)|(?:FROM\s+)(\w+)", query, re.IGNORECASE)
    if match:
        query_identifier = next((m for m in match.groups() if m), "query_result")
    else:
        query_identifier = "query_result"

    if "." in output:
        log.error("❌ Only CSV output is supported without pandas.")
        return None

    return f"{query_identifier}.csv"
