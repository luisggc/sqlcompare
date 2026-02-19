from __future__ import annotations

import csv

import typer

from sqlcompare.db import DBConnection
from sqlcompare.helpers import resolve_output_filename
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

            print(tabulate(rows, headers=columns, tablefmt="pretty"))
            print(f"\nTotal rows: {len(rows)}")
        else:
            print("Query executed successfully. No rows returned.")
        return

    output = resolve_output_filename(output, q)
    if not output:
        return

    with open(output, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)
    log.info(f"Results saved to CSV file: {output}")


def query_cmd(
    q: str = typer.Argument(..., help="SQL query to run"),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Database connector name"
    ),
    output: str = typer.Option(
        "terminal",
        "--output",
        "-o",
        help="Output format or file path. Use 'terminal' or provide a .csv filename",
    ),
) -> None:
    """Execute SQL query and display or save results.

    Examples:
        # Display in terminal
        sqlcompare query "SELECT * FROM users LIMIT 10"

        # Save to CSV
        sqlcompare query "SELECT id, name FROM customers" --output results.csv

        # Use specific connection
        sqlcompare query "SELECT COUNT(*) FROM orders" -c snowflake_prod

        # Multi-line query
        sqlcompare query "
            SELECT u.id, u.name, COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.id, u.name
        " -c postgres_dev --output user_orders.csv

    Output Options:
        --output terminal: Display in terminal (default)
        --output file.csv: Save to CSV file
    """
    query(q, connection, output)
