import click
from .db_connection import DBConnection
from data_toolkit.core.log import log
from tabulate import tabulate
import re
import pandas as pd  # Add pandas import


@click.command()
@click.argument("q", type=str)
@click.option(
    "--connection",
    "-c",
    "--conn",
    "connection",
    help="Connection name, e.g. snowflake_prod",
)
@click.option(
    "--output",
    "-o",
    type=str,
    default="terminal",
    help="Output format or file path. Use 'terminal' for console display or provide a filename with .csv or .xlsx extension",
)
def query(q, connection, output):
    """
    Execute a query against a specified connection.

    Examples:
        # Query Snowflake dev environment and display in terminal
        dtk db query "SELECT * FROM my_table LIMIT 10"

        # Query Databricks prod and save as CSV
        dtk db query "SELECT * FROM users WHERE id > 100" -c snowflake_dev --output=results.csv

        # Query Snowflake prod and save as Excel file
        dtk db query "SELECT * FROM users WHERE id > 100" --output=results.xlsx

        # Multiline queries
        dtk db query -c databricks_dev "
        PASTE YOUR QUERY"

        # Postgres example
        dtk db query -c postgres_prod "SELECT id, number FROM public.loans LIMIT 10"
    """
    log.info(f"Connection: {connection}")
    log.info(f"Query: {q}")

    # Connect to the specified connection
    with DBConnection(connection) as db_conn:
        # Execute the query and get results as DataFrame using pandas.read_sql
        result_df = pd.read_sql(q, db_conn.engine or db_conn.conn)

        # Handle output based on the output parameter
        if output == "terminal":
            # Print results in a nicely formatted table
            if result_df is not None and not result_df.empty:
                print(
                    tabulate(
                        result_df, headers="keys", tablefmt="pretty", showindex=False
                    )
                )
                print(f"\nTotal rows: {len(result_df)}")
            else:
                print("Query executed successfully. No rows returned.")
        else:
            # If no extension is provided, infer filename from query
            if not any(output.lower().endswith(ext) for ext in [".csv", ".xlsx"]):
                # Extract the first word after SELECT or the table name after FROM
                match = re.search(
                    r"(?:SELECT\s+)(\w+)|(?:FROM\s+)(\w+)", q, re.IGNORECASE
                )
                if match:
                    query_identifier = next(
                        (m for m in match.groups() if m), "query_result"
                    )
                else:
                    query_identifier = "query_result"

                # Use this as the filename base
                output = f"{query_identifier}"

            # Determine output format based on filename extension
            if output.lower().endswith(".csv"):
                if not output.lower().endswith(".csv"):
                    output += ".csv"
                result_df.to_csv(output, index=False)
                log.info(f"Results saved to CSV file: {output}")
                print(f"Results saved to CSV file: {output}")
            elif output.lower().endswith(".xlsx"):
                if not output.lower().endswith(".xlsx"):
                    output += ".xlsx"
                # Prepare DataFrame for Excel by removing timezone info

                for col in result_df.columns:
                    if pd.api.types.is_datetime64_any_dtype(result_df[col]):
                        if result_df[col].dt.tz is not None:
                            result_df[col] = result_df[col].dt.tz_localize(None)
                result_df.to_excel(output, index=False)
                log.info(f"Results saved to Excel file: {output}")
            else:
                # Default to CSV if extension not recognized
                output = output + ".csv"
                result_df.to_csv(output, index=False)
                log.info(f"Results saved to CSV file: {output}")
                print(f"Results saved to CSV file: {output}")
