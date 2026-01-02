import click
from .test import test
from .query import query


@click.group()
def db():
    """Postgres functions to be implemented."""


db.add_command(test)
db.add_command(query)

__all__ = ["db", "add", "analyze_diff", "list_diffs", "connections"]
