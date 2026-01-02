"""Database comparison test command."""

from __future__ import annotations

import click

from .test_config import load_test_config, load_test_config_from_file
from .test_interactive import interactive_test_setup
from .test_runner import run_test


@click.command()
@click.argument("test-name", type=str, required=False)
@click.option(
    "-f",
    "--file",
    "yaml_file",
    type=click.Path(exists=True),
    help="Path to YAML test configuration file",
)
@click.option("--top-diff", type=int, default=300, help="Show the top N differences.")
def test(test_name: str | None, yaml_file: str | None, top_diff: int) -> None:
    """Run a database comparison test.

    Usage:
      dtk db test                    # Interactive mode
      dtk db test samples            # Load from tests/builtin/samples/test.yaml
      dtk db test my_test            # Load from ~/.config/data-toolkit/db_tests/my_test/test.yaml
      dtk db test -f mytest.yaml     # Load from custom YAML file
    """
    if yaml_file:
        test_cfg = load_test_config_from_file(yaml_file)
    elif test_name:
        test_cfg = load_test_config(test_name)
    else:
        test_cfg = interactive_test_setup()

    run_test(test_cfg, top_diff)
