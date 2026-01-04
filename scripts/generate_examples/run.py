from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_examples(config_path: Path) -> dict[str, dict[str, Any]]:
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("examples.yml must contain a mapping of example definitions.")
    return payload


def _run_command(command: str, cwd: Path, env: dict[str, str]) -> str:
    result = subprocess.run(
        command,
        shell=True,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Command failed with exit code "
            f"{result.returncode}:\n{command}\n\n{result.stdout}"
        )
    return result.stdout


def main() -> int:
    root = _repo_root()
    config_path = root / "scripts" / "generate_examples" / "examples.yml"
    examples = _load_examples(config_path)
    env = os.environ.copy()
    env.setdefault("NO_COLOR", "1")

    for name, config in examples.items():
        if not isinstance(config, dict):
            raise ValueError(f"Example '{name}' must be a mapping.")
        description = config.get("description")
        if not description or not isinstance(description, str):
            raise ValueError(f"Example '{name}' is missing a description string.")
        title = config.get("title")
        if not title or not isinstance(title, str):
            title = name.replace("_", " ").title()
        command = config.get("command")
        if not command or not isinstance(command, str):
            raise ValueError(f"Example '{name}' is missing a command string.")

        print(f"Generating example '{name}'...")
        output = _run_command(command, root, env)
        markdown_output = "\n".join(
            [
                f"# {title}",
                "",
                description,
                "",
                "```text",
                output.rstrip("\n"),
                "```",
                "",
            ]
        )
        output_path = root / "examples" / f"{name}.md"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown_output, encoding="utf-8")

    return 0


if __name__ == "__main__":
    sys.exit(main())
