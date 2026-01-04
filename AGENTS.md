# Repository Guidelines

## Project Structure & Module Organization
- `sqlcompare/` contains the core library and CLI entry points.
- `tests/` holds pytest-based tests (files named `test_*.py`).
- `examples/` includes sample inputs and demo usage.
- `scripts/` provides helper scripts for local workflows.
- `build/` contains build artifacts and is not a source of truth.

## Build, Test, and Development Commands
- `uv sync --extra dev`: install dev dependencies for local work.
- `uv run pytest`: run the full test suite.
- `uv run python -m sqlcompare`: run the CLI module directly for quick checks.

## Coding Style & Naming Conventions
- Python style: follow PEP 8 with 4-space indentation.
- Prefer explicit, descriptive names (for example, `dataset_path`, `compare_stats`).
- Avoid non-ASCII unless a file already uses it.

## Testing Guidelines
- Framework: pytest.
- Test files: `tests/test_*.py`; test functions: `test_*`.
- Run `uv run pytest` after any change; note in your summary if tests are skipped.

## Commit & Pull Request Guidelines
- No explicit commit format is enforced in this repo.
- Use clear, imperative commit messages (for example, “Add CSV diff output”).
- PRs should describe the change, include test results, and link related issues when available.

## Configuration & Environment Tips
- Ensure `uv` is installed before syncing dependencies.
- Keep local data and credentials out of the repo; use environment variables if needed.
