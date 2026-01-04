# AGENTS.md

## Scope
These instructions apply to the entire repository.

## Initial setup
- Install uv if not installed
- Install dev dependencies with: `uv sync --extra dev`

## Testing
- Run tests with: `uv run pytest`
- Always run tests after making a change.
- If tests are not run, state the reason in the final response.

## CLI conventions
- CLI help text should use user-facing terms such as "connector" (not "adapter").
- Any new CLI option or behavior change should be reflected in help strings.
