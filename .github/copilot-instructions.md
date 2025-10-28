## Purpose
Provide concise, actionable guidance so AI coding agents (and new devs) can be productive in this repository quickly.

## Quick orientation (big picture)
- Repository layout: top-level `tools.py` and an `agents/` folder. The repo is small and organized around reusable "tools" (utility functions) and agent implementations.
- Primary pattern: shared helpers live in `tools.py` and are consumed by modules under `agents/`.

## Key files to read first
- `tools.py` — exports core utilities. Example identifiers discovered: `validate_data_tool`, `calculate_total_tool`, `_get_top_n_tool` (leading underscore indicates a private helper).
- `agents/` — contains agent implementations that orchestrate the tools. Open the directory to find concrete usages of the tools and expected input/output shapes.

## Naming & project-specific conventions
- Tool functions use snake_case and often end with `_tool`. Follow this suffix when adding new shared helpers.
- Private helpers use a leading underscore (e.g. `_get_top_n_tool`). Keep private helpers inside `tools.py` unless they grow complex enough to deserve their own module.
- Keep tools pure and focused: tools should accept simple serializable inputs (dicts, lists, primitives) so agents can call them deterministically.

## Data flow and component boundaries
- Agents orchestrate higher-level workflows and call into `tools.py` for transformation/validation logic. The expected flow:
  1. Agent receives input (user request / event)
  2. Agent validates/normalizes input using `validate_data_tool`
  3. Agent applies domain logic via other tools (e.g., `calculate_total_tool`)
  4. Agent returns structured output or persists results
- When modifying a tool, scan `agents/` to find all callers — agents are the primary integration points.

## How to run quick checks (assumptions)
- Assumes a system Python is available. There is no visible requirements file in the repo snapshot; install dependencies if a `requirements.txt` or `pyproject.toml` is added.
- Quick smoke test (PowerShell): import the module and list exports:

```powershell
python -c "import tools; print([n for n in dir(tools) if not n.startswith('__')])"
```

This verifies Python can import `tools.py` and shows exported names such as `validate_data_tool`.

## Adding a new tool — concrete checklist
1. Add the function to `tools.py` unless it grows large. Name it using snake_case and include `_tool` suffix for public helpers.
2. If the helper is internal, prefix with `_` and keep it in `tools.py`.
3. Update or add examples in `agents/` that exercise the new tool.
4. Run the smoke import command above and write a small unit test or script (placed in `tests/` or `agents/` as appropriate).

## Debugging tips
- To find where a tool is used, search for its exact name under `agents/`.
- If an agent fails at runtime, replicate the failing input by calling the same sequence of tool functions interactively (REPL or a one-off script). Tools are designed to be callable in isolation.

## CI, tests, and conventions
- No CI/test files were detected in the provided snapshot. If CI is added, place unit tests under `tests/` and prefer small, focused tests for each tool.

## What I couldn't discover automatically
- Runtime expectations (Python version, external services, environment variables) are not present in the visible files. If the project uses a virtual environment or has infra dependencies, add a short README or manifest (`requirements.txt` / `pyproject.toml`) so agents can surface reproducible commands.

## Questions for the maintainer
- Do you want the `_tool` suffix enforced for all helpers, or only exported ones?
- Should new tools be split into a `tools/` package when the file grows? If so, name it `tools/__init__.py` and keep simple imports at the top-level for backward compatibility.

-- End of file
