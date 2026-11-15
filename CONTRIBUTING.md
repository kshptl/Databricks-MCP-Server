# Contributing

Issues and pull requests are welcome. For anything bigger than a small fix, open an issue first so we can agree on the approach.

## Setup

```bash
git clone https://github.com/kshptl/Databricks-MCP-Server
cd Databricks-MCP-Server
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Before you open a PR

```bash
pytest -q
ruff check src tests scripts
ruff format src tests scripts
python scripts/gen_tools_table.py   # if you added, renamed or re-described a tool
```

CI runs the same checks on Python 3.10–3.13 and also builds and installs the wheel.

## Adding a tool

1. Add the REST call to the matching module in `src/databricks_mcp/api/`. Keep it thin: build the request, call the client, and reshape the response only when the raw one is unusable.
2. Register it in `src/databricks_mcp/server.py` with `@tool("<toolset>", read_only=..., destructive=...)`.
   - Use typed arguments, plus `Annotated[..., Field(description=...)]` wherever the name alone isn't obvious.
   - Write a one-line docstring. That's the description the model sees.
   - Set `read_only=True` only if the call cannot change anything. Read-only mode trusts this flag.
   - For list endpoints, slim the output with `_slim(...)` and pass through pagination.
3. Add tests:
   - In `tests/test_api.py`, check the method, path, query and body. HTTP is mocked with `respx`.
   - In `tests/test_server.py`, add a test if the tool has its own logic.
4. Regenerate the README tables with `python scripts/gen_tools_table.py`.

Use the current version of each Databricks endpoint (check the [REST API reference](https://docs.databricks.com/api/workspace/introduction)). Don't add tools against deprecated APIs.

## Conventions

- Raise `ValueError` for bad input. The client raises `DatabricksAPIError`. The server turns both into MCP tool errors the model can read.
- Never retry a non-idempotent POST on a 5xx or timeout. The client already handles retries.
- Logs go to stderr. Under stdio, stdout belongs to the MCP protocol.

## License of contributions

The project is dual-licensed (AGPL-3.0 and commercial). By submitting a PR, you agree your contribution can be distributed under both.
