# Changelog

## 0.2.0 (first PyPI release)

A rewrite of the 0.1 code.

### Added
- Published to PyPI as `databricks-mcp-toolkit`, with a `databricks-mcp` command and `python -m databricks_mcp`.
- Unity Catalog tools: `list_catalogs`, `list_schemas`, `list_tables`, `get_table`, `list_volumes`.
- Volume file tools on the Files API: `list_volume_directory`, `read_volume_file`, `write_volume_file`, `delete_volume_file`, `create_volume_directory`.
- SQL warehouse tools: `list_sql_warehouses`, `get_sql_warehouse`, `start_sql_warehouse`, `stop_sql_warehouse`, plus `get_sql_statement` and `cancel_sql_statement`.
- Cluster tools: `restart_cluster`, `resize_cluster`, `list_spark_versions`, `list_node_types`.
- Job tools: `list_job_runs`, `cancel_run`, `run_job` with `job_parameters`. `get_run_output` now handles multi-task runs.
- Workspace tools: `import_notebook`, `create_workspace_directory`, `get_workspace_object`.
- Read-only mode (`--read-only` / `DATABRICKS_MCP_READ_ONLY`) and toolset filtering (`--toolsets`).
- MCP tool annotations (read-only, destructive and idempotent hints) on every tool.
- All Databricks auth methods, through `databricks-sdk` unified auth (OAuth M2M/U2M, profiles, Azure).
- Streamable HTTP transport.
- A unit test suite, plus CI on Python 3.10–3.13 and trusted-publishing release workflow.

### Changed
- Moved to current endpoints: Jobs `2.0` → `2.2`, Clusters `2.0` → `2.1`. Unity Catalog uses `2.1`.
- Tools take typed, named arguments instead of a single opaque `params` object.
- `execute_sql` waits for the result (polling past the API's 50-second limit) and returns flat columns and rows.
- Large responses are truncated with a hint instead of flooding the model's context.
- POST requests are no longer retried on 5xx or timeouts, which could have duplicated resources. 429s are retried with `Retry-After`.
- Renamed tools:
  - `list_notebooks` → `list_workspace`
  - `list_files` → `list_dbfs`
  - `submit_single_run` → `submit_run`
  - `wait_for_run_completion` → `wait_for_run`
  - `execute_command_simple` → `run_command`
- Requires `mcp>=2.3`.

### Fixed
- The server failed to start on a fresh install: it imported `FastMCP`, which was removed in MCP SDK 2.x, and the dependency wasn't pinned.
- `pyproject.toml` said MIT while `LICENSE` is AGPL-3.0. The metadata now matches the license.
- The wheel installed a top-level package named `src`.

### Removed
- The unused FastAPI app, the placeholder API-key auth, and the PowerShell/bash launcher scripts.
- `get_context_status`, and DBFS write helpers (use volumes instead).
