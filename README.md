# Databricks MCP Toolkit

[![PyPI](https://img.shields.io/pypi/v/databricks-mcp-toolkit.svg)](https://pypi.org/project/databricks-mcp-toolkit/)
[![Python](https://img.shields.io/pypi/pyversions/databricks-mcp-toolkit.svg)](https://pypi.org/project/databricks-mcp-toolkit/)
[![CI](https://github.com/kshptl/Databricks-MCP-Server/actions/workflows/ci.yml/badge.svg)](https://github.com/kshptl/Databricks-MCP-Server/actions/workflows/ci.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)

A [Model Context Protocol](https://modelcontextprotocol.io) server that lets Claude, Cursor, VS Code and other MCP clients work with a Databricks workspace. Through it, the assistant can browse Unity Catalog, run SQL, read and write volume files, manage clusters and jobs, export and import notebooks, and run code on a cluster.

- **Current Databricks APIs:** Jobs 2.2, Clusters 2.1, Unity Catalog 2.1, the Files API for volumes, and SQL Statement Execution.
- **Any Databricks auth method:** personal access tokens, OAuth (user or service principal), `~/.databrickscfg` profiles and Azure CLI, all handled by the official `databricks-sdk`.
- **Safe to point at real workspaces:** a read-only mode, per-toolset filtering, MCP tool annotations (read-only and destructive hints), no automatic retries of non-idempotent calls, and response size caps so a big result doesn't flood the model's context.

## Quick start

You need Python 3.10+ and [uv](https://docs.astral.sh/uv/) (or pipx/pip).

```bash
# try it without installing
uvx --from databricks-mcp-toolkit databricks-mcp list-tools

# or install it
pipx install databricks-mcp-toolkit      # or: pip install databricks-mcp-toolkit
databricks-mcp --version
```

### Claude Desktop

Add this to `claude_desktop_config.json` (Settings → Developer → Edit Config):

```json
{
  "mcpServers": {
    "databricks": {
      "command": "uvx",
      "args": ["--from", "databricks-mcp-toolkit", "databricks-mcp"],
      "env": {
        "DATABRICKS_HOST": "https://<your-workspace>.cloud.databricks.com",
        "DATABRICKS_TOKEN": "<personal-access-token>",
        "DATABRICKS_WAREHOUSE_ID": "<optional default SQL warehouse id>"
      }
    }
  }
}
```

### Claude Code

```bash
claude mcp add databricks \
  -e DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com \
  -e DATABRICKS_TOKEN=<personal-access-token> \
  -- uvx --from databricks-mcp-toolkit databricks-mcp
```

### Cursor / VS Code / other clients

Use the same command and env vars: `uvx --from databricks-mcp-toolkit databricks-mcp`. To serve over HTTP instead of stdio:

```bash
databricks-mcp run --transport streamable-http --host 127.0.0.1 --port 8000
```

## Authentication

Auth is delegated to [Databricks unified auth](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth), so whatever works for the Databricks CLI works here:

| Method | Set |
|---|---|
| Personal access token | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` |
| OAuth service principal (M2M) | `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` |
| OAuth user login (U2M) | Run `databricks auth login --host <url>` once, then set `DATABRICKS_CONFIG_PROFILE` (or nothing for `DEFAULT`) |
| Config profile | `DATABRICKS_CONFIG_PROFILE=<name>` from `~/.databrickscfg` |
| Azure | Azure CLI login or the `ARM_*` / `DATABRICKS_AZURE_*` variables |

For anything shared or automated, use a service principal that has only the permissions the assistant needs. The server can do anything that identity can do.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `DATABRICKS_WAREHOUSE_ID` | none | Default warehouse for `execute_sql` |
| `DATABRICKS_MCP_READ_ONLY` | `false` | Only expose tools that can't change anything (also `--read-only`) |
| `DATABRICKS_MCP_TOOLSETS` | all | Comma-separated subset: `clusters,jobs,workspace,volumes,dbfs,sql,unity_catalog,commands` (also `--toolsets`) |
| `DATABRICKS_MCP_MAX_RESPONSE_CHARS` | `50000` | Larger tool results are truncated with a note telling the model to narrow the request |
| `DATABRICKS_MCP_HTTP_TIMEOUT` | `60` | Per-request timeout, in seconds |
| `DATABRICKS_MCP_MAX_RETRIES` | `3` | Retries for 429s and for 5xx/timeouts on idempotent requests |
| `DATABRICKS_MCP_LOG_LEVEL` | `WARNING` | Logs go to stderr (stdout carries the MCP protocol) |
| `DATABRICKS_MCP_TRANSPORT` | `stdio` | `stdio` or `streamable-http` |

### Read-only mode

```bash
databricks-mcp run --read-only
```

This hides every tool that creates, changes or deletes something. That includes `execute_sql` and `run_command`, because arbitrary SQL or code can't be guaranteed read-only. What's left is browsing Unity Catalog, volumes, DBFS, the workspace, clusters, jobs, runs and warehouses. For read-only SQL, give a service principal `SELECT`-only grants and run without `--read-only`.

## Tools

48 tools in 8 toolsets. Run `databricks-mcp list-tools` to see what your settings expose.

<!-- tools:start -->

**Clusters** (`clusters`)

| Tool | Read-only | What it does |
|---|:---:|---|
| `list_clusters` | ✓ | List pinned and active clusters, plus clusters terminated in the last 30 days. |
| `get_cluster` | ✓ | Get the full configuration and state of one cluster. |
| `list_spark_versions` | ✓ | List Databricks Runtime versions (spark_version keys) available for new clusters. |
| `list_node_types` | ✓ | List instance types (node_type_id values) available in this workspace. |
| `create_cluster` |  | Create an all-purpose cluster. This starts billable compute. |
| `start_cluster` |  | Start a terminated cluster. |
| `restart_cluster` |  | Restart a running cluster. Running commands and notebook state are lost. |
| `resize_cluster` |  | Resize a running cluster to a fixed worker count or an autoscale range. |
| `terminate_cluster` |  | Terminate (stop) a cluster. Its config is kept and it can be started again. |

**Jobs** (`jobs`)

| Tool | Read-only | What it does |
|---|:---:|---|
| `list_jobs` | ✓ | List jobs (summary: id, name, creator, created time). |
| `get_job` | ✓ | Get a job's full settings (tasks, clusters, schedule, parameters). |
| `list_job_runs` | ✓ | List job runs, newest first. |
| `get_run` | ✓ | Get a run's details, including per-task state. |
| `get_run_output` | ✓ | Get a run's output: notebook exit value, logs, error and trace. For a multi-task run, returns each task's output. |
| `wait_for_run` | ✓ | Poll a run until it finishes or the timeout passes. Returns its final state. |
| `run_job` |  | Trigger a run of an existing job. Returns run_id. |
| `submit_run` |  | Submit a one-time run without creating a job. Returns run_id. |
| `cancel_run` |  | Cancel an active run. |

**Workspace / notebooks** (`workspace`)

| Tool | Read-only | What it does |
|---|:---:|---|
| `list_workspace` | ✓ | List notebooks, files and folders in a workspace directory. |
| `get_workspace_object` | ✓ | Get metadata (type, language, ids) for a workspace path. |
| `export_notebook` | ✓ | Export a notebook's contents as text. |
| `import_notebook` |  | Create or (with overwrite=true) replace a notebook. language is required for SOURCE. |
| `create_workspace_directory` |  | Create a workspace folder, including missing parents. |

**Unity Catalog volumes (Files API)** (`volumes`)

| Tool | Read-only | What it does |
|---|:---:|---|
| `list_volume_directory` | ✓ | List files and folders in a Unity Catalog volume directory. |
| `read_volume_file` | ✓ | Read the start of a text file in a volume (up to max_bytes). |
| `write_volume_file` |  | Write a text file to a volume. Fails if it exists unless overwrite=true. |
| `delete_volume_file` |  | Delete a file from a volume. |
| `create_volume_directory` |  | Create a directory in a volume, including missing parents. |

**DBFS (legacy, read-only)** (`dbfs`)

| Tool | Read-only | What it does |
|---|:---:|---|
| `list_dbfs` | ✓ | List a legacy DBFS path. Prefer volumes for new data. |
| `read_dbfs_file` | ✓ | Read the start of a text file on legacy DBFS. |

**SQL warehouses & queries** (`sql`)

| Tool | Read-only | What it does |
|---|:---:|---|
| `list_sql_warehouses` | ✓ | List SQL warehouses with their id, state and size. |
| `get_sql_warehouse` | ✓ | Get one SQL warehouse's full configuration and state. |
| `start_sql_warehouse` |  | Start a stopped SQL warehouse. |
| `stop_sql_warehouse` |  | Stop a SQL warehouse. Running queries on it are cancelled. |
| `execute_sql` |  | Run a SQL statement on a SQL warehouse and return columns + rows. If it outlives timeout_seconds you get a statement_id to poll with get_sql_statement. |
| `get_sql_statement` | ✓ | Get the state and (if finished) results of a SQL statement. |
| `cancel_sql_statement` |  | Cancel a running SQL statement. |

**Unity Catalog** (`unity_catalog`)

| Tool | Read-only | What it does |
|---|:---:|---|
| `list_catalogs` | ✓ | List Unity Catalog catalogs you can see. |
| `list_schemas` | ✓ | List schemas in a catalog. |
| `list_tables` | ✓ | List tables and views in a schema (names and types; use get_table for columns). |
| `get_table` | ✓ | Get a table's columns, types, comments, format and storage location. |
| `list_volumes` | ✓ | List Unity Catalog volumes in a schema. |

**Run code on a cluster** (`commands`)

| Tool | Read-only | What it does |
|---|:---:|---|
| `run_command` |  | Run one snippet of code on a running cluster and return its result. Creates and destroys a throwaway context; use create_execution_context to keep state between calls. |
| `create_execution_context` |  | Create a stateful execution context on a running cluster. Returns its id. |
| `execute_command` |  | Run code in an existing context; variables persist between calls. |
| `get_command_status` | ✓ | Get a command's status and, when finished, its results. |
| `cancel_command` |  | Cancel a running command. |
| `destroy_execution_context` |  | Destroy an execution context and free its resources on the cluster. |

<!-- tools:end -->
## Databricks API versions used

| Area | Endpoint |
|---|---|
| Jobs | `/api/2.2/jobs/*` (current version; queues runs by default, pages tasks past 100) |
| Clusters | `/api/2.1/clusters/*` |
| Unity Catalog | `/api/2.1/unity-catalog/*` |
| Volumes | Files API `/api/2.0/fs/files`, `/api/2.0/fs/directories` |
| SQL | `/api/2.0/sql/statements`, `/api/2.0/sql/warehouses` |
| Workspace | `/api/2.0/workspace/*` |
| Commands | `/api/1.2/contexts/*`, `/api/1.2/commands/*` (still the current version) |
| DBFS | `/api/2.0/dbfs/*` (legacy; read-only here) |

Files API 3.0 is still in beta. It needs an admin preview toggle and a regional hostname, so the server stays on 2.0 for now.

## Upgrading from 0.1.x

0.2.0 is a rewrite, and several things changed:

- **Install and command:** install with `pip install databricks-mcp-toolkit`, then run `databricks-mcp` (or `python -m databricks_mcp`). Clone-and-`pip install -e .` is no longer needed, and the old `start_mcp_server.sh` and `.ps1` scripts are gone.
- **Tool arguments:** tools now take typed, named arguments. Before, every tool took one opaque `params` object.
- **Renamed tools:**
  - `list_notebooks` → `list_workspace`
  - `list_files` → `list_dbfs`
  - `submit_single_run` → `submit_run`
  - `wait_for_run_completion` → `wait_for_run`
  - `execute_command_simple` → `run_command`
  - `get_context_status` was removed
- **MCP SDK:** it now requires `mcp>=2.3`. The 0.1.x code imported `FastMCP`, which doesn't exist in MCP 2.x, so a fresh install of 0.1.x fails to start.

## Development

```bash
git clone https://github.com/kshptl/Databricks-MCP-Server
cd Databricks-MCP-Server
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest              # unit tests; HTTP is mocked, so no workspace is needed
ruff check src tests && ruff format --check src tests
```

See [CONTRIBUTING.md](CONTRIBUTING.md) and [ARCHITECTURE.md](ARCHITECTURE.md). Releases are covered in [RELEASING.md](RELEASING.md).

## License

Dual-licensed:

- **[AGPL-3.0-only](LICENSE)** for open-source use. If you run a modified version as a network service, you must offer its source to the service's users.
- **Commercial license** if the AGPL terms don't fit your use. Open an issue or contact the author through their GitHub profile.

This project is not affiliated with or endorsed by Databricks, Inc.
