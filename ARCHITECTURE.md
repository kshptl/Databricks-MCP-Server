# Architecture

```
MCP client (Claude, Cursor, ...)
        │  stdio or streamable HTTP
        ▼
cli.py ──► server.py   build_server(): one MCPServer, one tool per operation,
                       toolset / read-only filtering, error + size handling
              │
              ▼
           api/*.py    thin functions per Databricks REST area
              │
              ▼
           client.py   DatabricksClient: httpx, auth, retries, error parsing
              │
              ▼
        Databricks REST API  (auth via databricks-sdk unified auth)
```

## Modules

| File | Responsibility |
|---|---|
| `cli.py` | Implements the `databricks-mcp` command (`run`, `list-tools`, `--version`). Sends logs to stderr. |
| `config.py` | Holds `Settings` read from `DATABRICKS_MCP_*` env vars. Auth is not configured here. |
| `client.py` | `DatabricksClient` and `DatabricksAPIError`. Gets auth headers per request from `databricks-sdk` (`Config().authenticate()`), so OAuth tokens refresh automatically. |
| `server.py` | `build_server()` registers the tools, sets their MCP annotations, filters by toolset and read-only mode, turns errors into `ToolError`, and truncates large responses. |
| `api/clusters.py` | Clusters API 2.1 |
| `api/jobs.py` | Jobs API 2.2, run polling and per-task output fan-out |
| `api/sql.py` | SQL Statement Execution. Waits up to 50s on the server, then polls client-side, and flattens the results into columns and rows. |
| `api/warehouses.py` | SQL Warehouses API |
| `api/unity_catalog.py` | Unity Catalog 2.1 catalogs, schemas, tables and volumes |
| `api/files.py` | Files API 2.0 for `/Volumes/...` paths. Uses Range reads. |
| `api/workspace.py` | Workspace API: list, export (decoded), import (encoded), mkdirs |
| `api/dbfs.py` | DBFS, read-only (legacy) |
| `api/command_execution.py` | Command Execution 1.2, including `run_once`, which always destroys its context |

## Design decisions

- **Retries.** The client always retries 429s, honoring `Retry-After`. It retries connection failures that never reached the server. It retries 5xx responses and timeouts only for GET, HEAD, PUT and DELETE. A POST like `clusters/create` is not safe to resend.
- **Errors.** Expected failures (`DatabricksAPIError`, `ValueError`) become `ToolError`, so the model sees the real message, such as `HTTP 400 INVALID_PARAMETER_VALUE: ...`. Anything else is a crash: the SDK hides its text from the model and logs the traceback.
- **Context budget.** List tools return slimmed records plus pagination tokens. Every response is capped at `DATABRICKS_MCP_MAX_RESPONSE_CHARS`.
- **Read-only mode.** It relies on each tool's `read_only` flag. SQL and code execution count as writes because their effects can't be checked ahead of time.
- **Testing.** All HTTP is mocked with `respx`. Server tests go through a real in-process MCP `Client`, so schema validation and error conversion are exercised end to end.
