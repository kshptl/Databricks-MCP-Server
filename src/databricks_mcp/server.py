# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""MCP server: registers one tool per Databricks operation."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

from databricks_mcp import __version__
from databricks_mcp.api import (
    clusters,
    command_execution,
    dbfs,
    files,
    jobs,
    sql,
    unity_catalog,
    warehouses,
    workspace,
)
from databricks_mcp.client import DatabricksAPIError, DatabricksClient
from databricks_mcp.config import Settings

logger = logging.getLogger(__name__)

TOOLSETS = ("clusters", "jobs", "workspace", "volumes", "dbfs", "sql", "unity_catalog", "commands")

INSTRUCTIONS = """\
Tools for a Databricks workspace. Tips:
- Explore data with list_catalogs -> list_schemas -> list_tables -> get_table, then query with execute_sql.
- execute_sql needs a SQL warehouse; use list_sql_warehouses if no default is configured.
- Files in Unity Catalog volumes live under /Volumes/<catalog>/<schema>/<volume>/.
- Long-running work (job runs, SQL, commands) returns an id; poll it rather than resubmitting.
- List tools return a page plus next_page_token when there is more.
"""

Language = Literal["python", "scala", "sql", "r"]


def _ann(read_only: bool, destructive: bool = False, idempotent: bool = False) -> ToolAnnotations:
    return ToolAnnotations(
        read_only_hint=read_only,
        destructive_hint=destructive if not read_only else False,
        idempotent_hint=idempotent or read_only,
        open_world_hint=True,
    )


def _slim(items: Iterable[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    return [{k: item[k] for k in keys if k in item} for item in items]


def build_server(
    client: DatabricksClient | None = None,
    settings: Settings | None = None,
    toolsets: Iterable[str] | None = None,
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "WARNING",
) -> MCPServer:
    settings = settings or Settings.from_env()
    client = client or DatabricksClient(timeout_seconds=settings.http_timeout_seconds, max_retries=settings.max_retries)
    enabled = set(toolsets) if toolsets is not None else set(TOOLSETS)
    unknown = enabled - set(TOOLSETS)
    if unknown:
        raise ValueError(f"Unknown toolset(s): {sorted(unknown)}. Valid: {', '.join(TOOLSETS)}")

    @asynccontextmanager
    async def lifespan(_server: MCPServer) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await client.aclose()

    mode = " (read-only mode: write tools are disabled)" if settings.read_only else ""
    server = MCPServer(
        name="databricks-mcp",
        version=__version__,
        instructions=INSTRUCTIONS + mode,
        website_url="https://github.com/kshptl/Databricks-MCP-Server",
        lifespan=lifespan,
        log_level=log_level,
    )

    def dump(result: Any) -> str:
        text = json.dumps(result, default=str, ensure_ascii=False)
        limit = settings.max_response_chars
        if len(text) > limit:
            return (
                text[:limit] + f"\n...[truncated {len(text) - limit} characters. Narrow the request: use "
                "limit/max_results, page_token, filters, or a more specific path.]"
            )
        return text

    async def run(call: Awaitable[Any]) -> str:
        # Expected failures become ToolError so the model sees the actual message
        # (the SDK hides the text of any other exception).
        try:
            return dump(await call)
        except DatabricksAPIError as exc:
            raise ToolError(str(exc)) from exc
        except ValueError as exc:
            raise ToolError(str(exc)) from exc

    def tool(
        toolset: str, read_only: bool, destructive: bool = False, idempotent: bool = False
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def register(fn: Callable[..., Any]) -> Callable[..., Any]:
            if toolset not in enabled or (settings.read_only and not read_only):
                return fn
            return server.tool(annotations=_ann(read_only, destructive, idempotent))(fn)

        return register

    # ---------------------------------------------------------------- clusters

    @tool("clusters", read_only=True)
    async def list_clusters(
        full: Annotated[bool, Field(description="Return the full cluster objects instead of a summary.")] = False,
        page_token: str | None = None,
    ) -> str:
        """List pinned and active clusters, plus clusters terminated in the last 30 days."""

        async def call() -> Any:
            result = await clusters.list_clusters(client, page_size=100, page_token=page_token)
            if not full:
                result["clusters"] = _slim(
                    result.get("clusters", []),
                    (
                        "cluster_id",
                        "cluster_name",
                        "state",
                        "spark_version",
                        "node_type_id",
                        "num_workers",
                        "autoscale",
                        "creator_user_name",
                        "cluster_source",
                        "autotermination_minutes",
                    ),
                )
            return result

        return await run(call())

    @tool("clusters", read_only=True)
    async def get_cluster(cluster_id: str) -> str:
        """Get the full configuration and state of one cluster."""
        return await run(clusters.get_cluster(client, cluster_id))

    @tool("clusters", read_only=True)
    async def list_spark_versions() -> str:
        """List Databricks Runtime versions (spark_version keys) available for new clusters."""
        return await run(clusters.list_spark_versions(client))

    @tool("clusters", read_only=True)
    async def list_node_types() -> str:
        """List instance types (node_type_id values) available in this workspace."""

        async def call() -> Any:
            result = await clusters.list_node_types(client)
            result["node_types"] = _slim(
                result.get("node_types", []),
                ("node_type_id", "memory_mb", "num_cores", "num_gpus", "category", "is_deprecated"),
            )
            return result

        return await run(call())

    @tool("clusters", read_only=False)
    async def create_cluster(
        cluster_name: str,
        spark_version: Annotated[
            str, Field(description="Runtime key from list_spark_versions, e.g. '16.4.x-scala2.12'.")
        ],
        node_type_id: Annotated[str, Field(description="Instance type from list_node_types.")],
        num_workers: Annotated[int | None, Field(description="Fixed worker count. Omit if using autoscale.")] = None,
        autoscale_min_workers: int | None = None,
        autoscale_max_workers: int | None = None,
        autotermination_minutes: Annotated[
            int, Field(description="Idle minutes before auto-terminate. 0 disables it.")
        ] = 60,
        extra_spec: Annotated[
            dict[str, Any] | None,
            Field(
                description="Any other Clusters API create fields, e.g. spark_conf, custom_tags, data_security_mode."
            ),
        ] = None,
    ) -> str:
        """Create an all-purpose cluster. This starts billable compute."""
        spec: dict[str, Any] = {
            **(extra_spec or {}),
            "cluster_name": cluster_name,
            "spark_version": spark_version,
            "node_type_id": node_type_id,
            "autotermination_minutes": autotermination_minutes,
        }
        if autoscale_min_workers is not None and autoscale_max_workers is not None:
            spec["autoscale"] = {"min_workers": autoscale_min_workers, "max_workers": autoscale_max_workers}
        else:
            spec["num_workers"] = num_workers or 0
        return await run(clusters.create_cluster(client, spec))

    @tool("clusters", read_only=False, idempotent=True)
    async def start_cluster(cluster_id: str) -> str:
        """Start a terminated cluster."""
        return await run(clusters.start_cluster(client, cluster_id))

    @tool("clusters", read_only=False, destructive=True)
    async def restart_cluster(cluster_id: str) -> str:
        """Restart a running cluster. Running commands and notebook state are lost."""
        return await run(clusters.restart_cluster(client, cluster_id))

    @tool("clusters", read_only=False, idempotent=True)
    async def resize_cluster(
        cluster_id: str,
        num_workers: int | None = None,
        autoscale_min_workers: int | None = None,
        autoscale_max_workers: int | None = None,
    ) -> str:
        """Resize a running cluster to a fixed worker count or an autoscale range."""
        return await run(
            clusters.resize_cluster(client, cluster_id, num_workers, autoscale_min_workers, autoscale_max_workers)
        )

    @tool("clusters", read_only=False, destructive=True, idempotent=True)
    async def terminate_cluster(cluster_id: str) -> str:
        """Terminate (stop) a cluster. Its config is kept and it can be started again."""
        return await run(clusters.terminate_cluster(client, cluster_id))

    # ---------------------------------------------------------------- jobs

    @tool("jobs", read_only=True)
    async def list_jobs(
        name: Annotated[str | None, Field(description="Exact job name filter (case-insensitive).")] = None,
        limit: Annotated[int, Field(ge=1, le=100)] = 25,
        page_token: str | None = None,
    ) -> str:
        """List jobs (summary: id, name, creator, created time)."""

        async def call() -> Any:
            result = await jobs.list_jobs(client, name=name, limit=limit, page_token=page_token)
            result["jobs"] = [
                {
                    "job_id": j.get("job_id"),
                    "name": (j.get("settings") or {}).get("name"),
                    "creator_user_name": j.get("creator_user_name"),
                    "created_time": j.get("created_time"),
                }
                for j in result.get("jobs", [])
            ]
            return result

        return await run(call())

    @tool("jobs", read_only=True)
    async def get_job(
        job_id: int,
        page_token: Annotated[str | None, Field(description="For jobs with more than 100 tasks.")] = None,
    ) -> str:
        """Get a job's full settings (tasks, clusters, schedule, parameters)."""
        return await run(jobs.get_job(client, job_id, page_token))

    @tool("jobs", read_only=True)
    async def list_job_runs(
        job_id: Annotated[int | None, Field(description="Omit to list runs across all jobs.")] = None,
        active_only: bool = False,
        completed_only: bool = False,
        limit: Annotated[int, Field(ge=1, le=25)] = 25,
        page_token: str | None = None,
    ) -> str:
        """List job runs, newest first."""

        async def call() -> Any:
            result = await jobs.list_runs(client, job_id, active_only, completed_only, limit, page_token)
            result["runs"] = _slim(
                result.get("runs", []),
                (
                    "run_id",
                    "job_id",
                    "run_name",
                    "status",
                    "state",
                    "start_time",
                    "end_time",
                    "run_duration",
                    "trigger",
                    "run_page_url",
                ),
            )
            return result

        return await run(call())

    @tool("jobs", read_only=True)
    async def get_run(run_id: int, page_token: str | None = None) -> str:
        """Get a run's details, including per-task state."""
        return await run(jobs.get_run(client, run_id, page_token))

    @tool("jobs", read_only=True)
    async def get_run_output(run_id: int) -> str:
        """Get a run's output: notebook exit value, logs, error and trace. For a multi-task
        run, returns each task's output."""
        return await run(jobs.get_run_output(client, run_id))

    @tool("jobs", read_only=True)
    async def wait_for_run(
        run_id: int,
        timeout_seconds: Annotated[int, Field(ge=1, le=1800)] = 600,
        poll_interval_seconds: Annotated[int, Field(ge=1, le=300)] = 10,
    ) -> str:
        """Poll a run until it finishes or the timeout passes. Returns its final state."""
        return await run(jobs.wait_for_run(client, run_id, poll_interval_seconds, timeout_seconds))

    @tool("jobs", read_only=False)
    async def run_job(
        job_id: int,
        job_parameters: Annotated[
            dict[str, str] | None, Field(description="Job-level parameters (preferred for jobs that define them).")
        ] = None,
        notebook_params: Annotated[
            dict[str, str] | None, Field(description="Legacy notebook widget params for older jobs.")
        ] = None,
    ) -> str:
        """Trigger a run of an existing job. Returns run_id."""
        return await run(jobs.run_job(client, job_id, job_parameters, notebook_params))

    @tool("jobs", read_only=False)
    async def submit_run(
        run_spec: Annotated[
            dict[str, Any],
            Field(
                description=(
                    "Jobs API runs/submit body. Needs 'tasks': a list of {task_key, notebook_task | "
                    "spark_python_task | sql_task | ..., plus existing_cluster_id, new_cluster, or "
                    "environment_key for serverless}. Optional: run_name, timeout_seconds, git_source."
                )
            ),
        ],
    ) -> str:
        """Submit a one-time run without creating a job. Returns run_id."""
        return await run(jobs.submit_run(client, run_spec))

    @tool("jobs", read_only=False, destructive=True, idempotent=True)
    async def cancel_run(run_id: int) -> str:
        """Cancel an active run."""
        return await run(jobs.cancel_run(client, run_id))

    # ---------------------------------------------------------------- workspace

    @tool("workspace", read_only=True)
    async def list_workspace(
        path: Annotated[str, Field(description="Workspace folder, e.g. /Users/me@example.com or /Shared.")],
    ) -> str:
        """List notebooks, files and folders in a workspace directory."""
        return await run(workspace.list_objects(client, path))

    @tool("workspace", read_only=True)
    async def get_workspace_object(path: str) -> str:
        """Get metadata (type, language, ids) for a workspace path."""
        return await run(workspace.get_status(client, path))

    @tool("workspace", read_only=True)
    async def export_notebook(
        path: str,
        format: Literal["SOURCE", "JUPYTER", "HTML"] = "SOURCE",
    ) -> str:
        """Export a notebook's contents as text."""
        return await run(workspace.export_notebook(client, path, format))

    @tool("workspace", read_only=False, destructive=True)
    async def import_notebook(
        path: str,
        content: Annotated[str, Field(description="Notebook source as plain text (not base64).")],
        language: Language | None = None,
        format: Literal["SOURCE", "JUPYTER"] = "SOURCE",
        overwrite: bool = False,
    ) -> str:
        """Create or (with overwrite=true) replace a notebook. language is required for SOURCE."""
        lang = language.upper() if language else None
        return await run(workspace.import_notebook(client, path, content, lang, format, overwrite))

    @tool("workspace", read_only=False, idempotent=True)
    async def create_workspace_directory(path: str) -> str:
        """Create a workspace folder, including missing parents."""
        return await run(workspace.mkdirs(client, path))

    # ---------------------------------------------------------------- volumes (Files API)

    @tool("volumes", read_only=True)
    async def list_volume_directory(
        path: Annotated[str, Field(description="e.g. /Volumes/main/default/raw/2026/")],
        page_token: str | None = None,
    ) -> str:
        """List files and folders in a Unity Catalog volume directory."""
        return await run(files.list_directory(client, path, page_size=500, page_token=page_token))

    @tool("volumes", read_only=True)
    async def read_volume_file(
        path: str,
        max_bytes: Annotated[int, Field(ge=1, le=1_000_000)] = 100_000,
    ) -> str:
        """Read the start of a text file in a volume (up to max_bytes)."""
        return await run(files.read_file(client, path, max_bytes))

    @tool("volumes", read_only=False, destructive=True, idempotent=True)
    async def write_volume_file(path: str, content: str, overwrite: bool = False) -> str:
        """Write a text file to a volume. Fails if it exists unless overwrite=true."""
        return await run(files.write_file(client, path, content, overwrite))

    @tool("volumes", read_only=False, destructive=True, idempotent=True)
    async def delete_volume_file(path: str) -> str:
        """Delete a file from a volume."""
        return await run(files.delete_file(client, path))

    @tool("volumes", read_only=False, idempotent=True)
    async def create_volume_directory(path: str) -> str:
        """Create a directory in a volume, including missing parents."""
        return await run(files.create_directory(client, path))

    # ---------------------------------------------------------------- dbfs (legacy)

    @tool("dbfs", read_only=True)
    async def list_dbfs(path: Annotated[str, Field(description="DBFS path, e.g. /FileStore or dbfs:/tmp.")]) -> str:
        """List a legacy DBFS path. Prefer volumes for new data."""
        return await run(dbfs.list_files(client, path))

    @tool("dbfs", read_only=True)
    async def read_dbfs_file(path: str, max_bytes: Annotated[int, Field(ge=1, le=1_048_576)] = 100_000) -> str:
        """Read the start of a text file on legacy DBFS."""
        return await run(dbfs.read_file(client, path, max_bytes))

    # ---------------------------------------------------------------- sql

    @tool("sql", read_only=True)
    async def list_sql_warehouses() -> str:
        """List SQL warehouses with their id, state and size."""

        async def call() -> Any:
            result = await warehouses.list_warehouses(client)
            result["warehouses"] = _slim(
                result.get("warehouses", []),
                (
                    "id",
                    "name",
                    "state",
                    "cluster_size",
                    "warehouse_type",
                    "enable_serverless_compute",
                    "auto_stop_mins",
                    "creator_name",
                ),
            )
            return result

        return await run(call())

    @tool("sql", read_only=True)
    async def get_sql_warehouse(warehouse_id: str) -> str:
        """Get one SQL warehouse's full configuration and state."""
        return await run(warehouses.get_warehouse(client, warehouse_id))

    @tool("sql", read_only=False, idempotent=True)
    async def start_sql_warehouse(warehouse_id: str) -> str:
        """Start a stopped SQL warehouse."""
        return await run(warehouses.start_warehouse(client, warehouse_id))

    @tool("sql", read_only=False, destructive=True, idempotent=True)
    async def stop_sql_warehouse(warehouse_id: str) -> str:
        """Stop a SQL warehouse. Running queries on it are cancelled."""
        return await run(warehouses.stop_warehouse(client, warehouse_id))

    @tool("sql", read_only=False, destructive=True)
    async def execute_sql(
        statement: str,
        warehouse_id: Annotated[
            str | None, Field(description="Defaults to DATABRICKS_WAREHOUSE_ID. See list_sql_warehouses.")
        ] = None,
        catalog: str | None = None,
        schema: str | None = None,
        parameters: Annotated[
            dict[str, str] | None, Field(description="Named parameters, referenced as :name in the statement.")
        ] = None,
        row_limit: Annotated[int, Field(ge=1, le=10_000)] = 1000,
        timeout_seconds: Annotated[int, Field(ge=5, le=900)] = 120,
    ) -> str:
        """Run a SQL statement on a SQL warehouse and return columns + rows. If it outlives
        timeout_seconds you get a statement_id to poll with get_sql_statement."""
        target = warehouse_id or settings.warehouse_id
        if not target:
            raise ToolError("No warehouse_id given and DATABRICKS_WAREHOUSE_ID is not set. Call list_sql_warehouses.")
        return await run(
            sql.execute(client, statement, target, catalog, schema, parameters, row_limit, timeout_seconds)
        )

    @tool("sql", read_only=True)
    async def get_sql_statement(statement_id: str) -> str:
        """Get the state and (if finished) results of a SQL statement."""
        return await run(sql.get_statement(client, statement_id))

    @tool("sql", read_only=False, idempotent=True)
    async def cancel_sql_statement(statement_id: str) -> str:
        """Cancel a running SQL statement."""
        return await run(sql.cancel_statement(client, statement_id))

    # ---------------------------------------------------------------- unity catalog

    @tool("unity_catalog", read_only=True)
    async def list_catalogs(
        max_results: Annotated[int, Field(ge=1, le=1000)] = 100, page_token: str | None = None
    ) -> str:
        """List Unity Catalog catalogs you can see."""

        async def call() -> Any:
            result = await unity_catalog.list_catalogs(client, max_results, page_token)
            result["catalogs"] = _slim(result.get("catalogs", []), ("name", "catalog_type", "comment", "owner"))
            return result

        return await run(call())

    @tool("unity_catalog", read_only=True)
    async def list_schemas(
        catalog_name: str, max_results: Annotated[int, Field(ge=1, le=1000)] = 100, page_token: str | None = None
    ) -> str:
        """List schemas in a catalog."""

        async def call() -> Any:
            result = await unity_catalog.list_schemas(client, catalog_name, max_results, page_token)
            result["schemas"] = _slim(result.get("schemas", []), ("name", "full_name", "comment", "owner"))
            return result

        return await run(call())

    @tool("unity_catalog", read_only=True)
    async def list_tables(
        catalog_name: str,
        schema_name: str,
        max_results: Annotated[int, Field(ge=1, le=1000)] = 100,
        page_token: str | None = None,
    ) -> str:
        """List tables and views in a schema (names and types; use get_table for columns)."""

        async def call() -> Any:
            result = await unity_catalog.list_tables(client, catalog_name, schema_name, max_results, page_token)
            result["tables"] = _slim(
                result.get("tables", []),
                ("name", "full_name", "table_type", "data_source_format", "comment", "updated_at"),
            )
            return result

        return await run(call())

    @tool("unity_catalog", read_only=True)
    async def get_table(
        full_name: Annotated[str, Field(description="catalog.schema.table")],
    ) -> str:
        """Get a table's columns, types, comments, format and storage location."""

        async def call() -> Any:
            table = await unity_catalog.get_table(client, full_name)
            table["columns"] = _slim(
                table.get("columns", []), ("name", "type_text", "nullable", "comment", "partition_index")
            )
            return table

        return await run(call())

    @tool("unity_catalog", read_only=True)
    async def list_volumes(
        catalog_name: str,
        schema_name: str,
        max_results: Annotated[int, Field(ge=1, le=1000)] = 100,
        page_token: str | None = None,
    ) -> str:
        """List Unity Catalog volumes in a schema."""
        return await run(unity_catalog.list_volumes(client, catalog_name, schema_name, max_results, page_token))

    # ---------------------------------------------------------------- commands (Command Execution 1.2)

    @tool("commands", read_only=False, destructive=True)
    async def run_command(
        cluster_id: str,
        command: str,
        language: Language = "python",
        timeout_seconds: Annotated[int, Field(ge=1, le=900)] = 120,
    ) -> str:
        """Run one snippet of code on a running cluster and return its result. Creates and
        destroys a throwaway context; use create_execution_context to keep state between calls."""
        return await run(command_execution.run_once(client, cluster_id, command, language, timeout_seconds))

    @tool("commands", read_only=False)
    async def create_execution_context(cluster_id: str, language: Language = "python") -> str:
        """Create a stateful execution context on a running cluster. Returns its id."""
        return await run(command_execution.create_context(client, cluster_id, language))

    @tool("commands", read_only=False, destructive=True)
    async def execute_command(
        cluster_id: str,
        context_id: str,
        command: str,
        language: Language = "python",
        wait: Annotated[bool, Field(description="Wait for the result instead of returning the command id.")] = True,
        timeout_seconds: Annotated[int, Field(ge=1, le=900)] = 120,
    ) -> str:
        """Run code in an existing context; variables persist between calls."""

        async def call() -> Any:
            started = await command_execution.execute(client, cluster_id, context_id, command, language)
            if not wait:
                return started
            return await command_execution.wait_for_command(
                client, cluster_id, context_id, started["id"], timeout_seconds
            )

        return await run(call())

    @tool("commands", read_only=True)
    async def get_command_status(cluster_id: str, context_id: str, command_id: str) -> str:
        """Get a command's status and, when finished, its results."""
        return await run(command_execution.command_status(client, cluster_id, context_id, command_id))

    @tool("commands", read_only=False, idempotent=True)
    async def cancel_command(cluster_id: str, context_id: str, command_id: str) -> str:
        """Cancel a running command."""
        return await run(command_execution.cancel(client, cluster_id, context_id, command_id))

    @tool("commands", read_only=False, idempotent=True)
    async def destroy_execution_context(cluster_id: str, context_id: str) -> str:
        """Destroy an execution context and free its resources on the cluster."""
        return await run(command_execution.destroy_context(client, cluster_id, context_id))

    return server
