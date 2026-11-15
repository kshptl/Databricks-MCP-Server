# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""Jobs API 2.2 (current). 2.2 pages tasks/job_clusters past 100 entries and queues jobs by default."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from databricks_mcp.client import DatabricksAPIError, DatabricksClient

BASE = "/api/2.2/jobs"
_TERMINAL_LIFE_CYCLE = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
_MAX_TASK_OUTPUTS = 20


async def list_jobs(
    client: DatabricksClient,
    name: str | None = None,
    limit: int = 25,
    page_token: str | None = None,
    expand_tasks: bool = False,
) -> dict[str, Any]:
    return await client.get(
        f"{BASE}/list",
        {
            "name": name,
            "limit": max(1, min(limit, 100)),
            "page_token": page_token,
            "expand_tasks": expand_tasks or None,
        },
    )


async def get_job(client: DatabricksClient, job_id: int, page_token: str | None = None) -> dict[str, Any]:
    return await client.get(f"{BASE}/get", {"job_id": job_id, "page_token": page_token})


async def run_job(
    client: DatabricksClient,
    job_id: int,
    job_parameters: dict[str, str] | None = None,
    notebook_params: dict[str, str] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"job_id": job_id}
    if job_parameters:
        body["job_parameters"] = job_parameters
    if notebook_params:
        body["notebook_params"] = notebook_params
    return await client.post(f"{BASE}/run-now", body)


async def submit_run(client: DatabricksClient, run_spec: dict[str, Any]) -> dict[str, Any]:
    if not run_spec.get("tasks"):
        raise ValueError("run_spec needs a non-empty 'tasks' list.")
    return await client.post(f"{BASE}/runs/submit", run_spec)


async def list_runs(
    client: DatabricksClient,
    job_id: int | None = None,
    active_only: bool = False,
    completed_only: bool = False,
    limit: int = 25,
    page_token: str | None = None,
) -> dict[str, Any]:
    return await client.get(
        f"{BASE}/runs/list",
        {
            "job_id": job_id,
            "active_only": active_only or None,
            "completed_only": completed_only or None,
            "limit": max(1, min(limit, 25)),
            "page_token": page_token,
        },
    )


async def get_run(client: DatabricksClient, run_id: int, page_token: str | None = None) -> dict[str, Any]:
    return await client.get(f"{BASE}/runs/get", {"run_id": run_id, "page_token": page_token})


async def cancel_run(client: DatabricksClient, run_id: int) -> dict[str, Any]:
    return await client.post(f"{BASE}/runs/cancel", {"run_id": run_id})


async def get_run_output(client: DatabricksClient, run_id: int) -> dict[str, Any]:
    """runs/get-output only works on task runs, not on a multi-task parent run.
    If I'm handed a parent run, I fetch each task's output instead."""
    run = await get_run(client, run_id)
    tasks = run.get("tasks") or []
    task_runs = [t for t in tasks if t.get("run_id") and t["run_id"] != run_id]
    if not task_runs:
        return await client.get(f"{BASE}/runs/get-output", {"run_id": run_id})

    outputs = []
    for task in task_runs[:_MAX_TASK_OUTPUTS]:
        task_run_id = task["run_id"]
        try:
            out = await client.get(f"{BASE}/runs/get-output", {"run_id": task_run_id})
        except DatabricksAPIError as exc:
            out = {"error": str(exc)}
        out.pop("metadata", None)  # duplicates what get_run already returns
        outputs.append({"task_key": task.get("task_key"), "run_id": task_run_id, **out})
    result: dict[str, Any] = {"run_id": run_id, "state": _state(run), "tasks": outputs}
    if len(task_runs) > _MAX_TASK_OUTPUTS:
        result["note"] = f"Showing {_MAX_TASK_OUTPUTS} of {len(task_runs)} task outputs."
    return result


def _state(run: dict[str, Any]) -> dict[str, Any]:
    return {"status": run.get("status"), "state": run.get("state")}


def is_terminal(run: dict[str, Any]) -> bool:
    status = (run.get("status") or {}).get("state")
    life_cycle = (run.get("state") or {}).get("life_cycle_state")
    return status == "TERMINATED" or life_cycle in _TERMINAL_LIFE_CYCLE


async def wait_for_run(
    client: DatabricksClient,
    run_id: int,
    poll_interval_seconds: float = 10,
    timeout_seconds: float = 600,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        run = await get_run(client, run_id)
        if is_terminal(run):
            return {"run_id": run_id, "finished": True, **_state(run), "run_page_url": run.get("run_page_url")}
        if time.monotonic() >= deadline:
            return {
                "run_id": run_id,
                "finished": False,
                **_state(run),
                "run_page_url": run.get("run_page_url"),
                "note": f"Still running after {timeout_seconds}s. Call again to keep waiting.",
            }
        await asyncio.sleep(poll_interval_seconds)
