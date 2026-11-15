# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""Command Execution API 1.2. Still the current (and only) version for running code
on an all-purpose cluster with state kept between commands."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from databricks_mcp.client import DatabricksClient

BASE = "/api/1.2"
_RUNNING = {"Queued", "Running", "Cancelling"}


async def create_context(client: DatabricksClient, cluster_id: str, language: str) -> dict[str, Any]:
    return await client.post(f"{BASE}/contexts/create", {"clusterId": cluster_id, "language": language})


async def context_status(client: DatabricksClient, cluster_id: str, context_id: str) -> dict[str, Any]:
    return await client.get(f"{BASE}/contexts/status", {"clusterId": cluster_id, "contextId": context_id})


async def destroy_context(client: DatabricksClient, cluster_id: str, context_id: str) -> dict[str, Any]:
    return await client.post(f"{BASE}/contexts/destroy", {"clusterId": cluster_id, "contextId": context_id})


async def execute(
    client: DatabricksClient, cluster_id: str, context_id: str, command: str, language: str
) -> dict[str, Any]:
    return await client.post(
        f"{BASE}/commands/execute",
        {"clusterId": cluster_id, "contextId": context_id, "command": command, "language": language},
    )


async def command_status(client: DatabricksClient, cluster_id: str, context_id: str, command_id: str) -> dict[str, Any]:
    return await client.get(
        f"{BASE}/commands/status",
        {"clusterId": cluster_id, "contextId": context_id, "commandId": command_id},
    )


async def cancel(client: DatabricksClient, cluster_id: str, context_id: str, command_id: str) -> dict[str, Any]:
    return await client.post(
        f"{BASE}/commands/cancel",
        {"clusterId": cluster_id, "contextId": context_id, "commandId": command_id},
    )


async def wait_for_command(
    client: DatabricksClient,
    cluster_id: str,
    context_id: str,
    command_id: str,
    timeout_seconds: float = 120,
    poll_interval_seconds: float = 1,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        status = await command_status(client, cluster_id, context_id, command_id)
        if status.get("status") not in _RUNNING or time.monotonic() >= deadline:
            return status
        await asyncio.sleep(poll_interval_seconds)


async def run_once(
    client: DatabricksClient,
    cluster_id: str,
    command: str,
    language: str,
    timeout_seconds: float = 120,
    poll_interval_seconds: float = 1,
) -> dict[str, Any]:
    """Create a context, run one command, wait, and always clean the context up."""
    context = await create_context(client, cluster_id, language)
    context_id = context["id"]
    try:
        started = await execute(client, cluster_id, context_id, command, language)
        return await wait_for_command(
            client, cluster_id, context_id, started["id"], timeout_seconds, poll_interval_seconds
        )
    finally:
        try:
            await destroy_context(client, cluster_id, context_id)
        except Exception:  # cleanup failure shouldn't hide the command's result
            pass
