# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""SQL Statement Execution API 2.0."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from databricks_mcp.client import DatabricksClient

BASE = "/api/2.0/sql/statements"
_TERMINAL = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}
# The API lets a request block for at most 50s; after that I poll.
_MAX_SERVER_WAIT = 50


async def execute(
    client: DatabricksClient,
    statement: str,
    warehouse_id: str,
    catalog: str | None = None,
    schema: str | None = None,
    parameters: dict[str, str] | None = None,
    row_limit: int = 1000,
    timeout_seconds: int = 120,
    poll_interval_seconds: float = 2,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "statement": statement,
        "warehouse_id": warehouse_id,
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
        "row_limit": row_limit,
        "wait_timeout": f"{max(5, min(timeout_seconds, _MAX_SERVER_WAIT))}s",
        "on_wait_timeout": "CONTINUE",
    }
    if catalog:
        body["catalog"] = catalog
    if schema:
        body["schema"] = schema
    if parameters:
        body["parameters"] = [{"name": k, "value": v} for k, v in parameters.items()]

    started = time.monotonic()
    response = await client.post(BASE, body)
    statement_id = response.get("statement_id")
    while _state(response) not in _TERMINAL and statement_id:
        if time.monotonic() - started >= timeout_seconds:
            break
        await asyncio.sleep(poll_interval_seconds)
        response = await client.get(f"{BASE}/{statement_id}")
    return summarize(response)


async def get_statement(client: DatabricksClient, statement_id: str) -> dict[str, Any]:
    return summarize(await client.get(f"{BASE}/{statement_id}"))


async def cancel_statement(client: DatabricksClient, statement_id: str) -> dict[str, Any]:
    await client.post(f"{BASE}/{statement_id}/cancel")
    return {"statement_id": statement_id, "cancel_requested": True}


def _state(response: dict[str, Any]) -> str | None:
    return (response.get("status") or {}).get("state")


def summarize(response: dict[str, Any]) -> dict[str, Any]:
    """Flatten the API's manifest/result split into columns + rows the model can read."""
    status = response.get("status") or {}
    state = status.get("state")
    out: dict[str, Any] = {"statement_id": response.get("statement_id"), "state": state}
    if status.get("error"):
        out["error"] = status["error"]
    if state not in _TERMINAL:
        out["note"] = "Still running. Call get_sql_statement with this statement_id to check again."
        return out

    manifest = response.get("manifest") or {}
    columns = (manifest.get("schema") or {}).get("columns") or []
    result = response.get("result") or {}
    rows = result.get("data_array") or []
    out["columns"] = [{"name": c.get("name"), "type": c.get("type_text") or c.get("type_name")} for c in columns]
    out["rows"] = rows
    out["row_count"] = len(rows)
    total = manifest.get("total_row_count")
    if total is not None:
        out["total_row_count"] = total
    out["truncated"] = bool(
        manifest.get("truncated")
        or result.get("next_chunk_index") is not None
        or (total is not None and total > len(rows))
    )
    return out
