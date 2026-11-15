# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""SQL Warehouses API 2.0."""

from __future__ import annotations

from typing import Any

from databricks_mcp.client import DatabricksClient

BASE = "/api/2.0/sql/warehouses"


async def list_warehouses(client: DatabricksClient) -> dict[str, Any]:
    return await client.get(BASE)


async def get_warehouse(client: DatabricksClient, warehouse_id: str) -> dict[str, Any]:
    return await client.get(f"{BASE}/{warehouse_id}")


async def start_warehouse(client: DatabricksClient, warehouse_id: str) -> dict[str, Any]:
    await client.post(f"{BASE}/{warehouse_id}/start")
    return {"warehouse_id": warehouse_id, "start_requested": True}


async def stop_warehouse(client: DatabricksClient, warehouse_id: str) -> dict[str, Any]:
    await client.post(f"{BASE}/{warehouse_id}/stop")
    return {"warehouse_id": warehouse_id, "stop_requested": True}
