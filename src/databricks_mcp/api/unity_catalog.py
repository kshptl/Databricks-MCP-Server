# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""Unity Catalog API 2.1 (read-only browsing: catalogs, schemas, tables, volumes)."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from databricks_mcp.client import DatabricksClient

BASE = "/api/2.1/unity-catalog"


async def list_catalogs(
    client: DatabricksClient, max_results: int = 100, page_token: str | None = None
) -> dict[str, Any]:
    return await client.get(f"{BASE}/catalogs", {"max_results": max_results, "page_token": page_token})


async def list_schemas(
    client: DatabricksClient,
    catalog_name: str,
    max_results: int = 100,
    page_token: str | None = None,
) -> dict[str, Any]:
    return await client.get(
        f"{BASE}/schemas",
        {"catalog_name": catalog_name, "max_results": max_results, "page_token": page_token},
    )


async def list_tables(
    client: DatabricksClient,
    catalog_name: str,
    schema_name: str,
    max_results: int = 100,
    page_token: str | None = None,
) -> dict[str, Any]:
    # omit_columns keeps the listing small; get_table has the full column list.
    return await client.get(
        f"{BASE}/tables",
        {
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "max_results": max_results,
            "page_token": page_token,
            "omit_columns": True,
            "omit_properties": True,
        },
    )


async def get_table(client: DatabricksClient, full_name: str) -> dict[str, Any]:
    return await client.get(f"{BASE}/tables/{quote(full_name, safe='')}")


async def list_volumes(
    client: DatabricksClient,
    catalog_name: str,
    schema_name: str,
    max_results: int = 100,
    page_token: str | None = None,
) -> dict[str, Any]:
    return await client.get(
        f"{BASE}/volumes",
        {
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "max_results": max_results,
            "page_token": page_token,
        },
    )
