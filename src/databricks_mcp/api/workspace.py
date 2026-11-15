# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""Workspace API 2.0 (notebooks, folders, workspace files)."""

from __future__ import annotations

import base64
from typing import Any

from databricks_mcp.client import DatabricksClient

BASE = "/api/2.0/workspace"


async def list_objects(client: DatabricksClient, path: str) -> dict[str, Any]:
    return await client.get(f"{BASE}/list", {"path": path})


async def get_status(client: DatabricksClient, path: str) -> dict[str, Any]:
    return await client.get(f"{BASE}/get-status", {"path": path})


async def export_notebook(client: DatabricksClient, path: str, format: str = "SOURCE") -> dict[str, Any]:
    """Export and decode. SOURCE/JUPYTER/HTML come back as text; DBC stays base64."""
    result = await client.get(f"{BASE}/export", {"path": path, "format": format})
    encoded = result.get("content")
    if encoded and format != "DBC":
        try:
            result["content"] = base64.b64decode(encoded).decode("utf-8")
            result["encoding"] = "utf-8"
        except (ValueError, UnicodeDecodeError):
            result["encoding"] = "base64"
    elif encoded:
        result["encoding"] = "base64"
    return result


async def import_notebook(
    client: DatabricksClient,
    path: str,
    content: str,
    language: str | None = None,
    format: str = "SOURCE",
    overwrite: bool = False,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "path": path,
        "format": format,
        "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        "overwrite": overwrite,
    }
    if language:
        body["language"] = language
    elif format == "SOURCE":
        raise ValueError("language is required when format is SOURCE.")
    return await client.post(f"{BASE}/import", body)


async def mkdirs(client: DatabricksClient, path: str) -> dict[str, Any]:
    return await client.post(f"{BASE}/mkdirs", {"path": path})


async def delete(client: DatabricksClient, path: str, recursive: bool = False) -> dict[str, Any]:
    return await client.post(f"{BASE}/delete", {"path": path, "recursive": recursive})
