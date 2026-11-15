# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""DBFS API 2.0. Legacy: Databricks recommends Unity Catalog volumes (see files.py).
I only keep read access here for workspaces that still have data in DBFS."""

from __future__ import annotations

import base64
from typing import Any

from databricks_mcp.client import DatabricksClient

BASE = "/api/2.0/dbfs"
_MAX_READ = 1_048_576  # the API's own per-call ceiling


async def list_files(client: DatabricksClient, path: str) -> dict[str, Any]:
    return await client.get(f"{BASE}/list", {"path": path})


async def read_file(client: DatabricksClient, path: str, max_bytes: int = 100_000) -> dict[str, Any]:
    length = max(1, min(max_bytes, _MAX_READ))
    result = await client.get(f"{BASE}/read", {"path": path, "offset": 0, "length": length})
    raw = base64.b64decode(result.get("data") or "")
    out: dict[str, Any] = {"path": path, "bytes_read": result.get("bytes_read", len(raw))}
    try:
        out["content"] = raw.decode("utf-8")
    except UnicodeDecodeError:
        out["content"] = None
        out["note"] = "Binary file; content not shown."
    status = await client.get(f"{BASE}/get-status", {"path": path})
    out["total_bytes"] = status.get("file_size")
    out["truncated"] = (status.get("file_size") or 0) > len(raw)
    return out
