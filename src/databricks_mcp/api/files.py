# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""Files API 2.0 for Unity Catalog volumes (/Volumes/<catalog>/<schema>/<volume>/...).

This is what Databricks recommends over DBFS now. Files API 3.0 exists but is still in
beta, needs an admin toggle and a regional hostname, so I'm staying on 2.0.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from databricks_mcp.client import DatabricksClient

BASE = "/api/2.0/fs"


def _path(path: str) -> str:
    if not path.startswith("/Volumes/"):
        raise ValueError("Files API paths must be absolute volume paths, e.g. /Volumes/main/default/my_vol/a.csv")
    return quote(path, safe="/")


async def list_directory(
    client: DatabricksClient,
    path: str,
    page_size: int | None = None,
    page_token: str | None = None,
) -> dict[str, Any]:
    directory = _path(path.rstrip("/") + "/")
    return await client.get(f"{BASE}/directories{directory}", {"page_size": page_size, "page_token": page_token})


async def read_file(client: DatabricksClient, path: str, max_bytes: int = 100_000) -> dict[str, Any]:
    # Range keeps me from pulling a 5 GB file into memory just to show the first page.
    response = await client.request(
        "GET",
        f"{BASE}/files{_path(path)}",
        headers={"Range": f"bytes=0-{max_bytes - 1}"},
        raw=True,
    )
    data = response.content[:max_bytes]
    total = _total_size(response) or len(data)
    result: dict[str, Any] = {
        "path": path,
        "content_type": response.headers.get("content-type"),
        "bytes_returned": len(data),
        "total_bytes": total,
        "truncated": total > len(data),
    }
    try:
        result["content"] = data.decode("utf-8")
    except UnicodeDecodeError:
        result["content"] = None
        result["note"] = "Binary file; content not shown."
    return result


def _total_size(response: Any) -> int | None:
    content_range = response.headers.get("content-range")  # "bytes 0-99/12345"
    if content_range and "/" in content_range:
        tail = content_range.rsplit("/", 1)[1]
        if tail.isdigit():
            return int(tail)
    length = response.headers.get("content-length")
    if length and length.isdigit() and response.status_code == 200:
        return int(length)
    return None


async def write_file(client: DatabricksClient, path: str, content: str, overwrite: bool = False) -> dict[str, Any]:
    await client.request(
        "PUT",
        f"{BASE}/files{_path(path)}",
        params={"overwrite": "true" if overwrite else "false"},
        content=content.encode("utf-8"),
        headers={"Content-Type": "application/octet-stream"},
        raw=True,
    )
    return {"path": path, "written_bytes": len(content.encode("utf-8"))}


async def delete_file(client: DatabricksClient, path: str) -> dict[str, Any]:
    await client.request("DELETE", f"{BASE}/files{_path(path)}", raw=True)
    return {"path": path, "deleted": True}


async def create_directory(client: DatabricksClient, path: str) -> dict[str, Any]:
    await client.request("PUT", f"{BASE}/directories{_path(path.rstrip('/') + '/')}", raw=True)
    return {"path": path, "created": True}
