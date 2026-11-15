# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""Clusters API 2.1."""

from __future__ import annotations

from typing import Any

from databricks_mcp.client import DatabricksClient

BASE = "/api/2.1/clusters"


async def list_clusters(
    client: DatabricksClient,
    page_size: int | None = None,
    page_token: str | None = None,
) -> dict[str, Any]:
    return await client.get(f"{BASE}/list", {"page_size": page_size, "page_token": page_token})


async def get_cluster(client: DatabricksClient, cluster_id: str) -> dict[str, Any]:
    return await client.get(f"{BASE}/get", {"cluster_id": cluster_id})


async def create_cluster(client: DatabricksClient, spec: dict[str, Any]) -> dict[str, Any]:
    return await client.post(f"{BASE}/create", spec)


async def start_cluster(client: DatabricksClient, cluster_id: str) -> dict[str, Any]:
    return await client.post(f"{BASE}/start", {"cluster_id": cluster_id})


async def restart_cluster(client: DatabricksClient, cluster_id: str) -> dict[str, Any]:
    return await client.post(f"{BASE}/restart", {"cluster_id": cluster_id})


async def resize_cluster(
    client: DatabricksClient,
    cluster_id: str,
    num_workers: int | None = None,
    autoscale_min_workers: int | None = None,
    autoscale_max_workers: int | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"cluster_id": cluster_id}
    if autoscale_min_workers is not None or autoscale_max_workers is not None:
        if autoscale_min_workers is None or autoscale_max_workers is None:
            raise ValueError("Set both autoscale_min_workers and autoscale_max_workers.")
        body["autoscale"] = {
            "min_workers": autoscale_min_workers,
            "max_workers": autoscale_max_workers,
        }
    elif num_workers is not None:
        body["num_workers"] = num_workers
    else:
        raise ValueError("Pass num_workers, or both autoscale_min_workers and autoscale_max_workers.")
    return await client.post(f"{BASE}/resize", body)


async def terminate_cluster(client: DatabricksClient, cluster_id: str) -> dict[str, Any]:
    # "delete" in the API terminates; the config is kept and the cluster can be restarted.
    return await client.post(f"{BASE}/delete", {"cluster_id": cluster_id})


async def list_spark_versions(client: DatabricksClient) -> dict[str, Any]:
    return await client.get(f"{BASE}/spark-versions")


async def list_node_types(client: DatabricksClient) -> dict[str, Any]:
    return await client.get(f"{BASE}/list-node-types")
