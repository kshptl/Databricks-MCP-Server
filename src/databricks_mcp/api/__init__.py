# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""Thin wrappers over Databricks REST endpoints. Each function takes a DatabricksClient."""

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

__all__ = [
    "clusters",
    "command_execution",
    "dbfs",
    "files",
    "jobs",
    "sql",
    "unity_catalog",
    "warehouses",
    "workspace",
]
