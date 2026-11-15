# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""Server settings.

Databricks auth is NOT configured here. I hand that to databricks-sdk's unified auth,
so anything the Databricks CLI accepts works: DATABRICKS_HOST + DATABRICKS_TOKEN,
OAuth M2M (DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET), a ~/.databrickscfg
profile via DATABRICKS_CONFIG_PROFILE, Azure CLI, and so on.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

_TRUE = {"1", "true", "yes", "on"}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in _TRUE


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    warehouse_id: str | None = None
    read_only: bool = False
    # Hard cap on what a single tool returns to the model. Big JSON blobs eat context fast.
    max_response_chars: int = 50_000
    http_timeout_seconds: int = 60
    max_retries: int = 3

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID") or None,
            read_only=_env_bool("DATABRICKS_MCP_READ_ONLY"),
            max_response_chars=_env_int("DATABRICKS_MCP_MAX_RESPONSE_CHARS", 50_000),
            http_timeout_seconds=_env_int("DATABRICKS_MCP_HTTP_TIMEOUT", 60),
            max_retries=_env_int("DATABRICKS_MCP_MAX_RETRIES", 3),
        )
