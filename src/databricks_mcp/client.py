# Copyright (c) 2025-2026 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub or PyPI, you may use it under the AGPL-3.0 terms.
# For commercial terms, see the project README.

"""Async HTTP client for the Databricks REST API."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from typing import Any

import httpx

from databricks_mcp import __version__

logger = logging.getLogger(__name__)

# Returns (workspace host, auth headers). Called per request so OAuth tokens refresh.
AuthProvider = Callable[[], tuple[str, dict[str, str]]]

# Safe to resend after a 5xx or timeout. POSTs are not: a retried clusters/create
# can leave you with two clusters.
_IDEMPOTENT = {"GET", "HEAD", "PUT", "DELETE"}
_MAX_BACKOFF_SECONDS = 30.0
_USER_AGENT = f"databricks-mcp-toolkit/{__version__}"


class DatabricksAPIError(Exception):
    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        error_code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code

    def __str__(self) -> str:
        parts = []
        if self.status_code:
            parts.append(f"HTTP {self.status_code}")
        if self.error_code:
            parts.append(self.error_code)
        prefix = " ".join(parts)
        return f"{prefix}: {self.message}" if prefix else self.message


def sdk_auth_provider() -> AuthProvider:
    """Auth via databricks-sdk, so every auth type the SDK supports works here too.

    I build the Config lazily: `databricks-mcp list-tools` shouldn't need credentials.
    """
    config = None

    def provide() -> tuple[str, dict[str, str]]:
        nonlocal config
        if config is None:
            from databricks.sdk.config import Config

            try:
                config = Config()
            except Exception as exc:  # the SDK raises ValueError with a decent message
                raise DatabricksAPIError(
                    "Databricks auth is not configured. Set DATABRICKS_HOST plus "
                    "DATABRICKS_TOKEN (or OAuth / a config profile). "
                    f"SDK said: {exc}"
                ) from exc
        return config.host.rstrip("/"), config.authenticate()

    return provide


def _clean_params(params: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not params:
        return None
    return {k: v for k, v in params.items() if v is not None}


def _error_from_response(response: httpx.Response) -> DatabricksAPIError:
    message = ""
    error_code = None
    try:
        body = response.json()
        if isinstance(body, dict):
            error_code = body.get("error_code")
            message = body.get("message") or body.get("error") or ""
    except ValueError:
        pass
    if not message:
        message = response.text[:500] or response.reason_phrase
    return DatabricksAPIError(message, status_code=response.status_code, error_code=error_code)


class DatabricksClient:
    def __init__(
        self,
        auth: AuthProvider | None = None,
        *,
        timeout_seconds: float = 60.0,
        max_retries: int = 3,
        backoff_seconds: float = 1.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._auth = auth or sdk_auth_provider()
        self._timeout = httpx.Timeout(timeout_seconds, connect=10.0)
        self._max_retries = max_retries
        self._backoff = backoff_seconds
        self._transport = transport
        self._http: httpx.AsyncClient | None = None

    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                timeout=self._timeout,
                transport=self._transport,
                headers={"User-Agent": _USER_AGENT},
            )
        return self._http

    async def aclose(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    def _delay(self, attempt: int, response: httpx.Response | None = None) -> float:
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    return min(float(retry_after), _MAX_BACKOFF_SECONDS)
                except ValueError:
                    pass
        return min(self._backoff * (2**attempt), _MAX_BACKOFF_SECONDS)

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
        content: bytes | None = None,
        headers: Mapping[str, str] | None = None,
        raw: bool = False,
    ) -> Any:
        """Send a request. Returns parsed JSON ({} for an empty body), or the
        httpx.Response itself when raw=True (used by the Files API)."""
        method = method.upper()
        host, auth_headers = self._auth()
        url = f"{host}{path if path.startswith('/') else '/' + path}"
        merged_headers = {**auth_headers, **(headers or {})}
        clean = _clean_params(params)

        attempt = 0
        while True:
            try:
                response = await self._client().request(
                    method,
                    url,
                    params=clean,
                    json=json,
                    content=content,
                    headers=merged_headers,
                )
            except httpx.ConnectError as exc:
                # Never reached the server, so any method is safe to resend.
                if attempt < self._max_retries:
                    await asyncio.sleep(self._delay(attempt))
                    attempt += 1
                    continue
                raise DatabricksAPIError(f"Could not connect to {host}: {exc}") from exc
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                if method in _IDEMPOTENT and attempt < self._max_retries:
                    await asyncio.sleep(self._delay(attempt))
                    attempt += 1
                    continue
                raise DatabricksAPIError(f"{method} {path} failed: {exc!r}") from exc

            status = response.status_code
            retryable = status == 429 or (status >= 500 and method in _IDEMPOTENT)
            if retryable and attempt < self._max_retries:
                delay = self._delay(attempt, response)
                logger.warning("%s %s -> %s, retrying in %.1fs", method, path, status, delay)
                await asyncio.sleep(delay)
                attempt += 1
                continue

            if status >= 400:
                raise _error_from_response(response)
            if raw:
                return response
            if not response.content:
                return {}
            try:
                return response.json()
            except ValueError:
                return {"raw": response.text}

    async def get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        return await self.request("GET", path, params=params)

    async def post(self, path: str, json: Any = None) -> Any:
        return await self.request("POST", path, json=json if json is not None else {})
