import httpx
import pytest

from databricks_mcp import __version__
from databricks_mcp.client import DatabricksAPIError

from .conftest import HOST


async def test_sends_auth_and_user_agent_and_drops_none_params(client, mock_api):
    route = mock_api.get("/api/2.1/clusters/list").respond(json={"clusters": []})
    assert await client.get("/api/2.1/clusters/list", {"page_size": 10, "page_token": None}) == {"clusters": []}
    request = route.calls.last.request
    assert request.headers["Authorization"] == "Bearer test-token"
    assert request.headers["User-Agent"] == f"databricks-mcp-toolkit/{__version__}"
    assert dict(request.url.params) == {"page_size": "10"}
    assert str(request.url).startswith(HOST)


async def test_empty_body_is_empty_dict(client, mock_api):
    mock_api.post("/api/2.1/clusters/start").respond(200)
    assert await client.post("/api/2.1/clusters/start", {"cluster_id": "x"}) == {}


async def test_error_message_and_code_surface(client, mock_api):
    mock_api.get("/api/2.1/clusters/get").respond(
        400, json={"error_code": "INVALID_PARAMETER_VALUE", "message": "Cluster abc does not exist"}
    )
    with pytest.raises(DatabricksAPIError) as info:
        await client.get("/api/2.1/clusters/get", {"cluster_id": "abc"})
    assert info.value.status_code == 400
    assert info.value.error_code == "INVALID_PARAMETER_VALUE"
    assert str(info.value) == "HTTP 400 INVALID_PARAMETER_VALUE: Cluster abc does not exist"


async def test_non_json_error_body(client, mock_api):
    mock_api.get("/x").respond(502, text="bad gateway html")
    with pytest.raises(DatabricksAPIError, match="bad gateway html"):
        await client.get("/x")


async def test_get_retries_5xx_then_succeeds(client, mock_api):
    route = mock_api.get("/x").mock(
        side_effect=[httpx.Response(503), httpx.Response(503), httpx.Response(200, json={"ok": 1})]
    )
    assert await client.get("/x") == {"ok": 1}
    assert route.call_count == 3


async def test_post_is_not_retried_on_5xx(client, mock_api):
    # A retried create could duplicate the resource.
    route = mock_api.post("/api/2.1/clusters/create").respond(500, json={"message": "boom"})
    with pytest.raises(DatabricksAPIError):
        await client.post("/api/2.1/clusters/create", {"cluster_name": "a"})
    assert route.call_count == 1


async def test_429_is_retried_even_for_post(client, mock_api):
    route = mock_api.post("/x").mock(
        side_effect=[httpx.Response(429, headers={"Retry-After": "0"}), httpx.Response(200, json={"id": 1})]
    )
    assert await client.post("/x") == {"id": 1}
    assert route.call_count == 2


async def test_gives_up_after_max_retries(client, mock_api):
    route = mock_api.get("/x").respond(429)
    with pytest.raises(DatabricksAPIError) as info:
        await client.get("/x")
    assert info.value.status_code == 429
    assert route.call_count == 3  # first try + max_retries=2


async def test_connect_error_retried_then_wrapped(client, mock_api):
    route = mock_api.post("/x").mock(side_effect=httpx.ConnectError("refused"))
    with pytest.raises(DatabricksAPIError, match="Could not connect"):
        await client.post("/x")
    assert route.call_count == 3


async def test_read_timeout_not_retried_for_post(client, mock_api):
    route = mock_api.post("/x").mock(side_effect=httpx.ReadTimeout("slow"))
    with pytest.raises(DatabricksAPIError):
        await client.post("/x")
    assert route.call_count == 1


async def test_raw_returns_response(client, mock_api):
    mock_api.get("/raw").respond(200, content=b"bytes")
    response = await client.request("GET", "/raw", raw=True)
    assert response.content == b"bytes"


async def test_missing_sdk_auth_gives_clear_error(monkeypatch):
    from databricks_mcp.client import DatabricksClient

    for var in (
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_CONFIG_PROFILE",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", "/nonexistent/.databrickscfg")
    c = DatabricksClient()
    with pytest.raises(DatabricksAPIError, match="auth is not configured"):
        await c.get("/api/2.1/clusters/list")
