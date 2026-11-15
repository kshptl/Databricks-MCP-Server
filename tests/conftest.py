import pytest
import respx

from databricks_mcp.client import DatabricksClient

HOST = "https://test.cloud.databricks.com"


def fake_auth():
    return HOST, {"Authorization": "Bearer test-token"}


@pytest.fixture
def mock_api():
    with respx.mock(base_url=HOST, assert_all_called=False) as router:
        yield router


@pytest.fixture
async def client():
    c = DatabricksClient(auth=fake_auth, backoff_seconds=0, max_retries=2)
    yield c
    await c.aclose()
