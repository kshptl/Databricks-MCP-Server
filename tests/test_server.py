import json

import pytest
from mcp import Client

from databricks_mcp.cli import main as cli_main
from databricks_mcp.config import Settings
from databricks_mcp.server import TOOLSETS, build_server


def text(result) -> str:
    return result.content[0].text


async def tool_names(server) -> set[str]:
    return {t.name for t in await server.list_tools()}


async def test_every_tool_has_description_and_annotations(client):
    tools = await build_server(client, Settings()).list_tools()
    assert len(tools) == 48
    for tool in tools:
        assert tool.description, tool.name
        assert tool.annotations is not None and tool.annotations.read_only_hint is not None, tool.name


async def test_read_only_mode_drops_every_write_tool(client):
    full = await build_server(client, Settings()).list_tools()
    read_only = await build_server(client, Settings(read_only=True)).list_tools()
    expected = {t.name for t in full if t.annotations.read_only_hint}
    assert {t.name for t in read_only} == expected
    for name in ("execute_sql", "run_command", "create_cluster", "write_volume_file", "run_job"):
        assert name not in expected


async def test_toolsets_filter(client):
    names = await tool_names(build_server(client, Settings(), toolsets=["unity_catalog"]))
    assert names == {"list_catalogs", "list_schemas", "list_tables", "get_table", "list_volumes"}
    with pytest.raises(ValueError, match="Unknown toolset"):
        build_server(client, Settings(), toolsets=["bogus"])
    assert len(TOOLSETS) == 8


async def test_list_clusters_end_to_end_slims_output(client, mock_api):
    mock_api.get("/api/2.1/clusters/list").respond(
        json={"clusters": [{"cluster_id": "c1", "cluster_name": "a", "state": "RUNNING", "spark_env_vars": {"X": "1"}}]}
    )
    async with Client(build_server(client, Settings())) as mcp:
        result = await mcp.call_tool("list_clusters", {})
    assert not result.is_error
    assert json.loads(text(result)) == {"clusters": [{"cluster_id": "c1", "cluster_name": "a", "state": "RUNNING"}]}


async def test_api_errors_reach_the_model(client, mock_api):
    mock_api.get("/api/2.1/clusters/get").respond(
        400, json={"error_code": "INVALID_PARAMETER_VALUE", "message": "Cluster nope does not exist"}
    )
    async with Client(build_server(client, Settings())) as mcp:
        result = await mcp.call_tool("get_cluster", {"cluster_id": "nope"})
    assert result.is_error
    assert "Cluster nope does not exist" in text(result)


async def test_execute_sql_without_warehouse_is_explained(client):
    async with Client(build_server(client, Settings(warehouse_id=None))) as mcp:
        result = await mcp.call_tool("execute_sql", {"statement": "SELECT 1"})
    assert result.is_error and "list_sql_warehouses" in text(result)


async def test_execute_sql_uses_default_warehouse(client, mock_api):
    route = mock_api.post("/api/2.0/sql/statements").respond(
        json={"statement_id": "s", "status": {"state": "SUCCEEDED"}, "result": {"data_array": [["1"]]}}
    )
    async with Client(build_server(client, Settings(warehouse_id="wh-default"))) as mcp:
        result = await mcp.call_tool("execute_sql", {"statement": "SELECT 1"})
    assert json.loads(route.calls.last.request.content)["warehouse_id"] == "wh-default"
    assert json.loads(text(result))["rows"] == [["1"]]


async def test_bad_arguments_are_rejected_by_schema(client):
    async with Client(build_server(client, Settings())) as mcp:
        result = await mcp.call_tool("list_jobs", {"limit": 1000})
    assert result.is_error


async def test_large_responses_are_truncated(client, mock_api):
    mock_api.get("/api/2.1/clusters/get").respond(json={"blob": "x" * 5000})
    async with Client(build_server(client, Settings(max_response_chars=500))) as mcp:
        result = await mcp.call_tool("get_cluster", {"cluster_id": "c1"})
    assert len(text(result)) < 700 and "[truncated" in text(result)


async def test_import_notebook_uppercases_language(client, mock_api):
    route = mock_api.post("/api/2.0/workspace/import").respond(json={})
    async with Client(build_server(client, Settings())) as mcp:
        await mcp.call_tool("import_notebook", {"path": "/Shared/nb", "content": "1", "language": "python"})
    assert json.loads(route.calls.last.request.content)["language"] == "PYTHON"


def test_cli_list_tools(capsys):
    assert cli_main(["list-tools", "--read-only", "--toolsets", "sql"]) == 0
    out = capsys.readouterr().out
    assert "list_sql_warehouses [read-only]" in out and "execute_sql" not in out


def test_cli_rejects_unknown_toolset(capsys):
    assert cli_main(["list-tools", "--toolsets", "nope"]) == 2
    assert "Unknown toolset" in capsys.readouterr().err


def test_readme_tool_tables_are_current():
    import importlib.util
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("gen_tools_table", root / "scripts" / "gen_tools_table.py")
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    assert gen.render() in (root / "README.md").read_text(), "Run: python scripts/gen_tools_table.py"


def test_cli_defaults_to_run(monkeypatch):
    import databricks_mcp.cli as cli

    seen = {}

    class FakeServer:
        def run(self, transport, **kwargs):
            seen["transport"] = transport

    def fake_build(settings, toolsets, log_level):
        seen.update(read_only=settings.read_only, toolsets=toolsets)
        return FakeServer()

    monkeypatch.setattr(cli, "build_server", fake_build)
    assert cli.main(["--read-only", "--toolsets", "sql,unity_catalog"]) == 0
    assert seen == {"read_only": True, "toolsets": ["sql", "unity_catalog"], "transport": "stdio"}
