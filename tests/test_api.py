import base64
import json

import httpx
import pytest

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
from databricks_mcp.client import DatabricksAPIError


def body(route):
    return json.loads(route.calls.last.request.content)


# ------------------------------------------------------------------ clusters


async def test_clusters_use_api_2_1(client, mock_api):
    mock_api.get("/api/2.1/clusters/list").respond(json={"clusters": []})
    get = mock_api.get("/api/2.1/clusters/get").respond(json={"cluster_id": "c1"})
    term = mock_api.post("/api/2.1/clusters/delete").respond(json={})
    await clusters.list_clusters(client)
    await clusters.get_cluster(client, "c1")
    await clusters.terminate_cluster(client, "c1")
    assert get.calls.last.request.url.params["cluster_id"] == "c1"
    assert body(term) == {"cluster_id": "c1"}


async def test_resize_autoscale_and_validation(client, mock_api):
    route = mock_api.post("/api/2.1/clusters/resize").respond(json={})
    await clusters.resize_cluster(client, "c1", autoscale_min_workers=1, autoscale_max_workers=4)
    assert body(route) == {"cluster_id": "c1", "autoscale": {"min_workers": 1, "max_workers": 4}}
    await clusters.resize_cluster(client, "c1", num_workers=3)
    assert body(route) == {"cluster_id": "c1", "num_workers": 3}
    with pytest.raises(ValueError):
        await clusters.resize_cluster(client, "c1")
    with pytest.raises(ValueError):
        await clusters.resize_cluster(client, "c1", autoscale_min_workers=1)


# ------------------------------------------------------------------ jobs


async def test_jobs_use_api_2_2_and_clamp_limit(client, mock_api):
    route = mock_api.get("/api/2.2/jobs/list").respond(json={"jobs": []})
    await jobs.list_jobs(client, limit=500)
    assert route.calls.last.request.url.params["limit"] == "100"
    runs = mock_api.get("/api/2.2/jobs/runs/list").respond(json={"runs": []})
    await jobs.list_runs(client, job_id=7, active_only=True, limit=999)
    params = runs.calls.last.request.url.params
    assert params["limit"] == "25" and params["active_only"] == "true" and "completed_only" not in params


async def test_run_job_body(client, mock_api):
    route = mock_api.post("/api/2.2/jobs/run-now").respond(json={"run_id": 9})
    assert await jobs.run_job(client, 5, job_parameters={"env": "dev"}) == {"run_id": 9}
    assert body(route) == {"job_id": 5, "job_parameters": {"env": "dev"}}


async def test_submit_run_requires_tasks(client):
    with pytest.raises(ValueError, match="tasks"):
        await jobs.submit_run(client, {"run_name": "x"})


async def test_get_run_output_single_task(client, mock_api):
    mock_api.get("/api/2.2/jobs/runs/get").respond(json={"run_id": 1})
    out = mock_api.get("/api/2.2/jobs/runs/get-output").respond(json={"notebook_output": {"result": "42"}})
    assert await jobs.get_run_output(client, 1) == {"notebook_output": {"result": "42"}}
    assert out.calls.last.request.url.params["run_id"] == "1"


async def test_get_run_output_fans_out_over_tasks(client, mock_api):
    mock_api.get("/api/2.2/jobs/runs/get").respond(
        json={
            "run_id": 1,
            "status": {"state": "TERMINATED"},
            # "skipped" never got a run_id; it must not shift the pairing of the others
            "tasks": [{"task_key": "skipped"}, {"task_key": "a", "run_id": 11}, {"task_key": "b", "run_id": 12}],
        }
    )

    def output(request):
        run_id = request.url.params["run_id"]
        if run_id == "12":
            return httpx.Response(400, json={"message": "no output"})
        return httpx.Response(200, json={"notebook_output": {"result": "ok"}, "metadata": {"big": 1}})

    mock_api.get("/api/2.2/jobs/runs/get-output").mock(side_effect=output)
    result = await jobs.get_run_output(client, 1)
    assert result["tasks"][0] == {"task_key": "a", "run_id": 11, "notebook_output": {"result": "ok"}}
    assert result["tasks"][1]["task_key"] == "b"
    assert result["tasks"][1]["error"] == "HTTP 400: no output"


@pytest.mark.parametrize(
    "run,terminal",
    [
        ({"status": {"state": "RUNNING"}}, False),
        ({"status": {"state": "TERMINATED"}}, True),
        ({"state": {"life_cycle_state": "INTERNAL_ERROR"}}, True),
        ({"state": {"life_cycle_state": "PENDING"}}, False),
    ],
)
def test_is_terminal(run, terminal):
    assert jobs.is_terminal(run) is terminal


async def test_wait_for_run_polls_until_done(client, mock_api):
    route = mock_api.get("/api/2.2/jobs/runs/get").mock(
        side_effect=[
            httpx.Response(200, json={"status": {"state": "RUNNING"}}),
            httpx.Response(200, json={"status": {"state": "TERMINATED", "termination_details": {"code": "SUCCESS"}}}),
        ]
    )
    result = await jobs.wait_for_run(client, 3, poll_interval_seconds=0, timeout_seconds=10)
    assert result["finished"] is True and route.call_count == 2


async def test_wait_for_run_times_out(client, mock_api):
    mock_api.get("/api/2.2/jobs/runs/get").respond(json={"status": {"state": "RUNNING"}})
    result = await jobs.wait_for_run(client, 3, poll_interval_seconds=0, timeout_seconds=0)
    assert result["finished"] is False and "Still running" in result["note"]


# ------------------------------------------------------------------ workspace


async def test_export_notebook_decodes_text(client, mock_api):
    encoded = base64.b64encode(b"print('hi')").decode()
    mock_api.get("/api/2.0/workspace/export").respond(json={"content": encoded, "file_type": "py"})
    result = await workspace.export_notebook(client, "/Users/a/nb")
    assert result["content"] == "print('hi')" and result["encoding"] == "utf-8"


async def test_import_notebook_encodes_and_requires_language(client, mock_api):
    route = mock_api.post("/api/2.0/workspace/import").respond(json={})
    await workspace.import_notebook(client, "/Users/a/nb", "x = 1", language="PYTHON", overwrite=True)
    sent = body(route)
    assert base64.b64decode(sent["content"]) == b"x = 1"
    assert sent["language"] == "PYTHON" and sent["overwrite"] is True
    with pytest.raises(ValueError, match="language"):
        await workspace.import_notebook(client, "/Users/a/nb", "x = 1")


# ------------------------------------------------------------------ files (volumes)


async def test_files_reject_non_volume_paths(client):
    with pytest.raises(ValueError, match="/Volumes/"):
        await files.list_directory(client, "/tmp/x")


async def test_list_directory_quotes_path(client, mock_api):
    route = mock_api.get(url__regex=r"/api/2\.0/fs/directories/.*").respond(json={"contents": []})
    await files.list_directory(client, "/Volumes/main/default/raw data")
    assert route.calls.last.request.url.raw_path.startswith(b"/api/2.0/fs/directories/Volumes/main/default/raw%20data/")


async def test_read_file_uses_range_and_reports_truncation(client, mock_api):
    route = mock_api.get("/api/2.0/fs/files/Volumes/m/d/v/a.csv").respond(
        206, content=b"a,b\n1,2\n", headers={"content-range": "bytes 0-7/5000", "content-type": "text/csv"}
    )
    result = await files.read_file(client, "/Volumes/m/d/v/a.csv", max_bytes=8)
    assert route.calls.last.request.headers["Range"] == "bytes=0-7"
    assert result["content"] == "a,b\n1,2\n" and result["truncated"] is True and result["total_bytes"] == 5000


async def test_read_binary_file(client, mock_api):
    mock_api.get("/api/2.0/fs/files/Volumes/m/d/v/img.png").respond(200, content=b"\x89PNG\xff\xfe")
    result = await files.read_file(client, "/Volumes/m/d/v/img.png")
    assert result["content"] is None and result["truncated"] is False


async def test_write_file_sends_raw_bytes(client, mock_api):
    route = mock_api.put("/api/2.0/fs/files/Volumes/m/d/v/out.txt").respond(204)
    result = await files.write_file(client, "/Volumes/m/d/v/out.txt", "hello", overwrite=True)
    request = route.calls.last.request
    assert request.content == b"hello" and request.url.params["overwrite"] == "true"
    assert result == {"path": "/Volumes/m/d/v/out.txt", "written_bytes": 5}


# ------------------------------------------------------------------ dbfs


async def test_dbfs_read(client, mock_api):
    mock_api.get("/api/2.0/dbfs/read").respond(json={"bytes_read": 3, "data": base64.b64encode(b"abc").decode()})
    mock_api.get("/api/2.0/dbfs/get-status").respond(json={"file_size": 10})
    result = await dbfs.read_file(client, "/tmp/x.txt", max_bytes=3)
    assert result["content"] == "abc" and result["truncated"] is True


# ------------------------------------------------------------------ sql


async def test_execute_sql_polls_and_summarizes(client, mock_api):
    post = mock_api.post("/api/2.0/sql/statements").respond(json={"statement_id": "s1", "status": {"state": "RUNNING"}})
    mock_api.get("/api/2.0/sql/statements/s1").respond(
        json={
            "statement_id": "s1",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "schema": {"columns": [{"name": "n", "type_name": "INT"}]},
                "total_row_count": 2,
            },
            "result": {"data_array": [["1"], ["2"]]},
        }
    )
    result = await sql.execute(client, "SELECT :n", "wh1", parameters={"n": "1"}, poll_interval_seconds=0)
    sent = body(post)
    assert sent["warehouse_id"] == "wh1" and sent["on_wait_timeout"] == "CONTINUE"
    assert sent["parameters"] == [{"name": "n", "value": "1"}]
    assert result == {
        "statement_id": "s1",
        "state": "SUCCEEDED",
        "columns": [{"name": "n", "type": "INT"}],
        "rows": [["1"], ["2"]],
        "row_count": 2,
        "total_row_count": 2,
        "truncated": False,
    }


async def test_execute_sql_caps_server_wait_at_50s(client, mock_api):
    post = mock_api.post("/api/2.0/sql/statements").respond(
        json={"statement_id": "s1", "status": {"state": "SUCCEEDED"}}
    )
    await sql.execute(client, "SELECT 1", "wh1", timeout_seconds=600)
    assert body(post)["wait_timeout"] == "50s"


async def test_execute_sql_returns_id_when_still_running(client, mock_api):
    mock_api.post("/api/2.0/sql/statements").respond(json={"statement_id": "s1", "status": {"state": "PENDING"}})
    result = await sql.execute(client, "SELECT 1", "wh1", timeout_seconds=0)
    assert result["state"] == "PENDING" and "get_sql_statement" in result["note"]


def test_summarize_failed_and_truncated():
    failed = sql.summarize({"statement_id": "s", "status": {"state": "FAILED", "error": {"message": "bad"}}})
    assert failed["error"] == {"message": "bad"}
    big = sql.summarize(
        {
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": []}, "truncated": True},
            "result": {"data_array": [[1]]},
        }
    )
    assert big["truncated"] is True


# ------------------------------------------------------------------ warehouses + unity catalog


async def test_warehouse_start(client, mock_api):
    route = mock_api.post("/api/2.0/sql/warehouses/wh1/start").respond(json={})
    assert await warehouses.start_warehouse(client, "wh1") == {"warehouse_id": "wh1", "start_requested": True}
    assert route.called


async def test_unity_catalog_paths(client, mock_api):
    tables = mock_api.get("/api/2.1/unity-catalog/tables").respond(json={"tables": []})
    one = mock_api.get("/api/2.1/unity-catalog/tables/main.default.t1").respond(json={"name": "t1"})
    await unity_catalog.list_tables(client, "main", "default")
    params = tables.calls.last.request.url.params
    assert params["catalog_name"] == "main" and params["omit_columns"] == "true"
    assert await unity_catalog.get_table(client, "main.default.t1") == {"name": "t1"}
    assert one.called


# ------------------------------------------------------------------ command execution


async def test_run_once_cleans_up_context(client, mock_api):
    mock_api.post("/api/1.2/contexts/create").respond(json={"id": "ctx"})
    mock_api.post("/api/1.2/commands/execute").respond(json={"id": "cmd"})
    mock_api.get("/api/1.2/commands/status").mock(
        side_effect=[
            httpx.Response(200, json={"status": "Running"}),
            httpx.Response(200, json={"status": "Finished", "results": {"resultType": "text", "data": "2"}}),
        ]
    )
    destroy = mock_api.post("/api/1.2/contexts/destroy").respond(json={})
    result = await command_execution.run_once(client, "c1", "1+1", "python", poll_interval_seconds=0)
    assert result["results"]["data"] == "2"
    assert body(destroy) == {"clusterId": "c1", "contextId": "ctx"}


async def test_run_once_cleans_up_on_failure(client, mock_api):
    mock_api.post("/api/1.2/contexts/create").respond(json={"id": "ctx"})
    mock_api.post("/api/1.2/commands/execute").respond(400, json={"message": "cluster not running"})
    destroy = mock_api.post("/api/1.2/contexts/destroy").respond(json={})
    with pytest.raises(DatabricksAPIError, match="cluster not running"):
        await command_execution.run_once(client, "c1", "1+1", "python")
    assert destroy.called
