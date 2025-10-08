# Copyright (c) 2025 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub, you may use it under the AGPL-3.0 terms.
# For commercial terms, contact kushapatel@live.com.

"""
Tests for the submit_single_run functionality.
"""

import pytest
from unittest.mock import AsyncMock, patch
from src.api import jobs


@pytest.mark.asyncio
async def test_submit_single_run_basic():
    """Test basic notebook run submission."""
    run_config = {
        "run_name": "Test Run",
        "tasks": [{
            "task_key": "test_task",
            "notebook_task": {
                "notebook_path": "/Users/test@example.com/TestNotebook",
                "base_parameters": {"param1": "value1"}
            },
            "existing_cluster_id": "test-cluster-123"
        }]
    }
    
    expected_response = {"run_id": 12345}
    
    with patch("src.api.jobs.make_api_request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = expected_response
        
        result = await jobs.submit_single_run(run_config)
        
        assert result == expected_response
        mock_request.assert_called_once_with(
            "POST",
            "/api/2.1/jobs/runs/submit",
            data=run_config
        )


@pytest.mark.asyncio
async def test_submit_single_run_with_new_cluster():
    """Test notebook run with new cluster configuration."""
    run_config = {
        "run_name": "Test Run with New Cluster",
        "tasks": [{
            "task_key": "test_task",
            "notebook_task": {
                "notebook_path": "/Users/test@example.com/TestNotebook"
            },
            "new_cluster": {
                "spark_version": "11.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            }
        }]
    }
    
    expected_response = {"run_id": 12346}
    
    with patch("src.api.jobs.make_api_request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = expected_response
        
        result = await jobs.submit_single_run(run_config)
        
        assert result == expected_response
        assert result["run_id"] == 12346


@pytest.mark.asyncio
async def test_submit_single_run_with_git_source():
    """Test notebook run from Git repository."""
    run_config = {
        "run_name": "Git-based Run",
        "git_source": {
            "git_url": "https://github.com/test/repo",
            "git_provider": "gitHub",
            "git_branch": "main"
        },
        "tasks": [{
            "task_key": "git_task",
            "notebook_task": {
                "notebook_path": "notebooks/TestNotebook"
            },
            "existing_cluster_id": "test-cluster-123"
        }]
    }
    
    expected_response = {"run_id": 12347}
    
    with patch("src.api.jobs.make_api_request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = expected_response
        
        result = await jobs.submit_single_run(run_config)
        
        assert result == expected_response
        assert "git_source" in run_config


@pytest.mark.asyncio
async def test_submit_single_run_with_libraries():
    """Test notebook run with library dependencies."""
    run_config = {
        "run_name": "Run with Libraries",
        "tasks": [{
            "task_key": "task_with_libs",
            "notebook_task": {
                "notebook_path": "/Users/test@example.com/TestNotebook"
            },
            "existing_cluster_id": "test-cluster-123",
            "libraries": [
                {"pypi": {"package": "pandas==1.5.0"}},
                {"maven": {"coordinates": "org.apache.spark:spark-sql_2.12:3.3.0"}}
            ]
        }]
    }
    
    expected_response = {"run_id": 12348}
    
    with patch("src.api.jobs.make_api_request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = expected_response
        
        result = await jobs.submit_single_run(run_config)
        
        assert result == expected_response


@pytest.mark.asyncio
async def test_get_run_output():
    """Test getting output from a completed run."""
    run_id = 12345
    expected_response = {
        "notebook_output": {
            "result": "Success",
            "truncated": False
        },
        "metadata": {
            "run_id": run_id,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS"
            }
        }
    }
    
    with patch("src.api.jobs.make_api_request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = expected_response
        
        result = await jobs.get_run_output(run_id)
        
        assert result == expected_response
        mock_request.assert_called_once_with(
            "GET",
            "/api/2.0/jobs/runs/get-output",
            params={"run_id": run_id}
        )


@pytest.mark.asyncio
async def test_wait_for_run_completion_success():
    """Test waiting for a run to complete successfully."""
    run_id = 12345
    
    # Mock responses: first two calls show running, third shows completed
    mock_responses = [
        {
            "run_id": run_id,
            "state": {
                "life_cycle_state": "RUNNING",
                "state_message": "In progress"
            }
        },
        {
            "run_id": run_id,
            "state": {
                "life_cycle_state": "RUNNING",
                "state_message": "In progress"
            }
        },
        {
            "run_id": run_id,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "Completed"
            }
        }
    ]
    
    with patch("src.api.jobs.get_run", new_callable=AsyncMock) as mock_get_run:
        mock_get_run.side_effect = mock_responses
        
        result = await jobs.wait_for_run_completion(
            run_id=run_id,
            poll_interval=1,
            max_wait_seconds=10
        )
        
        assert result["state"]["result_state"] == "SUCCESS"
        assert mock_get_run.call_count == 3


@pytest.mark.asyncio
async def test_wait_for_run_completion_timeout():
    """Test timeout when waiting for run completion."""
    run_id = 12345
    
    # Mock response that always shows running
    mock_response = {
        "run_id": run_id,
        "state": {
            "life_cycle_state": "RUNNING",
            "state_message": "In progress"
        }
    }
    
    with patch("src.api.jobs.get_run", new_callable=AsyncMock) as mock_get_run:
        mock_get_run.return_value = mock_response
        
        with pytest.raises(TimeoutError) as exc_info:
            await jobs.wait_for_run_completion(
                run_id=run_id,
                poll_interval=1,
                max_wait_seconds=3
            )
        
        assert "did not complete within 3 seconds" in str(exc_info.value)


@pytest.mark.asyncio
async def test_wait_for_run_completion_failure():
    """Test waiting for a run that fails."""
    run_id = 12345
    
    mock_response = {
        "run_id": run_id,
        "state": {
            "life_cycle_state": "TERMINATED",
            "result_state": "FAILED",
            "state_message": "Task failed with error"
        }
    }
    
    with patch("src.api.jobs.get_run", new_callable=AsyncMock) as mock_get_run:
        mock_get_run.return_value = mock_response
        
        result = await jobs.wait_for_run_completion(
            run_id=run_id,
            poll_interval=1,
            max_wait_seconds=10
        )
        
        assert result["state"]["result_state"] == "FAILED"
        assert result["state"]["life_cycle_state"] == "TERMINATED"


@pytest.mark.asyncio
async def test_submit_multi_task_workflow():
    """Test submitting a multi-task workflow."""
    run_config = {
        "run_name": "Multi-task Workflow",
        "tasks": [
            {
                "task_key": "task1",
                "notebook_task": {
                    "notebook_path": "/Workflows/Task1"
                },
                "existing_cluster_id": "cluster-123"
            },
            {
                "task_key": "task2",
                "depends_on": [{"task_key": "task1"}],
                "notebook_task": {
                    "notebook_path": "/Workflows/Task2"
                },
                "existing_cluster_id": "cluster-123"
            }
        ]
    }
    
    expected_response = {"run_id": 12349}
    
    with patch("src.api.jobs.make_api_request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = expected_response
        
        result = await jobs.submit_single_run(run_config)
        
        assert result == expected_response
        # Verify the multi-task structure was preserved
        submitted_config = mock_request.call_args[1]["data"]
        assert len(submitted_config["tasks"]) == 2
        assert submitted_config["tasks"][1]["depends_on"][0]["task_key"] == "task1"


@pytest.mark.asyncio
async def test_submit_with_access_control():
    """Test submitting a run with access control list."""
    run_config = {
        "run_name": "Run with ACL",
        "tasks": [{
            "task_key": "test_task",
            "notebook_task": {
                "notebook_path": "/Users/test@example.com/TestNotebook"
            },
            "existing_cluster_id": "test-cluster-123"
        }],
        "access_control_list": [
            {
                "group_name": "users",
                "permission_level": "CAN_VIEW"
            }
        ]
    }
    
    expected_response = {"run_id": 12350}
    
    with patch("src.api.jobs.make_api_request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = expected_response
        
        result = await jobs.submit_single_run(run_config)
        
        assert result == expected_response
        submitted_config = mock_request.call_args[1]["data"]
        assert "access_control_list" in submitted_config
