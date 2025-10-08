# Copyright (c) 2025 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub, you may use it under the AGPL-3.0 terms.
# For commercial terms, contact kushapatel@live.com.

"""
Unit tests for command execution API module.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from src.api import command_execution
from src.core.utils import DatabricksAPIError


@pytest.fixture
def mock_make_api_request():
    """Fixture to mock make_api_request."""
    with patch("src.api.command_execution.make_api_request") as mock:
        yield mock


@pytest.mark.asyncio
async def test_create_execution_context(mock_make_api_request):
    """Test creating an execution context."""
    # Arrange
    cluster_id = "test-cluster-123"
    language = "python"
    expected_response = {"id": "context-123"}
    mock_make_api_request.return_value = expected_response
    
    # Act
    result = await command_execution.create_execution_context(cluster_id, language)
    
    # Assert
    assert result == expected_response
    mock_make_api_request.assert_called_once_with(
        "POST",
        "/api/1.2/contexts/create",
        data={"clusterId": cluster_id, "language": language}
    )


@pytest.mark.asyncio
async def test_execute_command(mock_make_api_request):
    """Test executing a command."""
    # Arrange
    cluster_id = "test-cluster-123"
    context_id = "context-123"
    command = "print('Hello, World!')"
    language = "python"
    expected_response = {"id": "command-456"}
    mock_make_api_request.return_value = expected_response
    
    # Act
    result = await command_execution.execute_command(
        cluster_id, context_id, command, language
    )
    
    # Assert
    assert result == expected_response
    mock_make_api_request.assert_called_once_with(
        "POST",
        "/api/1.2/commands/execute",
        data={
            "clusterId": cluster_id,
            "contextId": context_id,
            "command": command,
            "language": language
        }
    )


@pytest.mark.asyncio
async def test_get_command_status(mock_make_api_request):
    """Test getting command status."""
    # Arrange
    cluster_id = "test-cluster-123"
    context_id = "context-123"
    command_id = "command-456"
    expected_response = {
        "id": command_id,
        "status": "Finished",
        "results": {"resultType": "text", "data": "Hello, World!"}
    }
    mock_make_api_request.return_value = expected_response
    
    # Act
    result = await command_execution.get_command_status(
        cluster_id, context_id, command_id
    )
    
    # Assert
    assert result == expected_response
    mock_make_api_request.assert_called_once_with(
        "GET",
        "/api/1.2/commands/status",
        params={
            "clusterId": cluster_id,
            "contextId": context_id,
            "commandId": command_id
        }
    )


@pytest.mark.asyncio
async def test_cancel_command(mock_make_api_request):
    """Test cancelling a command."""
    # Arrange
    cluster_id = "test-cluster-123"
    context_id = "context-123"
    command_id = "command-456"
    expected_response = {}
    mock_make_api_request.return_value = expected_response
    
    # Act
    result = await command_execution.cancel_command(
        cluster_id, context_id, command_id
    )
    
    # Assert
    assert result == expected_response
    mock_make_api_request.assert_called_once_with(
        "POST",
        "/api/1.2/commands/cancel",
        data={
            "clusterId": cluster_id,
            "contextId": context_id,
            "commandId": command_id
        }
    )


@pytest.mark.asyncio
async def test_destroy_execution_context(mock_make_api_request):
    """Test destroying an execution context."""
    # Arrange
    cluster_id = "test-cluster-123"
    context_id = "context-123"
    expected_response = {}
    mock_make_api_request.return_value = expected_response
    
    # Act
    result = await command_execution.destroy_execution_context(cluster_id, context_id)
    
    # Assert
    assert result == expected_response
    mock_make_api_request.assert_called_once_with(
        "POST",
        "/api/1.2/contexts/destroy",
        data={"clusterId": cluster_id, "contextId": context_id}
    )


@pytest.mark.asyncio
async def test_get_context_status(mock_make_api_request):
    """Test getting context status."""
    # Arrange
    cluster_id = "test-cluster-123"
    context_id = "context-123"
    expected_response = {"id": context_id, "status": "Running"}
    mock_make_api_request.return_value = expected_response
    
    # Act
    result = await command_execution.get_context_status(cluster_id, context_id)
    
    # Assert
    assert result == expected_response
    mock_make_api_request.assert_called_once_with(
        "GET",
        "/api/1.2/contexts/status",
        params={"clusterId": cluster_id, "contextId": context_id}
    )


@pytest.mark.asyncio
async def test_execute_command_simple_success(mock_make_api_request):
    """Test simple command execution that completes successfully."""
    # Arrange
    cluster_id = "test-cluster-123"
    command = "1 + 1"
    language = "python"
    
    # Mock responses for the workflow
    mock_make_api_request.side_effect = [
        {"id": "context-123"},  # create_execution_context
        {"id": "command-456"},  # execute_command
        {  # get_command_status - Finished
            "id": "command-456",
            "status": "Finished",
            "results": {"resultType": "text", "data": "2"}
        },
        {}  # destroy_execution_context
    ]
    
    # Act
    result = await command_execution.execute_command_simple(
        cluster_id, command, language, max_wait_seconds=10
    )
    
    # Assert
    assert result["status"] == "Finished"
    assert result["results"]["data"] == "2"
    assert mock_make_api_request.call_count == 4


@pytest.mark.asyncio
async def test_execute_command_simple_error(mock_make_api_request):
    """Test simple command execution that fails with an error."""
    # Arrange
    cluster_id = "test-cluster-123"
    command = "invalid syntax"
    language = "python"
    
    # Mock responses for the workflow
    mock_make_api_request.side_effect = [
        {"id": "context-123"},  # create_execution_context
        {"id": "command-456"},  # execute_command
        {  # get_command_status - Error
            "id": "command-456",
            "status": "Error",
            "results": {"cause": "SyntaxError: invalid syntax"}
        },
        {}  # destroy_execution_context (cleanup)
    ]
    
    # Act & Assert
    with pytest.raises(DatabricksAPIError, match="Command execution failed"):
        await command_execution.execute_command_simple(
            cluster_id, command, language, max_wait_seconds=10
        )
    
    # Verify cleanup was called
    assert mock_make_api_request.call_count == 4


@pytest.mark.asyncio
async def test_execute_command_simple_timeout(mock_make_api_request):
    """Test simple command execution that times out."""
    # Arrange
    cluster_id = "test-cluster-123"
    command = "import time; time.sleep(100)"
    language = "python"
    
    # Mock responses - command keeps running
    # With max_wait_seconds=4 and poll_interval=2, we get 2 status checks:
    # - First check at elapsed=0
    # - Second check at elapsed=2
    # - Loop exits when elapsed=4 (not < 4)
    mock_make_api_request.side_effect = [
        {"id": "context-123"},  # create_execution_context
        {"id": "command-456"},  # execute_command
        {"id": "command-456", "status": "Running"},  # get_command_status #1
        {"id": "command-456", "status": "Running"},  # get_command_status #2
        {}  # destroy_execution_context (cleanup)
    ]
    
    # Act & Assert
    with pytest.raises(DatabricksAPIError, match="timed out"):
        await command_execution.execute_command_simple(
            cluster_id, command, language, max_wait_seconds=4
        )
    
    # Verify cleanup was called (5 total: create, execute, 2 status checks, destroy)
    assert mock_make_api_request.call_count == 5


@pytest.mark.asyncio
async def test_execute_command_simple_no_context_id(mock_make_api_request):
    """Test simple command execution when context creation fails."""
    # Arrange
    cluster_id = "test-cluster-123"
    command = "1 + 1"
    language = "python"
    
    # Mock response with no context ID
    mock_make_api_request.return_value = {}
    
    # Act & Assert
    with pytest.raises(DatabricksAPIError, match="no context ID returned"):
        await command_execution.execute_command_simple(
            cluster_id, command, language, max_wait_seconds=10
        )


@pytest.mark.asyncio
async def test_execute_command_simple_cleanup_failure(mock_make_api_request):
    """Test that cleanup failure doesn't prevent returning results."""
    # Arrange
    cluster_id = "test-cluster-123"
    command = "1 + 1"
    language = "python"
    
    # Mock responses - cleanup fails but should be caught
    mock_make_api_request.side_effect = [
        {"id": "context-123"},  # create_execution_context
        {"id": "command-456"},  # execute_command
        {  # get_command_status - Finished
            "id": "command-456",
            "status": "Finished",
            "results": {"resultType": "text", "data": "2"}
        },
        DatabricksAPIError("Cleanup failed")  # destroy_execution_context fails
    ]
    
    # Act - should not raise despite cleanup failure
    result = await command_execution.execute_command_simple(
        cluster_id, command, language, max_wait_seconds=10
    )
    
    # Assert
    assert result["status"] == "Finished"
    assert mock_make_api_request.call_count == 4


# Tests for MCP Tool Polling Behavior

@pytest.mark.asyncio
async def test_mcp_get_command_status_polls_until_finished():
    """Test that MCP tool polls until command reaches finished state."""
    from src.server.databricks_mcp_server import DatabricksMCPServer
    import json
    
    # Arrange
    server = DatabricksMCPServer()
    params = {
        "cluster_id": "test-cluster",
        "context_id": "test-context",
        "command_id": "test-command",
        "poll_interval": 0.1,  # Fast polling for test
        "max_wait_seconds": 10
    }
    
    # Mock the get_command_status API to return Running, then Finished
    with patch("src.api.command_execution.get_command_status") as mock_get_status:
        mock_get_status.side_effect = [
            {"status": "Queued", "id": "test-command"},
            {"status": "Running", "id": "test-command"},
            {"status": "Finished", "id": "test-command", "results": {"data": "success"}}
        ]
        
        # Act
        result = await server.call_tool("get_command_status", {"params": params})
        
        # Assert
        result_text = result[0].text
        result_data = json.loads(result_text)
        assert result_data["status"] == "Finished"
        assert result_data["results"]["data"] == "success"
        assert mock_get_status.call_count == 3  # Polled 3 times


@pytest.mark.asyncio
async def test_mcp_get_command_status_returns_immediately_on_error():
    """Test that MCP tool returns immediately when command has error status."""
    from src.server.databricks_mcp_server import DatabricksMCPServer
    import json
    
    # Arrange
    server = DatabricksMCPServer()
    params = {
        "cluster_id": "test-cluster",
        "context_id": "test-context",
        "command_id": "test-command",
        "poll_interval": 1,
        "max_wait_seconds": 10
    }
    
    # Mock the get_command_status API to return Error immediately
    with patch("src.api.command_execution.get_command_status") as mock_get_status:
        mock_get_status.return_value = {
            "status": "Error",
            "id": "test-command",
            "results": {"cause": "Test error"}
        }
        
        # Act
        result = await server.call_tool("get_command_status", {"params": params})
        
        # Assert
        result_text = result[0].text
        result_data = json.loads(result_text)
        assert result_data["status"] == "Error"
        assert mock_get_status.call_count == 1  # Only called once, no polling


@pytest.mark.asyncio
async def test_mcp_get_command_status_timeout():
    """Test that MCP tool respects max_wait_seconds timeout."""
    from src.server.databricks_mcp_server import DatabricksMCPServer
    import json
    
    # Arrange
    server = DatabricksMCPServer()
    params = {
        "cluster_id": "test-cluster",
        "context_id": "test-context",
        "command_id": "test-command",
        "poll_interval": 0.1,  # Fast polling for test
        "max_wait_seconds": 0.3  # Short timeout
    }
    
    # Mock the get_command_status API to always return Running
    with patch("src.api.command_execution.get_command_status") as mock_get_status:
        mock_get_status.return_value = {"status": "Running", "id": "test-command"}
        
        # Act
        result = await server.call_tool("get_command_status", {"params": params})
        
        # Assert
        result_text = result[0].text
        result_data = json.loads(result_text)
        assert result_data["status"] == "Running"
        # Verify no timeout_reached flag is present
        assert "timeout_reached" not in result_data
        # Should have polled multiple times (at 0, 0.1, 0.2, timeout at 0.3)
        assert mock_get_status.call_count >= 3


@pytest.mark.asyncio
async def test_mcp_get_command_status_cancelled():
    """Test that MCP tool returns immediately when command is cancelled."""
    from src.server.databricks_mcp_server import DatabricksMCPServer
    import json
    
    # Arrange
    server = DatabricksMCPServer()
    params = {
        "cluster_id": "test-cluster",
        "context_id": "test-context",
        "command_id": "test-command"
    }
    
    # Mock the get_command_status API to return Cancelled
    with patch("src.api.command_execution.get_command_status") as mock_get_status:
        mock_get_status.return_value = {"status": "Cancelled", "id": "test-command"}
        
        # Act
        result = await server.call_tool("get_command_status", {"params": params})
        
        # Assert
        result_text = result[0].text
        result_data = json.loads(result_text)
        assert result_data["status"] == "Cancelled"
        assert mock_get_status.call_count == 1  # No polling needed
