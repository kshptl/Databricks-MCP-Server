"""
API for executing commands on Databricks clusters.

This module provides functions for creating execution contexts and running
commands on Databricks clusters. Execution contexts maintain state between
commands, allowing for sequential, dependent command execution.
"""

import logging
from typing import Any, Dict, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_execution_context(cluster_id: str, language: str) -> Dict[str, Any]:
    """
    Create an execution context for running cluster commands.
    
    An execution context is a stateful environment on a cluster that maintains
    state between command executions. This allows sequential commands to reference
    variables and data from previous commands.
    
    Args:
        cluster_id: ID of the running cluster
        language: Programming language (python, scala, sql, or r)
        
    Returns:
        Response containing the context ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Creating execution context on cluster {cluster_id} for language {language}")
    return await make_api_request(
        "POST",
        "/api/1.2/contexts/create",
        data={"clusterId": cluster_id, "language": language}
    )


async def execute_command(
    cluster_id: str,
    context_id: str,
    command: str,
    language: str
) -> Dict[str, Any]:
    """
    Execute a command in an existing execution context.
    
    Args:
        cluster_id: ID of the running cluster
        context_id: ID of the execution context
        command: Code to execute
        language: Programming language (python, scala, sql, or r)
        
    Returns:
        Response containing the command execution ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Executing command in context {context_id} on cluster {cluster_id}")
    logger.debug(f"Command: {command[:100]}...")  # Log first 100 chars of command
    
    return await make_api_request(
        "POST",
        "/api/1.2/commands/execute",
        data={
            "clusterId": cluster_id,
            "contextId": context_id,
            "command": command,
            "language": language
        }
    )


async def get_command_status(
    cluster_id: str,
    context_id: str,
    command_id: str
) -> Dict[str, Any]:
    """
    Get the status and results of a command execution.
    
    Args:
        cluster_id: ID of the running cluster
        context_id: ID of the execution context
        command_id: ID of the command execution
        
    Returns:
        Response containing command status and results
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting status for command {command_id}")
    return await make_api_request(
        "GET",
        "/api/1.2/commands/status",
        params={
            "clusterId": cluster_id,
            "contextId": context_id,
            "commandId": command_id
        }
    )


async def cancel_command(
    cluster_id: str,
    context_id: str,
    command_id: str
) -> Dict[str, Any]:
    """
    Cancel a running command.
    
    Args:
        cluster_id: ID of the running cluster
        context_id: ID of the execution context
        command_id: ID of the command execution to cancel
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Cancelling command {command_id}")
    return await make_api_request(
        "POST",
        "/api/1.2/commands/cancel",
        data={
            "clusterId": cluster_id,
            "contextId": context_id,
            "commandId": command_id
        }
    )


async def destroy_execution_context(cluster_id: str, context_id: str) -> Dict[str, Any]:
    """
    Destroy an execution context and clean up resources.
    
    Args:
        cluster_id: ID of the running cluster
        context_id: ID of the execution context to destroy
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Destroying execution context {context_id} on cluster {cluster_id}")
    return await make_api_request(
        "POST",
        "/api/1.2/contexts/destroy",
        data={"clusterId": cluster_id, "contextId": context_id}
    )


async def get_context_status(cluster_id: str, context_id: str) -> Dict[str, Any]:
    """
    Get the status of an execution context.
    
    Args:
        cluster_id: ID of the running cluster
        context_id: ID of the execution context
        
    Returns:
        Response containing context status
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting status for context {context_id}")
    return await make_api_request(
        "GET",
        "/api/1.2/contexts/status",
        params={"clusterId": cluster_id, "contextId": context_id}
    )


async def execute_command_simple(
    cluster_id: str,
    command: str,
    language: str,
    max_wait_seconds: int = 60
) -> Dict[str, Any]:
    """
    Convenience function to execute a single command without managing contexts.
    
    This function creates a context, executes the command, waits for completion,
    retrieves results, and destroys the context. Useful for one-off command execution.
    
    Args:
        cluster_id: ID of the running cluster
        command: Code to execute
        language: Programming language (python, scala, sql, or r)
        max_wait_seconds: Maximum time to wait for command completion (default: 60)
        
    Returns:
        Response containing command results
        
    Raises:
        DatabricksAPIError: If the API request fails or command times out
    """
    import asyncio
    
    logger.info(f"Executing simple command on cluster {cluster_id}")
    context_id = None
    
    try:
        # Create context
        context_response = await create_execution_context(cluster_id, language)
        context_id = context_response.get("id")
        
        if not context_id:
            raise DatabricksAPIError("Failed to create execution context: no context ID returned")
        
        # Execute command
        command_response = await execute_command(cluster_id, context_id, command, language)
        command_id = command_response.get("id")
        
        if not command_id:
            raise DatabricksAPIError("Failed to execute command: no command ID returned")
        
        # Poll for completion
        elapsed = 0
        poll_interval = 2  # seconds
        
        while elapsed < max_wait_seconds:
            status_response = await get_command_status(cluster_id, context_id, command_id)
            status = status_response.get("status")
            
            if status == "Finished":
                logger.info(f"Command completed successfully")
                return status_response
            elif status == "Error":
                error_msg = status_response.get("results", {}).get("cause", "Unknown error")
                raise DatabricksAPIError(f"Command execution failed: {error_msg}")
            elif status == "Cancelled":
                raise DatabricksAPIError("Command was cancelled")
            
            # Still running, wait and retry
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        
        # Timeout reached
        raise DatabricksAPIError(f"Command execution timed out after {max_wait_seconds} seconds")
        
    finally:
        # Always clean up context
        if context_id:
            try:
                await destroy_execution_context(cluster_id, context_id)
            except Exception as e:
                logger.warning(f"Failed to destroy context {context_id}: {str(e)}")
