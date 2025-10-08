# Copyright (c) 2025 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub, you may use it under the AGPL-3.0 terms.
# For commercial terms, contact kushapatel@live.com.

"""
API for executing SQL statements on Databricks.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request
from src.core.config import settings

# Configure logging
logger = logging.getLogger(__name__)


async def execute_statement(
    statement: str,
    warehouse_id: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    row_limit: int = 10000,
    byte_limit: int = 100000000,  # 100MB
) -> Dict[str, Any]:
    """
    Execute a SQL statement.
    
    Args:
        statement: The SQL statement to execute
        warehouse_id: ID of the SQL warehouse to use
        catalog: Optional catalog to use
        schema: Optional schema to use
        parameters: Optional statement parameters
        row_limit: Maximum number of rows to return
        byte_limit: Maximum number of bytes to return
        
    Returns:
        Response containing query results
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Executing SQL statement: {statement[:100]}...")
    
    # Use default warehouse ID if not provided
    if not warehouse_id:
        warehouse_id = settings.DATABRICKS_WAREHOUSE_ID
        if not warehouse_id:
            raise ValueError("warehouse_id is required. Set DATABRICKS_WAREHOUSE_ID environment variable or provide warehouse_id parameter")
    
    request_data = {
        "statement": statement,
        "warehouse_id": warehouse_id,
    }
    
    if catalog:
        request_data["catalog"] = catalog
        
    if schema:
        request_data["schema"] = schema
        
    if parameters:
        request_data["parameters"] = parameters
        
    return await make_api_request("POST", "/api/2.0/sql/statements", data=request_data)


async def execute_and_wait(
    statement: str,
    warehouse_id: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = 300,  # 5 minutes
    poll_interval_seconds: int = 1,
) -> Dict[str, Any]:
    """
    Execute a SQL statement and wait for completion.
    
    Args:
        statement: The SQL statement to execute
        warehouse_id: ID of the SQL warehouse to use
        catalog: Optional catalog to use
        schema: Optional schema to use
        parameters: Optional statement parameters
        timeout_seconds: Maximum time to wait for completion
        poll_interval_seconds: How often to poll for status
        
    Returns:
        Response containing query results
        
    Raises:
        DatabricksAPIError: If the API request fails
        TimeoutError: If query execution times out
    """
    import asyncio
    import time
    
    logger.info(f"Executing SQL statement with waiting: {statement[:100]}...")
    
    # Use default warehouse ID if not provided
    if not warehouse_id:
        warehouse_id = settings.DATABRICKS_WAREHOUSE_ID
        if not warehouse_id:
            raise ValueError("warehouse_id is required. Set DATABRICKS_WAREHOUSE_ID environment variable or provide warehouse_id parameter")
    
    # Start execution
    response = await execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        parameters=parameters,
    )
    
    statement_id = response.get("statement_id")
    if not statement_id:
        raise ValueError("No statement_id returned from execution")
    
    # Poll for completion
    start_time = time.time()
    status = response.get("status", {}).get("state", "")
    
    while status in ["PENDING", "RUNNING"]:
        # Check timeout
        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(f"Query execution timed out after {timeout_seconds} seconds")
        
        # Wait before polling again
        await asyncio.sleep(poll_interval_seconds)
        
        # Check status
        status_response = await get_statement_status(statement_id)
        status = status_response.get("status", {}).get("state", "")
        
        if status == "SUCCEEDED":
            return status_response
        elif status in ["FAILED", "CANCELED", "CLOSED"]:
            error_message = status_response.get("status", {}).get("error", {}).get("message", "Unknown error")
            raise DatabricksAPIError(f"Query execution failed: {error_message}", response=status_response)
    
    return response


async def get_statement_status(statement_id: str) -> Dict[str, Any]:
    """
    Get the status of a SQL statement.
    
    Args:
        statement_id: ID of the statement to check
        
    Returns:
        Response containing statement status
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting status of SQL statement: {statement_id}")
    return await make_api_request("GET", f"/api/2.0/sql/statements/{statement_id}", params={})


async def cancel_statement(statement_id: str) -> Dict[str, Any]:
    """
    Cancel a running SQL statement.
    
    Args:
        statement_id: ID of the statement to cancel
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Cancelling SQL statement: {statement_id}")
    return await make_api_request("POST", f"/api/2.0/sql/statements/{statement_id}/cancel", data={})
