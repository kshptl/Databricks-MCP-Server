"""
Utility functions for the Databricks MCP server.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union

import httpx
from httpx import RequestError

from src.core.config import get_api_headers, get_databricks_api_url

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class DatabricksAPIError(Exception):
    """Exception raised for errors in the Databricks API."""

    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[Any] = None):
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(self.message)


async def make_api_request(
    method: str,
    endpoint: str,
    data: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    files: Optional[Dict[str, Any]] = None,
    max_retries: int = 3,
    timeout_seconds: float = 30.0,
) -> Dict[str, Any]:
    """
    Make an async request to the Databricks API with timeout and retry handling.
    
    Args:
        method: HTTP method ("GET", "POST", "PUT", "DELETE")
        endpoint: API endpoint path
        data: Request body data
        params: Query parameters
        files: Files to upload
        max_retries: Maximum number of retry attempts
        timeout_seconds: Request timeout in seconds
        
    Returns:
        Response data as a dictionary
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    import asyncio
    
    url = get_databricks_api_url(endpoint)
    headers = get_api_headers()
    
    # Configure timeout
    timeout_config = httpx.Timeout(timeout_seconds)
    
    # Retry logic
    for attempt in range(max_retries + 1):
        try:
            # Log the request (omit sensitive information)
            safe_data = "**REDACTED**" if data else None
            logger.debug(f"API Request (attempt {attempt + 1}/{max_retries + 1}): {method} {url} Params: {params} Data: {safe_data}")
            
            # Make the async request using httpx with timeout
            async with httpx.AsyncClient(timeout=timeout_config) as client:
                response = await client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=data if data and not files else None,
                    files=files,
                )
            
            # Check for HTTP errors
            response.raise_for_status()
            
            # Parse response
            if response.content:
                return response.json()
            return {}
            
        except httpx.TimeoutException as e:
            # Handle timeout specifically
            if attempt < max_retries:
                wait_time = (attempt + 1) * 2  # Exponential backoff: 2, 4, 6 seconds
                logger.warning(f"Request timed out (attempt {attempt + 1}/{max_retries + 1}), retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                # Final timeout - don't crash, return graceful error
                error_msg = f"API request timed out after {max_retries + 1} attempts (timeout: {timeout_seconds}s)"
                logger.error(f"API Timeout: {error_msg}")
                raise DatabricksAPIError(error_msg, status_code=408) from e
            
        except httpx.RequestError as e:
            # Handle connection errors with retry
            if attempt < max_retries and not isinstance(e, (httpx.HTTPStatusError,)):
                wait_time = (attempt + 1) * 2
                logger.warning(f"Connection error (attempt {attempt + 1}/{max_retries + 1}), retrying in {wait_time}s: {str(e)}")
                await asyncio.sleep(wait_time)
                continue
            else:
                # Log and re-raise
                status_code = getattr(e.response, "status_code", None) if hasattr(e, "response") else None
                error_msg = f"API request failed: {str(e)}"
                
                # Try to extract error details from response
                error_response = None
                if hasattr(e, "response") and e.response is not None:
                    try:
                        error_response = e.response.json()
                        error_msg = f"{error_msg} - {error_response.get('error', '')}"
                    except ValueError:
                        error_response = e.response.text
                
                logger.error(f"API Error: {error_msg}")
                raise DatabricksAPIError(error_msg, status_code, error_response) from e
        
        except Exception as e:
            # Handle any other exceptions (final fallback)
            error_msg = f"Unexpected API request error: {str(e)}"
            logger.error(f"API Error: {error_msg}", exc_info=True)
            raise DatabricksAPIError(error_msg) from e


def format_response(
    success: bool, 
    data: Optional[Union[Dict[str, Any], List[Any]]] = None, 
    error: Optional[str] = None,
    status_code: int = 200
) -> Dict[str, Any]:
    """
    Format a standardized response.
    
    Args:
        success: Whether the operation was successful
        data: Response data
        error: Error message if not successful
        status_code: HTTP status code
        
    Returns:
        Formatted response dictionary
    """
    response = {
        "success": success,
        "status_code": status_code,
    }
    
    if data is not None:
        response["data"] = data
        
    if error:
        response["error"] = error
        
    return response
