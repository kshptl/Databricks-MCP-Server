"""
Core utility functions for the Databricks MCP Toolkit.

This module provides essential utilities for making HTTP requests to the Databricks API,
including connection pooling, retry logic, and standardized error handling.

Author: Kush Patel
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Union
from functools import lru_cache

import httpx
from httpx import RequestError

from src.core.config import get_api_headers, get_databricks_api_url

# Configure structured logging with enhanced formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)
logger = logging.getLogger(__name__)

# Global HTTP client with connection pooling for better performance
# Reusing connections significantly improves throughput for multiple API calls
_http_client: Optional[httpx.AsyncClient] = None


class DatabricksAPIError(Exception):
    """
    Custom exception for Databricks API errors.
    
    This exception encapsulates API errors with detailed context including
    HTTP status codes and response data for better debugging.
    
    Attributes:
        message: Human-readable error description
        status_code: HTTP status code if available
        response: Raw API response for debugging
    """

    def __init__(
        self, 
        message: str, 
        status_code: Optional[int] = None, 
        response: Optional[Any] = None
    ):
        """Initialize API error with context."""
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(self.message)
    
    def __str__(self) -> str:
        """Return formatted error message with status code."""
        if self.status_code:
            return f"[{self.status_code}] {self.message}"
        return self.message


async def get_http_client() -> httpx.AsyncClient:
    """
    Get or create a singleton HTTP client with connection pooling.
    
    Connection pooling significantly improves performance by reusing TCP connections
    across multiple requests, reducing overhead from connection establishment.
    
    Returns:
        Configured AsyncClient instance with pooling and timeout settings
    """
    global _http_client
    
    if _http_client is None:
        # Configure connection pooling limits for optimal performance
        limits = httpx.Limits(
            max_keepalive_connections=20,  # Keep up to 20 connections alive
            max_connections=50,             # Allow up to 50 total connections
            keepalive_expiry=30.0          # Keep connections alive for 30 seconds
        )
        
        # Default timeout configuration (can be overridden per request)
        timeout = httpx.Timeout(30.0, connect=10.0)
        
        _http_client = httpx.AsyncClient(
            limits=limits,
            timeout=timeout,
            follow_redirects=True  # Automatically follow redirects
        )
        
        logger.debug("Initialized HTTP client with connection pooling")
    
    return _http_client


async def close_http_client() -> None:
    """
    Close the global HTTP client and release resources.
    
    Should be called during application shutdown to properly clean up
    connections and free system resources.
    """
    global _http_client
    
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None
        logger.debug("Closed HTTP client")


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
    Make an async request to the Databricks API with intelligent retry logic.
    
    This function uses connection pooling for better performance and implements
    exponential backoff for transient failures. Retries are attempted for
    timeout and connection errors, but not for client errors (4xx).
    
    Args:
        method: HTTP method ("GET", "POST", "PUT", "DELETE")
        endpoint: API endpoint path (e.g., "/api/2.0/clusters/list")
        data: Request body data (automatically converted to JSON)
        params: URL query parameters
        files: Files to upload (for multipart/form-data requests)
        max_retries: Maximum retry attempts for transient failures
        timeout_seconds: Request timeout in seconds
        
    Returns:
        Parsed JSON response as a dictionary
        
    Raises:
        DatabricksAPIError: If the API request fails after all retries
        
    Example:
        >>> result = await make_api_request(
        ...     "POST",
        ...     "/api/2.0/clusters/create",
        ...     data={"cluster_name": "my-cluster", "spark_version": "11.3.x-scala2.12"}
        ... )
    """
    # Build full URL and prepare headers
    url = get_databricks_api_url(endpoint)
    headers = get_api_headers()
    
    # Use connection pooling for better performance
    client = await get_http_client()
    
    # Configure per-request timeout (overrides default)
    timeout_config = httpx.Timeout(timeout_seconds, connect=10.0)
    
    # Exponential backoff retry logic
    for attempt in range(max_retries + 1):
        try:
            # Redact sensitive data in logs for security
            safe_data = "**REDACTED**" if data else None
            logger.debug(
                f"API Request [{method}] {endpoint} "
                f"(attempt {attempt + 1}/{max_retries + 1}) "
                f"Params: {params} Data: {safe_data}"
            )
            
            # Make the request using the pooled client
            # This reuses existing connections for better performance
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data if data and not files else None,
                files=files,
                timeout=timeout_config
            )
            
            # Raise exception for HTTP errors (4xx, 5xx)
            response.raise_for_status()
            
            # Parse and return JSON response
            if response.content:
                result = response.json()
                logger.debug(f"API Response [{response.status_code}]: Success")
                return result
            
            # Empty response is valid
            return {}
            
        except httpx.TimeoutException as e:
            # Timeout errors are retryable
            if attempt < max_retries:
                # Exponential backoff: 2s, 4s, 6s
                wait_time = (attempt + 1) * 2
                logger.warning(
                    f"Request timeout (attempt {attempt + 1}/{max_retries + 1}), "
                    f"retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)
                continue
            
            # All retries exhausted
            error_msg = (
                f"API request timed out after {max_retries + 1} attempts "
                f"(timeout: {timeout_seconds}s)"
            )
            logger.error(f"Timeout Error: {error_msg}")
            raise DatabricksAPIError(error_msg, status_code=408) from e
            
        except httpx.HTTPStatusError as e:
            # HTTP errors (4xx, 5xx) - extract detailed error info
            status_code = e.response.status_code
            
            # Don't retry client errors (4xx) as they won't succeed
            should_retry = (
                attempt < max_retries and 
                status_code >= 500  # Only retry server errors (5xx)
            )
            
            # Try to extract error message from response
            error_detail = ""
            try:
                error_data = e.response.json()
                error_detail = error_data.get("message") or error_data.get("error", "")
            except (ValueError, AttributeError):
                error_detail = e.response.text[:200]  # Truncate long error messages
            
            error_msg = f"HTTP {status_code}: {error_detail}"
            
            if should_retry:
                wait_time = (attempt + 1) * 2
                logger.warning(
                    f"Server error {status_code} (attempt {attempt + 1}/{max_retries + 1}), "
                    f"retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)
                continue
            
            # Don't retry or all retries exhausted
            logger.error(f"API Error: {error_msg}")
            raise DatabricksAPIError(error_msg, status_code, e.response) from e
            
        except httpx.RequestError as e:
            # Connection/network errors - retryable
            if attempt < max_retries:
                wait_time = (attempt + 1) * 2
                logger.warning(
                    f"Connection error (attempt {attempt + 1}/{max_retries + 1}), "
                    f"retrying in {wait_time}s: {str(e)}"
                )
                await asyncio.sleep(wait_time)
                continue
            
            # All retries exhausted
            error_msg = f"Connection failed: {str(e)}"
            logger.error(f"Network Error: {error_msg}")
            raise DatabricksAPIError(error_msg) from e
        
        except Exception as e:
            # Unexpected errors - log with traceback
            error_msg = f"Unexpected error during API request: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise DatabricksAPIError(error_msg) from e
    
    # This should never be reached due to raises in the loop
    raise DatabricksAPIError("Unexpected: Retry loop exited without return")


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
