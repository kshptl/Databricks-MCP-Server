"""
Configuration management for the Databricks MCP Toolkit.

This module handles all configuration through environment variables with
Pydantic-based validation, ensuring type safety and proper error handling.

Author: Kush Patel
"""

import os
import logging
from typing import Dict, Optional
from functools import lru_cache

# Conditional import for .env file support
try:
    from dotenv import load_dotenv
    # Load environment variables from .env file if present
    # This is particularly useful for local development
    load_dotenv()
    _dotenv_loaded = True
except ImportError:
    # python-dotenv is optional - not critical for production deployments
    _dotenv_loaded = False

from pydantic import Field, field_validator, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

# Application version - update during releases
VERSION = "0.1.0"

# Configure module logger
logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """
    Application configuration with validation and type safety.
    
    All settings are loaded from environment variables with sensible defaults
    where appropriate. Sensitive values like tokens are handled securely.
    
    Environment Variables:
        DATABRICKS_HOST: Databricks workspace URL (required)
        DATABRICKS_TOKEN: Personal access token (required)
        DATABRICKS_WAREHOUSE_ID: SQL warehouse ID (optional)
        SERVER_HOST: MCP server bind address (default: 0.0.0.0)
        SERVER_PORT: MCP server port (default: 8000)
        DEBUG: Enable debug mode (default: false)
        LOG_LEVEL: Logging level (default: INFO)
    """
    
    # Databricks API Configuration
    # These are required for communicating with Databricks
    DATABRICKS_HOST: str = Field(
        default="",
        description="Databricks workspace URL (e.g., https://example.databricks.net)"
    )
    
    DATABRICKS_TOKEN: SecretStr = Field(
        default=SecretStr(""),
        description="Databricks personal access token (kept secure)"
    )
    
    DATABRICKS_WAREHOUSE_ID: Optional[str] = Field(
        default=None,
        description="SQL warehouse ID for SQL execution (optional)"
    )
    
    # MCP Server Configuration
    SERVER_HOST: str = Field(
        default="0.0.0.0",
        description="Server bind address (0.0.0.0 for all interfaces)"
    )
    
    SERVER_PORT: int = Field(
        default=8000,
        ge=1,  # Port must be >= 1
        le=65535,  # Port must be <= 65535
        description="Server port number"
    )
    
    # Application Settings
    DEBUG: bool = Field(
        default=False,
        description="Enable debug mode for verbose logging"
    )
    
    LOG_LEVEL: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    
    # Application metadata
    VERSION: str = Field(
        default=VERSION,
        description="Application version"
    )
    
    # Pydantic v2 configuration
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"  # Ignore unknown environment variables
    )
    
    @field_validator("DATABRICKS_HOST")
    @classmethod
    def validate_databricks_host(cls, v: str) -> str:
        """
        Validate Databricks host URL format.
        
        Ensures the host starts with http:// or https:// and removes
        trailing slashes for consistency.
        
        Args:
            v: Host URL to validate
            
        Returns:
            Validated and normalized host URL
            
        Raises:
            ValueError: If host format is invalid
        """
        if not v:
            raise ValueError(
                "DATABRICKS_HOST is required. "
                "Set it in .env file or as environment variable."
            )
        
        if not v.startswith(("https://", "http://")):
            raise ValueError(
                "DATABRICKS_HOST must start with http:// or https://. "
                f"Got: {v}"
            )
        
        # Remove trailing slash for consistency
        return v.rstrip("/")
    
    @field_validator("DATABRICKS_TOKEN")
    @classmethod
    def validate_databricks_token(cls, v: SecretStr) -> SecretStr:
        """
        Validate Databricks access token.
        
        Ensures token is not empty and has reasonable length.
        
        Args:
            v: Token to validate (as SecretStr for security)
            
        Returns:
            Validated token
            
        Raises:
            ValueError: If token is invalid
        """
        token_value = v.get_secret_value()
        
        if not token_value:
            raise ValueError(
                "DATABRICKS_TOKEN is required. "
                "Generate a personal access token in Databricks workspace."
            )
        
        # Basic token format validation
        if len(token_value) < 10:
            raise ValueError(
                "DATABRICKS_TOKEN appears too short. "
                "Ensure you're using a valid personal access token."
            )
        
        return v
    
    @field_validator("LOG_LEVEL")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """
        Validate logging level.
        
        Args:
            v: Log level string
            
        Returns:
            Validated log level in uppercase
            
        Raises:
            ValueError: If log level is invalid
        """
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        
        if v_upper not in valid_levels:
            raise ValueError(
                f"LOG_LEVEL must be one of {valid_levels}. Got: {v}"
            )
        
        return v_upper


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance (singleton pattern).
    
    Using lru_cache ensures we only create one Settings instance,
    avoiding redundant environment variable parsing.
    
    Returns:
        Global Settings instance
    """
    return Settings()


# Create global settings instance for convenience
# However, prefer using get_settings() for better testability
settings = get_settings()

# Log configuration status
if _dotenv_loaded:
    logger.info("Configuration loaded from .env file")
else:
    logger.info("Configuration loaded from environment variables (dotenv not available)")


def get_api_headers() -> Dict[str, str]:
    """
    Get HTTP headers for Databricks API authentication.
    
    Returns headers with Bearer token authentication and JSON content type.
    The token is securely retrieved from settings without exposing it in logs.
    
    Returns:
        Dictionary of HTTP headers for API requests
        
    Example:
        >>> headers = get_api_headers()
        >>> # Use with requests: requests.get(url, headers=headers)
    """
    return {
        "Authorization": f"Bearer {settings.DATABRICKS_TOKEN.get_secret_value()}",
        "Content-Type": "application/json",
        "User-Agent": f"Databricks-MCP-Toolkit/{VERSION}",  # Identify our client
    }


def get_databricks_api_url(endpoint: str) -> str:
    """
    Construct the complete Databricks API URL from an endpoint path.
    
    This function handles URL normalization by ensuring proper formatting
    of both the base host and endpoint path, preventing common URL issues.
    
    Args:
        endpoint: API endpoint path. Can start with or without leading slash.
                 Examples: "/api/2.0/clusters/list" or "api/2.0/clusters/list"
    
    Returns:
        Complete URL to the Databricks API endpoint
        
    Example:
        >>> url = get_databricks_api_url("/api/2.0/clusters/list")
        >>> # Returns: "https://example.databricks.net/api/2.0/clusters/list"
    """
    # Normalize endpoint - ensure it starts with a slash
    if not endpoint.startswith("/"):
        endpoint = f"/{endpoint}"
    
    # Normalize host - trailing slash already removed during validation
    host = settings.DATABRICKS_HOST
    
    return f"{host}{endpoint}"


def validate_configuration() -> bool:
    """
    Validate that all required configuration is present and valid.
    
    This is useful for health checks and startup validation to fail fast
    if configuration is missing or invalid.
    
    Returns:
        True if configuration is valid, False otherwise
        
    Raises:
        ValueError: If critical configuration is missing or invalid
    """
    try:
        # Access settings to trigger validation
        _ = settings.DATABRICKS_HOST
        _ = settings.DATABRICKS_TOKEN.get_secret_value()
        
        logger.info("Configuration validation successful")
        return True
        
    except ValueError as e:
        logger.error(f"Configuration validation failed: {e}")
        raise
