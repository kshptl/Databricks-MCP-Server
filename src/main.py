"""
Application entry point for Databricks MCP Toolkit.

This module bootstraps the MCP server with proper logging configuration
and command-line argument parsing for flexible deployment scenarios.

Author: Kush Patel
"""

import asyncio
import logging
import os
import sys
from typing import Optional

from src.core.config import settings, validate_configuration
from src.server.databricks_mcp_server import DatabricksMCPServer

# Module-level logger
logger = logging.getLogger(__name__)


async def start_mcp_server():
    """
    Initialize and start the MCP server instance.
    
    Creates a new DatabricksMCPServer and runs it in stdio mode,
    which is the standard communication channel for MCP protocols.
    The server will handle incoming tool requests until shutdown.
    """
    logger.info("Initializing Databricks MCP Toolkit server...")
    server = DatabricksMCPServer()
    
    # Run in stdio mode for MCP protocol compatibility
    await server.run_stdio_async()


def setup_logging(log_level: Optional[str] = None):
    """
    Configure structured logging for the application.
    
    Sets up console logging with timestamps and proper formatting.
    Log level can be overridden via command-line argument or uses
    the configured default from environment variables.
    
    Args:
        log_level: Optional log level override (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Determine effective log level
    effective_level = log_level or settings.LOG_LEVEL
    level = getattr(logging, effective_level)
    
    # Configure root logger with structured format
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(name)-30s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )
    
    logger.debug(f"Logging configured at {effective_level} level")


async def main():
    """
    Main application entry point with startup sequence.
    
    Performs configuration validation, logging setup, and server
    initialization in the correct order to ensure clean startup.
    """
    # Initialize logging first for visibility
    setup_logging()
    
    # Display startup banner
    logger.info("=" * 60)
    logger.info(f"Databricks MCP Toolkit v{settings.VERSION}")
    logger.info(f"Author: Kush Patel")
    logger.info("=" * 60)
    
    # Validate configuration before starting
    try:
        validate_configuration()
        logger.info(f"Connected to: {settings.DATABRICKS_HOST}")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Please check your .env file or environment variables")
        sys.exit(1)
    
    # Start the MCP server
    try:
        await start_mcp_server()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received, stopping server...")
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    
    parser = argparse.ArgumentParser(description="Databricks MCP Server")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the log level",
    )
    
    args = parser.parse_args()
    
    # Set up logging with command line arguments
    setup_logging(args.log_level)
    
    # Run the main function
    asyncio.run(main())
