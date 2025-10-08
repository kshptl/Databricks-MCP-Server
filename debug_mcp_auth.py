#!/usr/bin/env python
"""
Debug script to see what credentials the MCP server receives from AI agent
"""

import asyncio
import json
import logging
import sys
import os
from typing import Any, Dict, List

from mcp.server import FastMCP
from mcp.types import TextContent

# Import settings for comparison
sys.path.insert(0, '/home/patekus4/databricks-mcp-server/src')
from src.core.config import settings

# Enhanced logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("mcp_debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DebugMCPServer(FastMCP):
    def __init__(self):
        super().__init__(name="debug-databricks", version="1.0.0")
        logger.info("=== DEBUG MCP SERVER STARTED ===")
        
        # Log all environment variables that start with DATABRICKS
        logger.info("Environment variables:")
        for key, value in os.environ.items():
            if key.startswith("DATABRICKS"):
                masked_value = f"{value[:10]}...{value[-4:]}" if len(value) > 14 else value
                logger.info(f"  {key}: {masked_value}")
        
        self._register_debug_tools()
    
    def _register_debug_tools(self):
        @self.tool(
            name="debug_auth",
            description="Debug authentication credentials being used by AI agent"
        )
        async def debug_auth(params: Dict[str, Any]) -> List[TextContent]:
            logger.info("=== DEBUG AUTH TOOL CALLED ===")
            logger.info(f"Params received: {params}")
            
            # Get current environment as seen by the server
            debug_info = {
                "server_env": {
                    "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST", "NOT_SET"),
                    "DATABRICKS_TOKEN_LENGTH": len(os.getenv("DATABRICKS_TOKEN", "")),
                    "DATABRICKS_TOKEN_PREFIX": os.getenv("DATABRICKS_TOKEN", "")[:10],
                    "DATABRICKS_TOKEN_SUFFIX": os.getenv("DATABRICKS_TOKEN", "")[-4:],
                    "DATABRICKS_WAREHOUSE_ID": os.getenv("DATABRICKS_WAREHOUSE_ID", "NOT_SET")
                },
                "config_object": {
                    "DATABRICKS_HOST": getattr(settings, 'DATABRICKS_HOST', 'NOT_SET'),
                    "DATABRICKS_TOKEN_PREFIX": getattr(settings, 'DATABRICKS_TOKEN', '')[:10],
                    "DATABRICKS_WAREHOUSE_ID": getattr(settings, 'DATABRICKS_WAREHOUSE_ID', 'NOT_SET')
                }
            }
            
            logger.info(f"Debug info: {debug_info}")
            
            # Try to make a real API call and see what happens
            try:
                import httpx
                headers = {
                    "Authorization": f"Bearer {os.getenv('DATABRICKS_TOKEN', '')}",
                    "Content-Type": "application/json"
                }
                
                url = f"{os.getenv('DATABRICKS_HOST', '')}/api/2.0/clusters/list"
                
                async with httpx.AsyncClient() as client:
                    response = await client.get(url, headers=headers)
                    
                debug_info["api_test"] = {
                    "status_code": response.status_code,
                    "url": url,
                    "headers_sent": {
                        "Authorization": f"Bearer {os.getenv('DATABRICKS_TOKEN', '')[:10]}...{os.getenv('DATABRICKS_TOKEN', '')[-4:]}",
                        "Content-Type": "application/json"
                    }
                }
                
                if response.status_code == 200:
                    result = response.json()
                    debug_info["api_test"]["success"] = True
                    debug_info["api_test"]["clusters_found"] = len(result.get("clusters", []))
                else:
                    debug_info["api_test"]["success"] = False
                    debug_info["api_test"]["error"] = response.text
                    
            except Exception as e:
                debug_info["api_test"] = {"exception": str(e)}
            
            return [TextContent(type="text", text=json.dumps(debug_info, indent=2))]


async def main():
    server = DebugMCPServer()
    await server.run_stdio_async()

if __name__ == "__main__":
    asyncio.run(main())
