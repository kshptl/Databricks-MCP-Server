"""
Databricks MCP Server

This module implements a standalone MCP server that provides tools for interacting
with Databricks APIs. It follows the Model Context Protocol standard, communicating
via stdio and directly connecting to Databricks when tools are invoked.
"""

import asyncio
import json
import logging
import sys
import os
from typing import Any, Dict, List, Optional, Union, cast

from mcp.server import FastMCP
from mcp.types import TextContent, Tool
from mcp.server.stdio import stdio_server

from src.api import clusters, command_execution, dbfs, jobs, notebooks, sql
from src.core.config import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    filename="databricks_mcp.log",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class DatabricksMCPServer(FastMCP):
    """An MCP server for Databricks APIs."""

    def __init__(self):
        """Initialize the Databricks MCP server."""
        super().__init__(name="databricks-mcp", 
                         version="1.0.0", 
                         instructions="Use this server to manage Databricks resources")
        logger.info("Initializing Databricks MCP server")
        logger.info(f"Databricks host: {settings.DATABRICKS_HOST}")
        
        # Register tools
        self._register_tools()
    
    def _register_tools(self):
        """Register all Databricks MCP tools."""
        
        # Cluster management tools
        @self.tool(
            name="list_clusters",
            description="List all Databricks clusters",
        )
        async def list_clusters(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing clusters with params: {params}")
            try:
                result = await clusters.list_clusters()
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error listing clusters: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="create_cluster",
            description="Create a new Databricks cluster with parameters: cluster_name (required), spark_version (required), node_type_id (required), num_workers, autotermination_minutes",
        )
        async def create_cluster(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating cluster with params: {params}")
            try:
                result = await clusters.create_cluster(params)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error creating cluster: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="terminate_cluster",
            description="Terminate a Databricks cluster with parameter: cluster_id (required)",
        )
        async def terminate_cluster(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Terminating cluster with params: {params}")
            try:
                result = await clusters.terminate_cluster(params.get("cluster_id"))
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error terminating cluster: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="get_cluster",
            description="Get information about a specific Databricks cluster with parameter: cluster_id (required)",
        )
        async def get_cluster(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting cluster info with params: {params}")
            try:
                result = await clusters.get_cluster(params.get("cluster_id"))
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error getting cluster info: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="start_cluster",
            description="Start a terminated Databricks cluster with parameter: cluster_id (required)",
        )
        async def start_cluster(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Starting cluster with params: {params}")
            try:
                result = await clusters.start_cluster(params.get("cluster_id"))
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error starting cluster: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        # Job management tools
        @self.tool(
            name="list_jobs",
            description="List all Databricks jobs",
        )
        async def list_jobs(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing jobs with params: {params}")
            try:
                result = await jobs.list_jobs()
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error listing jobs: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="get_job",
            description="Get information about a specific Databricks job with parameter: job_id (required)",
        )
        async def get_job(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting job info with params: {params}")
            try:
                result = await jobs.get_job(params.get("job_id"))
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error getting job info: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="run_job",
            description="Run a Databricks job with parameters: job_id (required), notebook_params (optional)",
        )
        async def run_job(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Running job with params: {params}")
            try:
                notebook_params = params.get("notebook_params", {})
                result = await jobs.run_job(params.get("job_id"), notebook_params)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error running job: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="submit_single_run",
            description="Submit and run a notebook as a one-time job (not part of an existing job). Parameters: run_config (required) - a dictionary containing the run configuration with tasks (list of task definitions), run_name (optional), timeout_seconds (optional), git_source (optional), and cluster configuration (new_cluster or existing_cluster_id). Each task should have a task_key and either notebook_task, spark_jar_task, spark_python_task, etc.",
        )
        async def submit_single_run(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Submitting single run with params: {params}")
            try:
                run_config = params.get("run_config", params)
                result = await jobs.submit_single_run(run_config)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error submitting single run: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="get_run_output",
            description="Get the output from a completed job run. Parameters: run_id (required)",
        )
        async def get_run_output(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting run output with params: {params}")
            try:
                result = await jobs.get_run_output(params.get("run_id"))
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error getting run output: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="get_run",
            description="Get information about a specific job run. Parameters: run_id (required)",
        )
        async def get_run_tool(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting run info with params: {params}")
            try:
                result = await jobs.get_run(params.get("run_id"))
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error getting run info: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="wait_for_run_completion",
            description="Wait for a job run to complete by polling its status. Parameters: run_id (required), poll_interval (optional, default 10 seconds), max_wait_seconds (optional, default 3600 seconds)",
        )
        async def wait_for_run_completion(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Waiting for run completion with params: {params}")
            try:
                run_id = params.get("run_id")
                poll_interval = params.get("poll_interval", 10)
                max_wait_seconds = params.get("max_wait_seconds", 3600)
                result = await jobs.wait_for_run_completion(run_id, poll_interval, max_wait_seconds)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error waiting for run completion: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        # Notebook management tools
        @self.tool(
            name="list_notebooks",
            description="List notebooks in a workspace directory with parameter: path (required)",
        )
        async def list_notebooks(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing notebooks with params: {params}")
            try:
                result = await notebooks.list_notebooks(params.get("path"))
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error listing notebooks: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="export_notebook",
            description="Export a notebook from the workspace with parameters: path (required), format (optional, one of: SOURCE, HTML, JUPYTER, DBC)",
        )
        async def export_notebook(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Exporting notebook with params: {params}")
            try:
                format_type = params.get("format", "SOURCE")
                result = await notebooks.export_notebook(params.get("path"), format_type)
                
                # For notebooks, we might want to trim the response for readability
                content = result.get("content", "")
                if len(content) > 1000:
                    summary = f"{content[:1000]}... [content truncated, total length: {len(content)} characters]"
                    result["content"] = summary
                
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error exporting notebook: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        # DBFS tools
        @self.tool(
            name="list_files",
            description="List files and directories in a DBFS path with parameter: dbfs_path (required)",
        )
        async def list_files(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Listing files with params: {params}")
            try:
                result = await dbfs.list_files(params.get("dbfs_path"))
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error listing files: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        # SQL tools
        @self.tool(
            name="execute_sql",
            description="Execute a SQL statement with parameters: statement (required), warehouse_id (optional, uses DATABRICKS_WAREHOUSE_ID env var if not provided), catalog (optional), schema (optional)",
        )
        async def execute_sql(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Executing SQL with params: {params}")
            try:
                statement = params.get("statement")
                warehouse_id = params.get("warehouse_id")  # Now optional
                catalog = params.get("catalog")
                schema = params.get("schema")
                
                result = await sql.execute_statement(statement, warehouse_id, catalog, schema)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error executing SQL: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        # Command execution tools
        @self.tool(
            name="create_execution_context",
            description="Create an execution context for running commands on a cluster. The context maintains state between commands, allowing sequential commands to reference variables from previous commands. Parameters: cluster_id (required), language (required: python, scala, sql, or r)",
        )
        async def create_execution_context_tool(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Creating execution context with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                language = params.get("language")
                result = await command_execution.create_execution_context(cluster_id, language)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error creating execution context: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="execute_command",
            description="Execute a command in an existing execution context. This allows running code that depends on variables or state from previous commands. Parameters: cluster_id (required), context_id (required), command (required), language (required: python, scala, sql, or r)",
        )
        async def execute_command_tool(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Executing command with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                context_id = params.get("context_id")
                command = params.get("command")
                language = params.get("language")
                result = await command_execution.execute_command(cluster_id, context_id, command, language)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error executing command: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="get_command_status",
            description="Get the status and results of a command execution. This tool automatically polls until the command reaches a final state (Finished, Error, or Cancelled). Parameters: cluster_id (required), context_id (required), command_id (required), poll_interval (optional, default: 2 seconds), max_wait_seconds (optional, default: 300 seconds)",
        )
        async def get_command_status_tool(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting command status with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                context_id = params.get("context_id")
                command_id = params.get("command_id")
                poll_interval = params.get("poll_interval", 2)
                max_wait_seconds = params.get("max_wait_seconds", 300)
                
                # Poll until command reaches a final state
                elapsed = 0
                while elapsed < max_wait_seconds:
                    result = await command_execution.get_command_status(cluster_id, context_id, command_id)
                    status = result.get("status")
                    
                    # Check if command has reached a final state
                    if status not in ["Queued", "Running"]:
                        logger.info(f"Command reached final state: {status}")
                        return [TextContent(type="text", text=json.dumps(result))]
                    
                    # Still running, wait and retry
                    logger.debug(f"Command status: {status}, waiting {poll_interval} seconds...")
                    await asyncio.sleep(poll_interval)
                    elapsed += poll_interval
                
                # Timeout reached - return the last status without modification
                logger.warning(f"Command status polling timed out after {max_wait_seconds} seconds")
                result = await command_execution.get_command_status(cluster_id, context_id, command_id)
                return [TextContent(type="text", text=json.dumps(result))]
                
            except Exception as e:
                logger.error(f"Error getting command status: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="cancel_command",
            description="Cancel a running command execution. Parameters: cluster_id (required), context_id (required), command_id (required)",
        )
        async def cancel_command_tool(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Cancelling command with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                context_id = params.get("context_id")
                command_id = params.get("command_id")
                result = await command_execution.cancel_command(cluster_id, context_id, command_id)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error cancelling command: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="destroy_execution_context",
            description="Destroy an execution context and clean up resources. Call this when done with a series of commands. Parameters: cluster_id (required), context_id (required)",
        )
        async def destroy_execution_context_tool(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Destroying execution context with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                context_id = params.get("context_id")
                result = await command_execution.destroy_execution_context(cluster_id, context_id)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error destroying execution context: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="get_context_status",
            description="Get the status of an execution context to verify it's still valid. Parameters: cluster_id (required), context_id (required)",
        )
        async def get_context_status_tool(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Getting context status with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                context_id = params.get("context_id")
                result = await command_execution.get_context_status(cluster_id, context_id)
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error getting context status: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
        
        @self.tool(
            name="execute_command_simple",
            description="Execute a single command without manually managing contexts. This convenience tool creates a context, executes the command, waits for completion, and cleans up automatically. Use this for one-off commands. Parameters: cluster_id (required), command (required), language (required: python, scala, sql, or r), max_wait_seconds (optional, default: 60)",
        )
        async def execute_command_simple_tool(params: Dict[str, Any]) -> List[TextContent]:
            logger.info(f"Executing simple command with params: {params}")
            try:
                cluster_id = params.get("cluster_id")
                command = params.get("command")
                language = params.get("language")
                max_wait_seconds = params.get("max_wait_seconds", 60)
                result = await command_execution.execute_command_simple(
                    cluster_id, command, language, max_wait_seconds
                )
                return [TextContent(type="text", text=json.dumps(result))]
            except Exception as e:
                logger.error(f"Error executing simple command: {str(e)}")
                return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


async def main():
    """Main entry point for the MCP server."""
    try:
        logger.info("Starting Databricks MCP server")
        server = DatabricksMCPServer()
        
        # Use the built-in method for stdio servers
        # This is the recommended approach for MCP servers
        await server.run_stdio_async()
            
    except Exception as e:
        logger.error(f"Error in Databricks MCP server: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    # Turn off buffering in stdout
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(line_buffering=True)
    
    asyncio.run(main())
