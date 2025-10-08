# Copyright (c) 2025 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub, you may use it under the AGPL-3.0 terms.
# For commercial terms, contact kushapatel@live.com.

#!/usr/bin/env python
"""
Databricks MCP Server - Direct Usage Example

This example demonstrates how to directly use the Databricks MCP server
without going through the MCP client protocol. It shows how to instantiate the
server class and call its tools directly using the correct async format.

IMPORTANT: All MCP tools require parameters wrapped in a 'params' object!
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.server.databricks_mcp_server import DatabricksMCPServer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def print_section_header(title: str) -> None:
    """Print a section header with the given title."""
    print(f"\n{title}")
    print("=" * len(title))


def print_result(tool_name: str, result: List[Any]) -> None:
    """Print the result from an MCP tool call."""
    print(f"\n🔧 {tool_name} Result:")
    print("-" * 30)
    
    if result and len(result) > 0:
        # Extract text from TextContent object
        result_text = result[0].text if hasattr(result[0], 'text') else str(result[0])
        
        try:
            # Try to parse as JSON for pretty printing
            parsed_result = json.loads(result_text)
            
            # Handle different result types
            if 'error' in parsed_result:
                print(f"❌ Error: {parsed_result['error']}")
                return
            
            # Format specific result types
            if 'clusters' in parsed_result:
                clusters = parsed_result['clusters']
                print(f"Found {len(clusters)} clusters:")
                for cluster in clusters[:5]:  # Show first 5
                    name = cluster.get('cluster_name', 'Unnamed')
                    state = cluster.get('state', 'Unknown')
                    cluster_id = cluster.get('cluster_id', 'Unknown')[:12] + '...'
                    print(f"  • {name} ({state}) - ID: {cluster_id}")
                    
            elif 'objects' in parsed_result:
                objects = parsed_result['objects']
                print(f"Found {len(objects)} objects:")
                for obj in objects[:5]:  # Show first 5
                    obj_type = obj.get('object_type', 'UNKNOWN')
                    path = obj.get('path', 'Unknown')
                    print(f"  • [{obj_type}] {path}")
                    
            elif 'jobs' in parsed_result:
                jobs = parsed_result['jobs']
                print(f"Found {len(jobs)} jobs:")
                for job in jobs[:5]:  # Show first 5
                    job_id = job.get('job_id', 'Unknown')
                    job_name = job.get('settings', {}).get('name', 'Unnamed')
                    print(f"  • {job_name} (ID: {job_id})")
                    
            elif 'files' in parsed_result:
                files = parsed_result['files']
                print(f"Found {len(files)} files/directories:")
                for file_obj in files[:5]:  # Show first 5
                    path = file_obj.get('path', 'Unknown')
                    is_dir = file_obj.get('is_dir', False)
                    file_type = "📁" if is_dir else "📄"
                    print(f"  • {file_type} {path}")
                    
            elif 'statement_id' in parsed_result:
                # SQL execution result
                statement_id = parsed_result.get('statement_id', 'Unknown')
                status = parsed_result.get('status', {}).get('state', 'Unknown')
                print(f"SQL Statement ID: {statement_id}")
                print(f"Status: {status}")
                
                if 'result' in parsed_result and parsed_result['result']:
                    result_data = parsed_result['result']
                    if 'data_array' in result_data:
                        rows = result_data['data_array']
                        print(f"Returned {len(rows)} rows:")
                        if rows:
                            print(f"Sample: {rows[0]}")
                            
            else:
                # Generic JSON result
                print(json.dumps(parsed_result, indent=2)[:500])
                if len(json.dumps(parsed_result)) > 500:
                    print("... (truncated)")
                    
        except json.JSONDecodeError:
            # Not JSON, print as-is
            print(result_text[:500])
            if len(result_text) > 500:
                print("... (truncated)")
    else:
        print("No result returned")


async def demonstrate_cluster_operations(server: DatabricksMCPServer) -> None:
    """Demonstrate cluster-related operations."""
    print_section_header("🖥️  Cluster Operations")
    
    # List all clusters
    print("\n1. Listing all clusters...")
    try:
        result = await server.call_tool('list_clusters', {'params': {}})
        print_result('list_clusters', result)
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Note: We won't create/delete clusters in the example to avoid charges
    print("\n💡 Other available cluster operations:")
    print("  • create_cluster - Create a new cluster")
    print("  • get_cluster - Get specific cluster info")
    print("  • start_cluster - Start a terminated cluster")  
    print("  • terminate_cluster - Terminate a running cluster")


async def demonstrate_job_operations(server: DatabricksMCPServer) -> None:
    """Demonstrate job-related operations."""
    print_section_header("⚙️  Job Operations")
    
    # List all jobs
    print("\n1. Listing all jobs...")
    try:
        result = await server.call_tool('list_jobs', {'params': {}})
        print_result('list_jobs', result)
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n💡 Other available job operations:")
    print("  • run_job - Execute a specific job")


async def demonstrate_notebook_operations(server: DatabricksMCPServer) -> None:
    """Demonstrate notebook-related operations."""
    print_section_header("📓 Notebook Operations")
    
    # List notebooks in root directory
    print("\n1. Listing notebooks in root directory...")
    try:
        result = await server.call_tool('list_notebooks', {
            'params': {'path': '/'}
        })
        print_result('list_notebooks', result)
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n💡 Other available notebook operations:")
    print("  • export_notebook - Export notebook content")


async def demonstrate_dbfs_operations(server: DatabricksMCPServer) -> None:
    """Demonstrate DBFS-related operations."""
    print_section_header("📂 DBFS Operations")
    
    # List files in databricks-datasets
    print("\n1. Listing files in /databricks-datasets...")
    try:
        result = await server.call_tool('list_files', {
            'params': {'dbfs_path': '/databricks-datasets'}
        })
        print_result('list_files', result)
    except Exception as e:
        print(f"❌ Error: {e}")


async def demonstrate_sql_operations(server: DatabricksMCPServer) -> None:
    """Demonstrate SQL execution."""
    print_section_header("🗃️  SQL Operations")
    
    warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID')
    if not warehouse_id:
        print("⚠️  DATABRICKS_WAREHOUSE_ID not set - SQL functionality limited")
        print("💡 Set this environment variable to use SQL features")
        return
    
    # Execute simple SQL query
    print("\n1. Executing simple SQL query...")
    try:
        result = await server.call_tool('execute_sql', {
            'params': {
                'statement': "SELECT 1 as test_number, 'Hello Databricks!' as greeting, CURRENT_TIMESTAMP() as current_time"
            }
        })
        print_result('execute_sql', result)
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Try a table query (may fail if table doesn't exist or no permissions)
    print("\n2. Attempting table query (may fail due to permissions)...")
    try:
        result = await server.call_tool('execute_sql', {
            'params': {
                'statement': "SHOW DATABASES LIMIT 5"
            }
        })
        print_result('execute_sql', result)
    except Exception as e:
        print(f"❌ Error: {e}")
        print("💡 This is normal if you don't have access to query system tables")


async def main() -> None:
    """Main function for the direct usage example."""
    print("🚀 Databricks MCP Server - Direct Usage Example")
    print("=" * 50)
    print("This example shows how to use the MCP server directly")
    print("with the correct async parameter format.\n")
    
    # Check environment variables
    required_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n💡 Please set these variables or create a .env file")
        print("   Example: export DATABRICKS_HOST='https://your-instance.azuredatabricks.net'")
        return
    
    # Show configuration
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    print(f"📋 Configuration:")
    print(f"   - DATABRICKS_HOST: {os.getenv('DATABRICKS_HOST')}")
    print(f"   - DATABRICKS_TOKEN: {'✅ Set' if os.getenv('DATABRICKS_TOKEN') else '❌ Missing'}")
    print(f"   - DATABRICKS_WAREHOUSE_ID: {'✅ Set' if warehouse_id else '⚠️  Not set'}")
    
    try:
        # Create the Databricks MCP server
        logger.info("Initializing Databricks MCP server...")
        server = DatabricksMCPServer()
        
        print("\n✅ Server initialized successfully!")
        
        # Demonstrate different operations
        await demonstrate_cluster_operations(server)
        await demonstrate_job_operations(server)  
        await demonstrate_notebook_operations(server)
        await demonstrate_dbfs_operations(server)
        await demonstrate_sql_operations(server)
        
        # Show parameter format reminder
        print_section_header("📝 Parameter Format Reminder")
        print("All MCP tools require parameters in this format:")
        print("```python")
        print("await server.call_tool('tool_name', {")
        print("    'params': {")
        print("        'parameter1': 'value1',")
        print("        'parameter2': 'value2'")
        print("    }")
        print("})")
        print("```")
        
        print("\n🎉 Direct usage example completed successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        print(f"\n❌ Error: {e}")
        print("\n💡 Troubleshooting tips:")
        print("  1. Verify environment variables are set correctly")
        print("  2. Check Databricks token permissions")
        print("  3. Ensure network connectivity to Databricks")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
