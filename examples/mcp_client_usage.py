# Copyright (c) 2025 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub, you may use it under the AGPL-3.0 terms.
# For commercial terms, contact kushapatel@live.com.

"""
Example of using the MCP client with the Databricks MCP server.

This example shows how to use the MCP client to connect to the Databricks MCP server
and call its tools through the MCP protocol with the correct parameter format.

IMPORTANT: All MCP tools require parameters wrapped in a 'params' object!
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.session import ClientSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def example_tool_calls(session: ClientSession):
    """Demonstrate various tool calls with correct parameter format."""
    
    print("\n🔧 Example Tool Calls")
    print("=" * 50)
    
    # Example 1: List clusters (no parameters needed)
    print("\n1. Listing Databricks clusters...")
    try:
        result = await session.call_tool("list_clusters", {"params": {}})
        print("✅ Success! Found clusters:")
        # Parse and display results
        if result and len(result) > 0:
            clusters_data = json.loads(result[0]["text"] if isinstance(result[0], dict) else result[0].text)
            if "clusters" in clusters_data:
                for cluster in clusters_data["clusters"][:3]:  # Show first 3
                    print(f"  - {cluster.get('cluster_name', 'Unnamed')}: {cluster.get('state', 'Unknown')}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Example 2: List notebooks in root directory
    print("\n2. Listing notebooks in root directory...")
    try:
        result = await session.call_tool("list_notebooks", {
            "params": {
                "path": "/"
            }
        })
        print("✅ Success! Found notebooks/directories:")
        # Parse and display results
        if result and len(result) > 0:
            notebooks_data = json.loads(result[0]["text"] if isinstance(result[0], dict) else result[0].text)
            if "objects" in notebooks_data:
                for obj in notebooks_data["objects"][:5]:  # Show first 5
                    obj_type = obj.get('object_type', 'UNKNOWN')
                    path = obj.get('path', 'Unknown path')
                    print(f"  - [{obj_type}] {path}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Example 3: Execute SQL query (requires warehouse)
    print("\n3. Executing SQL query...")
    try:
        # Simple test query
        result = await session.call_tool("execute_sql", {
            "params": {
                "statement": "SELECT 1 as test_value, 'Hello from Databricks!' as message"
            }
        })
        print("✅ Success! SQL executed:")
        # Parse and display results
        if result and len(result) > 0:
            sql_data = json.loads(result[0]["text"] if isinstance(result[0], dict) else result[0].text)
            if "result" in sql_data and "data_array" in sql_data["result"]:
                rows = sql_data["result"]["data_array"]
                print(f"  - Query returned {len(rows)} row(s)")
                if rows:
                    print(f"  - Sample data: {rows[0]}")
            elif "statement_id" in sql_data:
                print(f"  - Statement ID: {sql_data['statement_id']}")
                print(f"  - Status: {sql_data.get('status', {}).get('state', 'Unknown')}")
    except Exception as e:
        print(f"❌ Error: {e}")
        if "warehouse" in str(e).lower():
            print("  💡 Tip: Ensure DATABRICKS_WAREHOUSE_ID environment variable is set")
    
    # Example 4: List DBFS files
    print("\n4. Listing DBFS files...")
    try:
        result = await session.call_tool("list_files", {
            "params": {
                "dbfs_path": "/databricks-datasets"
            }
        })
        print("✅ Success! Found DBFS files:")
        # Parse and display results
        if result and len(result) > 0:
            files_data = json.loads(result[0]["text"] if isinstance(result[0], dict) else result[0].text)
            if "files" in files_data:
                for file_obj in files_data["files"][:5]:  # Show first 5
                    path = file_obj.get('path', 'Unknown')
                    is_dir = file_obj.get('is_dir', False)
                    file_type = "DIR" if is_dir else "FILE"
                    print(f"  - [{file_type}] {path}")
    except Exception as e:
        print(f"❌ Error: {e}")


async def interactive_tool_selector(session: ClientSession):
    """Interactive tool selection with proper parameter format guidance."""
    
    # List available tools
    tools_response = await session.list_tools()
    tools = tools_response.tools
    
    print(f"\n🛠️  Available MCP Tools ({len(tools)} total)")
    print("=" * 50)
    for i, tool in enumerate(tools, 1):
        print(f"{i:2d}. {tool.name}")
        print(f"    {tool.description}")
    
    while True:
        print(f"\n📝 Parameter Format Rule:")
        print("   All tools require parameters wrapped in a 'params' object!")
        print("   Example: {'params': {'parameter_name': 'value'}}")
        
        print(f"\nSelect a tool to run:")
        print("  - Enter tool number (1-{})".format(len(tools)))
        print("  - Enter tool name directly")
        print("  - Type 'examples' to see example calls")
        print("  - Type 'quit' to exit")
        
        choice = input("\n> ").strip()
        
        if choice.lower() == 'quit':
            break
        elif choice.lower() == 'examples':
            await example_tool_calls(session)
            continue
        
        # Find the selected tool
        selected_tool = None
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(tools):
                selected_tool = tools[idx]
        else:
            for tool in tools:
                if tool.name.lower() == choice.lower():
                    selected_tool = tool
                    break
        
        if not selected_tool:
            print("❌ Invalid choice. Please try again.")
            continue
        
        # Show tool info and get parameters
        print(f"\n🎯 Selected: {selected_tool.name}")
        print(f"Description: {selected_tool.description}")
        
        # Provide parameter examples based on tool name
        if selected_tool.name == "list_clusters":
            print("Example: {} (no parameters needed)")
        elif selected_tool.name == "get_cluster":
            print("Example: {'params': {'cluster_id': 'your-cluster-id'}}")
        elif selected_tool.name == "create_cluster":
            print("Example: {'params': {'cluster_name': 'my-cluster', 'spark_version': '11.3.x-scala2.12', 'node_type_id': 'i3.xlarge', 'num_workers': 2}}")
        elif selected_tool.name == "execute_sql":
            print("Example: {'params': {'statement': 'SELECT COUNT(*) FROM my_table'}}")
        elif selected_tool.name == "list_notebooks":
            print("Example: {'params': {'path': '/'}}")
        elif selected_tool.name == "list_files":
            print("Example: {'params': {'dbfs_path': '/databricks-datasets'}}")
        else:
            print("Example: {'params': {'parameter_name': 'value'}}")
        
        print("\nEnter parameters as JSON (or press Enter for empty params):")
        params_str = input("> ").strip()
        
        try:
            if params_str:
                # If user provides raw parameters, wrap them in 'params'
                user_params = json.loads(params_str)
                if 'params' not in user_params:
                    # Wrap in params object
                    params = {'params': user_params}
                    print(f"📦 Auto-wrapped in 'params': {json.dumps(params)}")
                else:
                    params = user_params
            else:
                params = {'params': {}}
            
            print(f"\n🚀 Calling {selected_tool.name}...")
            result = await session.call_tool(selected_tool.name, params)
            
            print("\n✅ Result:")
            print("=" * 30)
            if result and len(result) > 0:
                # Handle both dict and object formats
                result_text = result[0]["text"] if isinstance(result[0], dict) else result[0].text
                try:
                    # Try to parse as JSON for pretty printing
                    parsed_result = json.loads(result_text)
                    print(json.dumps(parsed_result, indent=2))
                except:
                    # If not JSON, print as-is
                    print(result_text)
            else:
                print("No result returned")
                
        except json.JSONDecodeError:
            print("❌ Invalid JSON format. Please try again.")
        except Exception as e:
            print(f"❌ Error calling tool: {e}")
            if "400" in str(e):
                print("💡 Tip: Check parameter format - all params must be wrapped in 'params' object")


async def connect_to_server():
    """Connect to the Databricks MCP server."""
    logger.info("Connecting to Databricks MCP server...")
    
    # Create parameters for connecting to the server
    # Use Python directly since we have the virtual environment set up
    params = StdioServerParameters(
        command=".venv/bin/python",  # Use venv Python
        args=["-m", "src.main"],     # Run the main module
        env=os.environ.copy()        # Pass all environment variables
    )
    
    logger.info("Launching server process...")
    
    async with stdio_client(params) as (recv, send):
        logger.info("Server launched, creating session...")
        session = ClientSession(recv, send)
        
        logger.info("Initializing session...")
        await session.initialize()
        
        print("✅ Connected to Databricks MCP Server!")
        
        # Run interactive tool selector
        await interactive_tool_selector(session)


async def main():
    """Run the MCP client example."""
    print("🚀 Databricks MCP Server - Client Usage Example")
    print("=" * 55)
    print("This example demonstrates how to use the Databricks MCP server")
    print("with the correct parameter format that AI agents need.")
    print()
    
    # Check environment
    required_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n💡 Please set these variables or create a .env file")
        return 1
    
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    print(f"📋 Configuration:")
    print(f"   - DATABRICKS_HOST: {os.getenv('DATABRICKS_HOST')}")
    print(f"   - DATABRICKS_TOKEN: {'✅ Set' if os.getenv('DATABRICKS_TOKEN') else '❌ Missing'}")
    print(f"   - DATABRICKS_WAREHOUSE_ID: {'✅ Set' if warehouse_id else '⚠️  Not set (SQL tools will need manual warehouse_id)'}")
    
    try:
        await connect_to_server()
        print("\n👋 Goodbye!")
        return 0
    except KeyboardInterrupt:
        print("\n👋 Interrupted by user")
        return 0
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        print(f"\n❌ Failed to connect: {e}")
        print("\n💡 Troubleshooting:")
        print("   1. Ensure the MCP server dependencies are installed: uv pip install -e .")
        print("   2. Check that environment variables are set correctly")
        print("   3. Verify Databricks credentials have appropriate permissions")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
