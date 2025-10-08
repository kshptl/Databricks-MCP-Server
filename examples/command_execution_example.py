# Copyright (c) 2025 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub, you may use it under the AGPL-3.0 terms.
# For commercial terms, contact kushapatel@live.com.

"""
Example demonstrating command execution on Databricks clusters.

This example shows both simple one-off command execution and
sequential command execution where later commands depend on
earlier commands' state.
"""

import asyncio
import json
import os
from typing import Dict, Any

from src.api import command_execution, clusters


async def simple_command_example(cluster_id: str):
    """
    Execute a simple one-off command using the convenience function.
    
    This is useful for quick operations that don't need persistent state.
    """
    print("\n=== Simple Command Execution ===")
    
    try:
        # Execute a simple Python command
        result = await command_execution.execute_command_simple(
            cluster_id=cluster_id,
            command="print('Hello from Databricks!'); result = 1 + 1; print(f'1 + 1 = {result}')",
            language="python",
            max_wait_seconds=60
        )
        
        print(f"Command Status: {result.get('status')}")
        print(f"Results: {json.dumps(result.get('results'), indent=2)}")
        
    except Exception as e:
        print(f"Error executing simple command: {e}")


async def sequential_commands_example(cluster_id: str):
    """
    Execute a series of dependent commands that build on each other's state.
    
    This demonstrates the key use case: maintaining context across multiple
    commands, allowing you to build up state incrementally.
    """
    print("\n=== Sequential Command Execution ===")
    
    context_id = None
    
    try:
        # Step 1: Create an execution context
        print("\n1. Creating execution context...")
        context_response = await command_execution.create_execution_context(
            cluster_id=cluster_id,
            language="python"
        )
        context_id = context_response.get("id")
        print(f"   Context created: {context_id}")
        
        # Step 2: Execute first command - create a DataFrame
        print("\n2. Creating a DataFrame...")
        cmd1_response = await command_execution.execute_command(
            cluster_id=cluster_id,
            context_id=context_id,
            command="""
# Create a simple DataFrame
data = [
    ("Alice", 25, "Engineering"),
    ("Bob", 30, "Sales"),
    ("Charlie", 35, "Engineering"),
    ("Diana", 28, "Marketing")
]

df = spark.createDataFrame(data, ["name", "age", "department"])
df.createOrReplaceTempView("employees")
print(f"Created DataFrame with {df.count()} rows")
""",
            language="python"
        )
        cmd1_id = cmd1_response.get("id")
        
        # Wait for command 1 to complete
        cmd1_status = await wait_for_command(cluster_id, context_id, cmd1_id)
        print(f"   Command 1 result: {cmd1_status.get('results', {}).get('data', 'N/A')}")
        
        # Step 3: Execute second command - filter the DataFrame
        print("\n3. Filtering DataFrame (using state from previous command)...")
        cmd2_response = await command_execution.execute_command(
            cluster_id=cluster_id,
            context_id=context_id,
            command="""
# Filter the DataFrame created in the previous command
engineering_df = df.filter(df.department == "Engineering")
count = engineering_df.count()
print(f"Engineering department has {count} employees")

# Show the results
engineering_df.show()
""",
            language="python"
        )
        cmd2_id = cmd2_response.get("id")
        
        # Wait for command 2 to complete
        cmd2_status = await wait_for_command(cluster_id, context_id, cmd2_id)
        print(f"   Command 2 completed")
        
        # Step 4: Execute SQL query on the temp view
        print("\n4. Executing SQL query (using temp view from step 2)...")
        cmd3_response = await command_execution.execute_command(
            cluster_id=cluster_id,
            context_id=context_id,
            command="""
# Query the temp view created earlier
result = spark.sql(\"\"\"
    SELECT department, AVG(age) as avg_age, COUNT(*) as count
    FROM employees
    GROUP BY department
    ORDER BY avg_age DESC
\"\"\")

print("Average age by department:")
result.show()
""",
            language="python"
        )
        cmd3_id = cmd3_response.get("id")
        
        # Wait for command 3 to complete
        cmd3_status = await wait_for_command(cluster_id, context_id, cmd3_id)
        print(f"   Command 3 completed")
        
        # Step 5: Execute final aggregation
        print("\n5. Final calculation using all previous state...")
        cmd4_response = await command_execution.execute_command(
            cluster_id=cluster_id,
            context_id=context_id,
            command="""
# Access variables from all previous commands
total_count = df.count()
engineering_count = engineering_df.count()
percentage = (engineering_count / total_count) * 100

print(f"Summary:")
print(f"  Total employees: {total_count}")
print(f"  Engineering employees: {engineering_count}")
print(f"  Engineering percentage: {percentage:.1f}%")
""",
            language="python"
        )
        cmd4_id = cmd4_response.get("id")
        
        # Wait for command 4 to complete
        cmd4_status = await wait_for_command(cluster_id, context_id, cmd4_id)
        print(f"   Command 4 result: {cmd4_status.get('results', {}).get('data', 'N/A')}")
        
        print("\n✓ All sequential commands completed successfully!")
        
    except Exception as e:
        print(f"Error in sequential execution: {e}")
        
    finally:
        # Step 6: Clean up - destroy the execution context
        if context_id:
            print("\n6. Cleaning up execution context...")
            try:
                await command_execution.destroy_execution_context(cluster_id, context_id)
                print(f"   Context {context_id} destroyed")
            except Exception as e:
                print(f"   Warning: Failed to destroy context: {e}")


async def wait_for_command(
    cluster_id: str,
    context_id: str,
    command_id: str,
    max_wait_seconds: int = 60,
    poll_interval: int = 2
) -> Dict[str, Any]:
    """
    Wait for a command to complete and return its status.
    
    Args:
        cluster_id: Cluster ID
        context_id: Context ID
        command_id: Command ID to wait for
        max_wait_seconds: Maximum time to wait
        poll_interval: Time between status checks
        
    Returns:
        Final command status
        
    Raises:
        Exception: If command fails or times out
    """
    elapsed = 0
    
    while elapsed < max_wait_seconds:
        status_response = await command_execution.get_command_status(
            cluster_id, context_id, command_id
        )
        status = status_response.get("status")
        
        if status == "Finished":
            return status_response
        elif status == "Error":
            error_msg = status_response.get("results", {}).get("cause", "Unknown error")
            raise Exception(f"Command failed: {error_msg}")
        elif status == "Cancelled":
            raise Exception("Command was cancelled")
        
        # Still running, wait and retry
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
    
    raise Exception(f"Command timed out after {max_wait_seconds} seconds")


async def sql_command_example(cluster_id: str):
    """
    Execute SQL commands in a context.
    """
    print("\n=== SQL Command Execution ===")
    
    context_id = None
    
    try:
        # Create context for SQL
        print("\n1. Creating SQL execution context...")
        context_response = await command_execution.create_execution_context(
            cluster_id=cluster_id,
            language="sql"
        )
        context_id = context_response.get("id")
        print(f"   Context created: {context_id}")
        
        # Execute SQL command
        print("\n2. Executing SQL query...")
        cmd_response = await command_execution.execute_command(
            cluster_id=cluster_id,
            context_id=context_id,
            command="SELECT 'Hello from SQL!' as message, CURRENT_TIMESTAMP() as timestamp",
            language="sql"
        )
        cmd_id = cmd_response.get("id")
        
        # Wait for completion
        result = await wait_for_command(cluster_id, context_id, cmd_id)
        print(f"   Query result: {json.dumps(result.get('results'), indent=2)}")
        
    except Exception as e:
        print(f"Error executing SQL: {e}")
        
    finally:
        if context_id:
            print("\n3. Cleaning up SQL context...")
            try:
                await command_execution.destroy_execution_context(cluster_id, context_id)
                print(f"   Context {context_id} destroyed")
            except Exception as e:
                print(f"   Warning: Failed to destroy context: {e}")


async def main():
    """Main entry point for the example."""
    # Get cluster ID from environment or command line
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
    
    if not cluster_id:
        print("Error: DATABRICKS_CLUSTER_ID environment variable is not set")
        print("\nUsage:")
        print("  export DATABRICKS_CLUSTER_ID=your-cluster-id")
        print("  python examples/command_execution_example.py")
        return
    
    print(f"Using cluster: {cluster_id}")
    
    # Check if cluster is running
    try:
        cluster_info = await clusters.get_cluster(cluster_id)
        state = cluster_info.get("state")
        print(f"Cluster state: {state}")
        
        if state != "RUNNING":
            print(f"\nWarning: Cluster is not in RUNNING state. Current state: {state}")
            print("Please start the cluster before running commands.")
            return
    except Exception as e:
        print(f"Error checking cluster status: {e}")
        return
    
    # Run examples
    try:
        # Example 1: Simple command execution
        await simple_command_example(cluster_id)
        
        # Example 2: Sequential commands with shared state
        await sequential_commands_example(cluster_id)
        
        # Example 3: SQL commands
        await sql_command_example(cluster_id)
        
        print("\n" + "="*50)
        print("All examples completed successfully!")
        print("="*50)
        
    except Exception as e:
        print(f"\nError running examples: {e}")


if __name__ == "__main__":
    asyncio.run(main())
