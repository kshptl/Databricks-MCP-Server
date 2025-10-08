"""
Example usage of the submit_single_run functionality.

This example demonstrates how to submit and run a Databricks notebook
as a one-time job using the MCP server.
"""

import asyncio
import json
import os
from src.api import jobs


async def example_1_simple_notebook_run():
    """Example 1: Run a notebook on an existing cluster."""
    print("\n=== Example 1: Simple notebook run on existing cluster ===")
    
    run_config = {
        "run_name": "Simple Notebook Run Example",
        "tasks": [{
            "task_key": "notebook_task",
            "notebook_task": {
                "notebook_path": "/Users/user@example.com/MyNotebook",
                "base_parameters": {
                    "input_date": "2025-01-06",
                    "environment": "production"
                }
            },
            "existing_cluster_id": "0923-164208-meows279"
        }],
        "timeout_seconds": 3600
    }
    
    try:
        result = await jobs.submit_single_run(run_config)
        run_id = result.get("run_id")
        print(f"✓ Notebook run submitted successfully!")
        print(f"  Run ID: {run_id}")
        return run_id
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


async def example_2_new_cluster_with_libraries():
    """Example 2: Run a notebook on a new cluster with library dependencies."""
    print("\n=== Example 2: Notebook run with new cluster and libraries ===")
    
    run_config = {
        "run_name": "Data Processing Job",
        "tasks": [{
            "task_key": "data_processing",
            "notebook_task": {
                "notebook_path": "/Users/user@example.com/DataProcessing",
                "base_parameters": {
                    "data_source": "s3://my-bucket/data",
                    "output_path": "s3://my-bucket/output"
                }
            },
            "new_cluster": {
                "spark_version": "11.3.x-scala2.12",
                "node_type_id": "i3.xlarge",  # AWS
                # For Azure: "node_type_id": "Standard_D3_v2"
                # For GCP: "node_type_id": "n1-highmem-4"
                "num_workers": 2,
                "spark_conf": {
                    "spark.speculation": "true"
                }
            },
            "libraries": [
                {"pypi": {"package": "pandas==1.5.0"}},
                {"pypi": {"package": "numpy==1.23.0"}},
                {"maven": {"coordinates": "com.databricks:spark-xml_2.12:0.14.0"}}
            ]
        }],
        "timeout_seconds": 7200
    }
    
    try:
        result = await jobs.submit_single_run(run_config)
        run_id = result.get("run_id")
        print(f"✓ Notebook run submitted successfully!")
        print(f"  Run ID: {run_id}")
        return run_id
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


async def example_3_git_based_notebook():
    """Example 3: Run a notebook from a Git repository."""
    print("\n=== Example 3: Run notebook from Git repository ===")
    
    run_config = {
        "run_name": "Git-based Notebook Run",
        "git_source": {
            "git_url": "https://github.com/myorg/myrepo",
            "git_provider": "gitHub",
            "git_branch": "main"
            # Alternatively use: "git_tag": "v1.0.0" or "git_commit": "abc123"
        },
        "tasks": [{
            "task_key": "notebook_from_git",
            "notebook_task": {
                "notebook_path": "notebooks/analysis/DataAnalysis",
                "base_parameters": {
                    "environment": "staging"
                }
            },
            "existing_cluster_id": "0923-164208-meows279"
        }],
        "timeout_seconds": 3600
    }
    
    try:
        result = await jobs.submit_single_run(run_config)
        run_id = result.get("run_id")
        print(f"✓ Notebook run submitted successfully!")
        print(f"  Run ID: {run_id}")
        return run_id
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


async def example_4_multi_task_workflow():
    """Example 4: Run multiple notebooks in a workflow."""
    print("\n=== Example 4: Multi-task notebook workflow ===")
    
    run_config = {
        "run_name": "ETL Workflow",
        "tasks": [
            {
                "task_key": "extract",
                "notebook_task": {
                    "notebook_path": "/Workflows/Extract",
                    "base_parameters": {"source": "database"}
                },
                "existing_cluster_id": "0923-164208-meows279"
            },
            {
                "task_key": "transform",
                "depends_on": [{"task_key": "extract"}],
                "notebook_task": {
                    "notebook_path": "/Workflows/Transform",
                    "base_parameters": {"transformations": "standard"}
                },
                "existing_cluster_id": "0923-164208-meows279"
            },
            {
                "task_key": "load",
                "depends_on": [{"task_key": "transform"}],
                "notebook_task": {
                    "notebook_path": "/Workflows/Load",
                    "base_parameters": {"target": "warehouse"}
                },
                "existing_cluster_id": "0923-164208-meows279"
            }
        ],
        "timeout_seconds": 7200
    }
    
    try:
        result = await jobs.submit_single_run(run_config)
        run_id = result.get("run_id")
        print(f"✓ Multi-task workflow submitted successfully!")
        print(f"  Run ID: {run_id}")
        return run_id
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


async def example_5_wait_for_completion():
    """Example 5: Submit a notebook run and wait for completion."""
    print("\n=== Example 5: Submit and wait for completion ===")
    
    run_config = {
        "run_name": "Quick Analysis",
        "tasks": [{
            "task_key": "analysis",
            "notebook_task": {
                "notebook_path": "/Users/user@example.com/QuickAnalysis",
                "base_parameters": {"quick": "true"}
            },
            "existing_cluster_id": "0923-164208-meows279"
        }],
        "timeout_seconds": 600
    }
    
    try:
        # Submit the run
        result = await jobs.submit_single_run(run_config)
        run_id = result.get("run_id")
        print(f"✓ Notebook run submitted: {run_id}")
        
        # Wait for completion
        print("  Waiting for completion...")
        final_state = await jobs.wait_for_run_completion(
            run_id=run_id,
            poll_interval=10,
            max_wait_seconds=600
        )
        
        # Check result
        state = final_state.get("state", {})
        result_state = state.get("result_state")
        
        if result_state == "SUCCESS":
            print(f"✓ Run completed successfully!")
            
            # Get the output
            output = await jobs.get_run_output(run_id)
            notebook_output = output.get("notebook_output", {})
            result_value = notebook_output.get("result")
            print(f"  Notebook output: {result_value}")
        else:
            print(f"✗ Run failed with state: {result_state}")
            state_message = state.get("state_message", "No error message")
            print(f"  Error: {state_message}")
        
        return run_id
    except TimeoutError:
        print(f"✗ Run did not complete within the timeout period")
        return None
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


async def example_6_with_permissions():
    """Example 6: Submit a notebook run with access control."""
    print("\n=== Example 6: Notebook run with access control ===")
    
    run_config = {
        "run_name": "Shared Analysis",
        "tasks": [{
            "task_key": "analysis",
            "notebook_task": {
                "notebook_path": "/Shared/Analysis",
                "base_parameters": {"shared": "true"}
            },
            "existing_cluster_id": "0923-164208-meows279"
        }],
        "access_control_list": [
            {
                "group_name": "data-scientists",
                "permission_level": "CAN_VIEW"
            },
            {
                "user_name": "admin@example.com",
                "permission_level": "CAN_MANAGE"
            }
        ],
        "timeout_seconds": 3600
    }
    
    try:
        result = await jobs.submit_single_run(run_config)
        run_id = result.get("run_id")
        print(f"✓ Notebook run submitted with permissions!")
        print(f"  Run ID: {run_id}")
        return run_id
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


async def main():
    """Run all examples."""
    print("=" * 70)
    print("Databricks Notebook Run Examples")
    print("=" * 70)
    
    # Check environment variables
    if not os.getenv("DATABRICKS_HOST"):
        print("\n⚠ Warning: DATABRICKS_HOST environment variable not set")
        print("  Please set it before running these examples")
        return
    
    if not os.getenv("DATABRICKS_TOKEN"):
        print("\n⚠ Warning: DATABRICKS_TOKEN environment variable not set")
        print("  Please set it before running these examples")
        return
    
    # Run examples
    # Note: In a real scenario, you'd update these with your actual cluster IDs and notebook paths
    print("\nNote: These examples use placeholder values.")
    print("Update cluster IDs and notebook paths before running.\n")
    
    # Example 1: Simple run
    # await example_1_simple_notebook_run()
    
    # Example 2: New cluster with libraries
    # await example_2_new_cluster_with_libraries()
    
    # Example 3: Git-based notebook
    # await example_3_git_based_notebook()
    
    # Example 4: Multi-task workflow
    # await example_4_multi_task_workflow()
    
    # Example 5: Wait for completion
    # await example_5_wait_for_completion()
    
    # Example 6: With permissions
    # await example_6_with_permissions()
    
    print("\n" + "=" * 70)
    print("Examples completed! Uncomment the examples you want to run.")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
