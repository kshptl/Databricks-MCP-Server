"""
Test script to run the 02b2 notebook on cluster 0306-140430-cqe49wju
"""

import asyncio
import json
from src.api import jobs


async def test_run_02b2_notebook():
    """Test running the 02b2 notebook."""
    
    # Possible notebook paths - we'll need to find the correct one
    possible_paths = [
        "/Users/patekus4@merck.com/02b2",
        "/Workspace/Users/patekus4@merck.com/02b2",
        "/02b2",
        "/Users/patekus4/02b2"
    ]
    
    cluster_id = "0306-140430-cqe49wju"
    
    print(f"Testing notebook run on cluster: {cluster_id}")
    print("=" * 70)
    
    # Try to submit the notebook run
    run_config = {
        "run_name": "Test 02b2 Notebook Run",
        "tasks": [{
            "task_key": "test_02b2",
            "notebook_task": {
                "notebook_path": possible_paths[0],  # Start with first path
                "base_parameters": {}
            },
            "existing_cluster_id": cluster_id
        }],
        "timeout_seconds": 3600
    }
    
    try:
        print(f"\n1. Submitting notebook run...")
        print(f"   Notebook path: {possible_paths[0]}")
        print(f"   Cluster ID: {cluster_id}")
        
        result = await jobs.submit_single_run(run_config)
        run_id = result.get("run_id")
        
        print(f"✓ Notebook run submitted successfully!")
        print(f"  Run ID: {run_id}")
        
        # Get initial status
        print(f"\n2. Checking run status...")
        run_info = await jobs.get_run(run_id)
        state = run_info.get("state", {})
        life_cycle_state = state.get("life_cycle_state")
        
        print(f"  Initial state: {life_cycle_state}")
        
        # Wait for a moment to see if it starts
        print(f"\n3. Waiting 10 seconds for run to start...")
