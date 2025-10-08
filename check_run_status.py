"""
Check the status of the submitted notebook run.
"""

import asyncio
from src.api import jobs


async def check_run():
    """Check the status of run 1065548238370157."""
    
    run_id = 1065548238370157
    
    print(f"Checking status of run {run_id}...")
    print("=" * 70)
    
    try:
        # Get run info
        run_info = await jobs.get_run(run_id)
        
        state = run_info.get("state", {})
        life_cycle_state = state.get("life_cycle_state")
        result_state = state.get("result_state")
        state_message = state.get("state_message", "")
        
        print(f"\nRun Status:")
        print(f"  Life Cycle State: {life_cycle_state}")
        print(f"  Result State: {result_state}")
        print(f"  State Message: {state_message}")
        
        # If completed, try to get output
        if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            print(f"\n✓ Run has completed!")
            
            if result_state == "SUCCESS":
                print(f"\n  Attempting to get run output...")
                try:
                    output = await jobs.get_run_output(run_id)
                    notebook_output = output.get("notebook_output", {})
                    result_value = notebook_output.get("result")
                    
                    print(f"\n  Notebook Output:")
                    print(f"  {result_value}")
                except Exception as e:
                    print(f"  Could not retrieve output: {e}")
            else:
                print(f"\n  Run did not complete successfully: {result_state}")
                print(f"  Message: {state_message}")
        else:
            print(f"\n  Run is still in progress: {life_cycle_state}")
        
    except Exception as e:
        print(f"\n✗ Error checking run status: {e}")


if __name__ == "__main__":
    asyncio.run(check_run())
