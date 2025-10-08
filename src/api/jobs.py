"""
API for managing Databricks jobs.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_job(job_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new Databricks job.
    
    Args:
        job_config: Job configuration
        
    Returns:
        Response containing the job ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Creating new job")
    return await make_api_request("POST", "/api/2.0/jobs/create", data=job_config)


async def run_job(job_id: int, notebook_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Run a job now.
    
    Args:
        job_id: ID of the job to run
        notebook_params: Optional parameters for the notebook
        
    Returns:
        Response containing the run ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Running job: {job_id}")
    
    run_params = {"job_id": job_id}
    if notebook_params:
        run_params["notebook_params"] = notebook_params
        
    return await make_api_request("POST", "/api/2.0/jobs/run-now", data=run_params)


async def list_jobs() -> Dict[str, Any]:
    """
    List all jobs.
    
    Returns:
        Response containing a list of jobs
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing all jobs")
    return await make_api_request("GET", "/api/2.0/jobs/list")


async def get_job(job_id: int) -> Dict[str, Any]:
    """
    Get information about a specific job.
    
    Args:
        job_id: ID of the job
        
    Returns:
        Response containing job information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting information for job: {job_id}")
    return await make_api_request("GET", "/api/2.0/jobs/get", params={"job_id": job_id})


async def update_job(job_id: int, new_settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an existing job.
    
    Args:
        job_id: ID of the job to update
        new_settings: New job settings
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating job: {job_id}")
    
    update_data = {
        "job_id": job_id,
        "new_settings": new_settings
    }
    
    return await make_api_request("POST", "/api/2.0/jobs/update", data=update_data)


async def delete_job(job_id: int) -> Dict[str, Any]:
    """
    Delete a job.
    
    Args:
        job_id: ID of the job to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting job: {job_id}")
    return await make_api_request("POST", "/api/2.0/jobs/delete", data={"job_id": job_id})


async def get_run(run_id: int) -> Dict[str, Any]:
    """
    Get information about a specific job run.
    
    Args:
        run_id: ID of the run
        
    Returns:
        Response containing run information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting information for run: {run_id}")
    return await make_api_request("GET", "/api/2.0/jobs/runs/get", params={"run_id": run_id})


async def cancel_run(run_id: int) -> Dict[str, Any]:
    """
    Cancel a job run.
    
    Args:
        run_id: ID of the run to cancel
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Cancelling run: {run_id}")
    return await make_api_request("POST", "/api/2.0/jobs/runs/cancel", data={"run_id": run_id})


async def submit_single_run(run_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Submit a one-time notebook run without creating a job.
    
    This submits a workload directly as a one-time run using the Jobs Submit API.
    The run does not appear in the Jobs UI but can be tracked via the Runs API.
    
    Args:
        run_config: Run configuration containing:
            - tasks: List of tasks to run (typically one notebook task)
            - run_name: Optional name for the run
            - timeout_seconds: Optional timeout
            - git_source: Optional git source configuration
            - notebook_params: Optional parameters for the notebook
            - new_cluster or existing_cluster_id: Cluster configuration
            - libraries: Optional list of libraries to install
            - access_control_list: Optional ACL for permissions
            
    Returns:
        Response containing the run_id
        
    Raises:
        DatabricksAPIError: If the API request fails
        
    Example:
        run_config = {
            "run_name": "My notebook run",
            "tasks": [{
                "task_key": "notebook_task",
                "notebook_task": {
                    "notebook_path": "/Users/user@example.com/MyNotebook",
                    "base_parameters": {"param1": "value1"}
                },
                "existing_cluster_id": "0923-164208-meows279"
            }]
        }
    """
    logger.info(f"Submitting single run: {run_config.get('run_name', 'Untitled')}")
    return await make_api_request("POST", "/api/2.1/jobs/runs/submit", data=run_config)


async def get_run_output(run_id: int) -> Dict[str, Any]:
    """
    Get the output from a completed job run.
    
    Args:
        run_id: ID of the run
        
    Returns:
        Response containing:
            - notebook_output: Output from the notebook (if notebook task)
            - error: Error message if the run failed
            - metadata: Metadata about the run
            
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting output for run: {run_id}")
    return await make_api_request("GET", "/api/2.0/jobs/runs/get-output", params={"run_id": run_id})


async def wait_for_run_completion(
    run_id: int, 
    poll_interval: int = 10,
    max_wait_seconds: int = 3600
) -> Dict[str, Any]:
    """
    Wait for a job run to complete by polling its status.
    
    Args:
        run_id: ID of the run to wait for
        poll_interval: Seconds to wait between status checks (default: 10)
        max_wait_seconds: Maximum seconds to wait before timing out (default: 3600)
        
    Returns:
        Final run information including state and result
        
    Raises:
        DatabricksAPIError: If the API request fails
        TimeoutError: If the run doesn't complete within max_wait_seconds
    """
    import asyncio
    
    logger.info(f"Waiting for run {run_id} to complete (max wait: {max_wait_seconds}s)")
    
    elapsed = 0
    while elapsed < max_wait_seconds:
        run_info = await get_run(run_id)
        state = run_info.get("state", {})
        life_cycle_state = state.get("life_cycle_state")
        
        # Check if run has completed
        if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            result_state = state.get("result_state")
            logger.info(f"Run {run_id} completed with state: {life_cycle_state}, result: {result_state}")
            return run_info
        
        # Still running, wait and check again
        logger.debug(f"Run {run_id} state: {life_cycle_state}, waiting {poll_interval}s...")
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
    
    # Timeout reached
    raise TimeoutError(f"Run {run_id} did not complete within {max_wait_seconds} seconds")
