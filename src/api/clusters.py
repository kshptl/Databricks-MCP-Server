"""
Databricks Cluster Management API Client.

This module provides a comprehensive interface for managing Databricks clusters,
including creation, termination, resizing, and monitoring operations.

Author: Kush Patel
"""

import logging
from typing import Any, Dict, List, Optional
from enum import Enum

from src.core.utils import DatabricksAPIError, make_api_request

# Configure module logger
logger = logging.getLogger(__name__)


class ClusterState(str, Enum):
    """
    Enumeration of possible Databricks cluster states.
    
    These states represent the lifecycle of a cluster from creation to termination.
    """
    PENDING = "PENDING"           # Cluster is being created
    RUNNING = "RUNNING"           # Cluster is operational
    RESTARTING = "RESTARTING"     # Cluster is restarting
    RESIZING = "RESIZING"         # Cluster is being resized
    TERMINATING = "TERMINATING"   # Cluster is shutting down
    TERMINATED = "TERMINATED"     # Cluster has been terminated
    ERROR = "ERROR"               # Cluster encountered an error
    UNKNOWN = "UNKNOWN"           # State cannot be determined


async def create_cluster(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new Databricks compute cluster.
    
    This function provisions a new cluster with the specified configuration.
    The cluster will be in PENDING state initially and transition to RUNNING
    once ready. Auto-termination can be configured to save costs.
    
    Args:
        cluster_config: Complete cluster configuration dictionary containing:
            - cluster_name (str): Human-readable cluster name
            - spark_version (str): Databricks Runtime version
            - node_type_id (str): Instance type for nodes
            - num_workers (int): Number of worker nodes
            - autotermination_minutes (int, optional): Auto-termination timeout
            - spark_conf (dict, optional): Spark configuration overrides
            - custom_tags (dict, optional): Resource tags
            
    Returns:
        Dictionary containing the newly created cluster_id
        
    Raises:
        DatabricksAPIError: If cluster creation fails or config is invalid
        
    Example:
        >>> config = {
        ...     "cluster_name": "analytics-cluster",
        ...     "spark_version": "11.3.x-scala2.12",
        ...     "node_type_id": "i3.xlarge",
        ...     "num_workers": 2
        ... }
        >>> result = await create_cluster(config)
        >>> cluster_id = result["cluster_id"]
    """
    # Log cluster creation for audit trail
    cluster_name = cluster_config.get("cluster_name", "unnamed")
    logger.info(f"Creating new cluster: {cluster_name}")
    
    # Make API request with configuration
    return await make_api_request(
        "POST", 
        "/api/2.0/clusters/create", 
        data=cluster_config
    )


async def terminate_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Permanently terminate a Databricks cluster.
    
    This operation stops the cluster and releases all associated compute resources.
    Terminated clusters can be restarted using start_cluster(). This is useful
    for cost savings when compute is not needed.
    
    Args:
        cluster_id: Unique identifier of the cluster to terminate
        
    Returns:
        Empty dictionary on successful termination
        
    Raises:
        DatabricksAPIError: If cluster_id is invalid or termination fails
        
    Note:
        Termination is asynchronous. Check cluster state with get_cluster()
        to confirm the cluster has reached TERMINATED state.
    """
    logger.info(f"Terminating cluster: {cluster_id}")
    
    # Note: API endpoint is 'delete' but operation is 'terminate'
    # Cluster metadata is retained and cluster can be restarted
    return await make_api_request(
        "POST", 
        "/api/2.0/clusters/delete", 
        data={"cluster_id": cluster_id}
    )


async def list_clusters() -> Dict[str, Any]:
    """
    Retrieve a list of all clusters in the workspace.
    
    Returns both active and terminated clusters. Use the state field
    to filter clusters by their current status.
    
    Returns:
        Dictionary containing:
            - clusters (list): Array of cluster objects with details
        
    Raises:
        DatabricksAPIError: If the API request fails
        
    Example:
        >>> result = await list_clusters()
        >>> for cluster in result.get("clusters", []):
        ...     print(f"{cluster['cluster_name']}: {cluster['state']}")
    """
    logger.info("Listing all clusters in workspace")
    
    # GET request with no parameters returns all clusters
    return await make_api_request("GET", "/api/2.0/clusters/list")


async def get_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Retrieve detailed information about a specific cluster.
    
    Provides comprehensive cluster metadata including configuration,
    current state, runtime information, and resource utilization.
    
    Args:
        cluster_id: Unique identifier of the cluster to query
        
    Returns:
        Dictionary containing complete cluster information:
            - cluster_id (str): Cluster identifier
            - cluster_name (str): Cluster name
            - state (str): Current cluster state (see ClusterState enum)
            - state_message (str): Detailed state description
            - spark_version (str): Databricks Runtime version
            - node_type_id (str): Instance type
            - num_workers (int): Number of worker nodes
            - driver (dict): Driver node information
            - executors (list): Worker node information
            - spark_conf (dict): Spark configuration
            - And many more fields...
            
    Raises:
        DatabricksAPIError: If cluster_id not found or request fails
        
    Example:
        >>> info = await get_cluster("1234-567890-abc123")
        >>> print(f"State: {info['state']}")
        >>> print(f"Workers: {info['num_workers']}")
    """
    logger.info(f"Retrieving cluster information: {cluster_id}")
    
    # Use query parameter for GET request
    return await make_api_request(
        "GET", 
        "/api/2.0/clusters/get", 
        params={"cluster_id": cluster_id}
    )


async def start_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Start a terminated Databricks cluster.
    
    Args:
        cluster_id: ID of the cluster to start
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Starting cluster: {cluster_id}")
    return await make_api_request("POST", "/api/2.0/clusters/start", data={"cluster_id": cluster_id})


async def resize_cluster(cluster_id: str, num_workers: int) -> Dict[str, Any]:
    """
    Resize a cluster by changing the number of workers.
    
    Args:
        cluster_id: ID of the cluster to resize
        num_workers: New number of workers
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Resizing cluster {cluster_id} to {num_workers} workers")
    return await make_api_request(
        "POST", 
        "/api/2.0/clusters/resize", 
        data={"cluster_id": cluster_id, "num_workers": num_workers}
    )


async def restart_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Restart a Databricks cluster.
    
    Args:
        cluster_id: ID of the cluster to restart
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Restarting cluster: {cluster_id}")
    return await make_api_request("POST", "/api/2.0/clusters/restart", data={"cluster_id": cluster_id})
