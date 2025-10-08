# Copyright (c) 2025 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub, you may use it under the AGPL-3.0 terms.
# For commercial terms, contact kushapatel@live.com.

"""
Simple script to show clusters from Databricks
"""

import asyncio
import json
import sys
from src.api import clusters

async def show_all_clusters():
    """Show all clusters in the Databricks workspace."""
    print("Fetching clusters from Databricks...")
    try:
        result = await clusters.list_clusters()
        print("\nClusters found:")
        print(json.dumps(result, indent=2))
        return result
    except Exception as e:
        print(f"Error listing clusters: {e}")
        return None

if __name__ == "__main__":
    asyncio.run(show_all_clusters()) 