# Copyright (c) 2025 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub, you may use it under the AGPL-3.0 terms.
# For commercial terms, contact kushapatel@live.com.

"""
Main entry point for running the server module directly.
This allows the module to be run with 'python -m src.server' or 'uv run src.server'.
"""

import asyncio
from src.server.databricks_mcp_server import main

if __name__ == "__main__":
    asyncio.run(main())
