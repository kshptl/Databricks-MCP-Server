# Copyright (c) 2025 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub, you may use it under the AGPL-3.0 terms.
# For commercial terms, contact kushapatel@live.com.

"""
Main entry point for running the databricks-mcp-server package.
This allows the package to be run with 'python -m src' or 'uv run src'.
"""

import asyncio
from src.main import main

if __name__ == "__main__":
    asyncio.run(main())
