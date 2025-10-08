# Copyright (c) 2025 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub, you may use it under the AGPL-3.0 terms.
# For commercial terms, contact kushapatel@live.com.

#!/bin/bash
# Wrapper script to run the MCP server start script from scripts directory

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Change to the script directory
cd "$SCRIPT_DIR"

# Run the actual server script
"$SCRIPT_DIR/scripts/start_mcp_server.sh" 