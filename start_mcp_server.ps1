# Copyright (c) 2025 Kush Patel
# Dual-licensed: AGPL-3.0-only OR commercial license.
# If you obtained this from GitHub, you may use it under the AGPL-3.0 terms.
# For commercial terms, contact kushapatel@live.com.

#!/usr/bin/env pwsh
# Wrapper script to run the MCP server start script from scripts directory

param(
    [switch]$SkipPrompt
)

# Get the directory of this script
$scriptPath = $MyInvocation.MyCommand.Path
$scriptDir = Split-Path $scriptPath -Parent

# Change to the script directory
Set-Location $scriptDir

# Run the actual server script with any parameters passed to this script
if ($SkipPrompt) {
    & "$scriptDir\scripts\start_mcp_server.ps1" -SkipPrompt
} else {
    & "$scriptDir\scripts\start_mcp_server.ps1"
} 