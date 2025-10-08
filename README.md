# Databricks MCP Toolkit

> Enterprise-grade Model Context Protocol (MCP) integration toolkit for Databricks, enabling seamless AI agent interactions with your data platform.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![MCP Protocol](https://img.shields.io/badge/MCP-1.0-green.svg)](https://modelcontextprotocol.io/)

## Overview

The Databricks MCP Toolkit provides a comprehensive implementation of the Model Context Protocol for Databricks, enabling AI agents and LLM-powered applications to programmatically interact with Databricks services. Built with enterprise requirements in mind, this toolkit offers robust cluster management, job orchestration, notebook execution, and SQL query capabilities through a standardized MCP interface.

### Key Capabilities

- **Cluster Operations** - Full lifecycle management of Databricks compute resources
- **Job Orchestration** - Submit, monitor, and control Databricks job executions
- **Notebook Integration** - Execute notebooks with parameter passing and output retrieval
- **SQL Execution** - Run SQL queries against Databricks SQL warehouses
- **Command Execution** - Execute Python, Scala, SQL, or R code directly on clusters
- **File System Access** - Browse and manage DBFS storage
- **Real-time Monitoring** - Track execution status with automatic polling

## Prerequisites

- **Python**: 3.10 or higher
- **Databricks Access**: Valid workspace URL and personal access token
- **SQL Warehouse**: (Optional) For SQL execution capabilities
- **MCP Client**: Claude Desktop, Cline, or any MCP-compatible client

## Installation

### Using pip (Standard Method)

```bash
# Clone the repository
git clone https://github.com/kshptl/databricks-mcp-toolkit.git
cd databricks-mcp-toolkit

# Create and activate virtual environment
python -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # Unix/Linux/macOS
.venv\Scripts\activate     # Windows

# Install the toolkit
pip install -e .
```

### Using conda

```bash
# Clone the repository
git clone https://github.com/kshptl/databricks-mcp-toolkit.git
cd databricks-mcp-toolkit

# Create conda environment
conda create -n databricks-mcp python=3.10
conda activate databricks-mcp

# Install the toolkit
pip install -e .
```

## Configuration

Create a `.env` file in the project root with your Databricks credentials:

```bash
# Required
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=dapi1234567890abcdef

# Optional (for SQL functionality)
DATABRICKS_WAREHOUSE_ID=1234567890123456
```

### Obtaining Credentials

1. **Workspace URL**: Your Databricks workspace URL (e.g., `https://adb-123456789.azuredatabricks.net`)
2. **Access Token**: Generate from User Settings → Developer → Access Tokens
3. **Warehouse ID**: Find in SQL → SQL Warehouses → Connection Details

## Using with MCP Clients

This toolkit is designed to work with MCP-compatible clients like Claude Desktop, Cline, and other AI assistants.

### Claude Desktop Configuration

Add this to your Claude Desktop configuration file:

**macOS/Linux**: `~/Library/Application Support/Claude/claude_desktop_config.json`  
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "databricks": {
      "command": "python",
      "args": ["-m", "src.main"],
      "cwd": "/absolute/path/to/databricks-mcp-toolkit",
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.azuredatabricks.net",
        "DATABRICKS_TOKEN": "dapi1234567890abcdef",
        "DATABRICKS_WAREHOUSE_ID": "your_warehouse_id"
      }
    }
  }
}
```

**Windows Example**:
```json
{
  "mcpServers": {
    "databricks": {
      "command": "python",
      "args": ["-m", "src.main"],
      "cwd": "C:\\Users\\YourName\\databricks-mcp-toolkit",
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.azuredatabricks.net",
        "DATABRICKS_TOKEN": "dapi1234567890abcdef",
        "DATABRICKS_WAREHOUSE_ID": "your_warehouse_id"
      }
    }
  }
}
```

### Cline Configuration

In Cline settings, add the MCP server:

1. Open Cline settings
2. Navigate to MCP Servers section
3. Add new server with these settings:
   - **Name**: databricks
   - **Command**: `python`
   - **Args**: `-m src.main`
   - **Working Directory**: Path to databricks-mcp-toolkit
   - **Environment Variables**:
     - `DATABRICKS_HOST`: Your workspace URL
     - `DATABRICKS_TOKEN`: Your access token
     - `DATABRICKS_WAREHOUSE_ID`: Your warehouse ID (optional)

### Verifying the Connection

After configuration, restart your MCP client and test the connection:

**In Claude Desktop or Cline, try:**
```
"List my Databricks clusters"
```

You should see the toolkit respond with your cluster information.

### Available Commands

Once configured, you can interact with Databricks using natural language:

- "List all my Databricks clusters"
- "Create a new cluster called 'analytics' with 2 workers"
- "Run this SQL query: SELECT * FROM my_table LIMIT 10"
- "Execute this Python code on cluster xyz: df.show()"
- "Submit a notebook run for /path/to/notebook"
- "Check the status of job run 12345"

The MCP server translates these requests into the appropriate API calls.

## MCP Tools Reference

The toolkit provides 23 MCP tools for Databricks operations.

### Cluster Management

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `list_clusters` | Enumerate all clusters | None |
| `create_cluster` | Provision new cluster | `cluster_name`, `spark_version`, `node_type_id`, `num_workers` |
| `get_cluster` | Retrieve cluster details | `cluster_id` |
| `start_cluster` | Start terminated cluster | `cluster_id` |
| `terminate_cluster` | Terminate running cluster | `cluster_id` |

### Job Management

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `list_jobs` | List all jobs | None |
| `get_job` | Get job configuration | `job_id` |
| `run_job` | Execute existing job | `job_id`, `notebook_params` (optional) |
| `submit_single_run` | Submit one-time notebook run | `run_config` |
| `get_run` | Get run details | `run_id` |
| `get_run_output` | Retrieve run output | `run_id` |
| `wait_for_run_completion` | Poll until run completes | `run_id`, `poll_interval`, `max_wait_seconds` |

### Notebook Operations

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `list_notebooks` | List workspace notebooks | `path` |
| `export_notebook` | Export notebook content | `path`, `format` (SOURCE/HTML/JUPYTER/DBC) |

### File System

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `list_files` | Browse DBFS contents | `dbfs_path` |

### SQL Execution

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `execute_sql` | Run SQL statements | `statement`, `warehouse_id` (optional), `catalog`, `schema` |

### Command Execution

Execute code directly on Databricks clusters with state persistence across commands.

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `create_execution_context` | Initialize execution context | `cluster_id`, `language` (python/scala/sql/r) |
| `execute_command` | Run code in context | `cluster_id`, `context_id`, `command`, `language` |
| `get_command_status` | Check command status (auto-polls) | `cluster_id`, `context_id`, `command_id` |
| `cancel_command` | Cancel running command | `cluster_id`, `context_id`, `command_id` |
| `destroy_execution_context` | Clean up context | `cluster_id`, `context_id` |
| `get_context_status` | Verify context validity | `cluster_id`, `context_id` |
| `execute_command_simple` | One-shot command execution | `cluster_id`, `command`, `language` |

## Testing

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run all tests
pytest tests/ -v

# Run with coverage
pytest --cov=src tests/ --cov-report=term-missing
```

## Troubleshooting

### Common Issues

**Connection Errors**
```
Issue: Unable to connect to Databricks API
Solution: Verify DATABRICKS_HOST and DATABRICKS_TOKEN in .env file
```

**SQL Execution Failures**
```
Issue: 400 Bad Request on SQL queries
Solution: Ensure DATABRICKS_WAREHOUSE_ID is set and warehouse is running
```

**Permission Denied**
```
Issue: 403 Forbidden on API calls
Solution: Verify your access token has required permissions for the operation
```

**Module Import Errors**
```
Issue: ModuleNotFoundError
Solution: Ensure dependencies are installed: pip install -e .
```

### Debug Mode

Enable detailed logging for troubleshooting:

```bash
LOG_LEVEL=DEBUG python -m src.main
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- **Issues**: Report bugs or request features via [GitHub Issues](https://github.com/kshptl/databricks-mcp-toolkit/issues)
- **Documentation**: See the [examples/](examples/) directory for detailed usage examples
- **MCP Protocol**: Learn more at [Model Context Protocol](https://modelcontextprotocol.io/)

## Acknowledgments

Built with the Model Context Protocol specification and Databricks Python SDK.

---

**Author**: Kush Patel  
**Repository**: https://github.com/kshptl/databricks-mcp-toolkit  
**MCP Version**: 1.0
