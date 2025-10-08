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

## Architecture

The toolkit is structured as a multi-layered architecture:

```
┌─────────────────────────────────────┐
│   MCP Protocol Interface Layer      │  ← Standard MCP tools
├─────────────────────────────────────┤
│   API Client Abstraction Layer      │  ← Databricks API wrappers
├─────────────────────────────────────┤
│   Core Services Layer               │  ← Auth, config, utilities
├─────────────────────────────────────┤
│   Databricks REST API               │  ← Databricks platform
└─────────────────────────────────────┘
```

## Prerequisites

- **Python**: 3.10 or higher
- **Package Manager**: `uv` (recommended) or `pip`
- **Databricks Access**: Valid workspace URL and personal access token
- **SQL Warehouse**: (Optional) For SQL execution capabilities

## Installation

### Using uv (Recommended)

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh  # Unix/Linux/macOS
# OR
irm https://astral.sh/uv/install.ps1 | iex      # Windows PowerShell

# Clone and install
git clone https://github.com/kshptl/databricks-mcp-toolkit.git
cd databricks-mcp-toolkit
uv pip install -e .
```

### Using pip

```bash
git clone https://github.com/kshptl/databricks-mcp-toolkit.git
cd databricks-mcp-toolkit
python -m venv .venv
source .venv/bin/activate  # Unix/Linux/macOS
# OR
.venv\Scripts\activate     # Windows

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

## Quick Start

### Starting the Server

```bash
# Activate virtual environment
source .venv/bin/activate  # Unix/Linux/macOS
.venv\Scripts\activate     # Windows

# Start the MCP server
python -m src.main
```

### Basic Usage Example

```python
from src.server.databricks_mcp_server import DatabricksMCPServer

async def main():
    server = DatabricksMCPServer()
    
    # List available clusters
    clusters = await server.call_tool('list_clusters', {'params': {}})
    print(f"Available clusters: {len(clusters)}")
    
    # Execute SQL query
    result = await server.call_tool('execute_sql', {
        'params': {
            'statement': 'SELECT COUNT(*) as row_count FROM my_table'
        }
    })
    print(f"Query result: {result}")
```

## MCP Tools Reference

The toolkit provides 23 MCP tools organized into functional categories. All tools follow a consistent parameter format with a `params` wrapper object.

### Tool Invocation Pattern

```python
await server.call_tool('tool_name', {
    'params': {
        'parameter1': 'value1',
        'parameter2': 'value2'
    }
})
```

### Cluster Management (5 tools)

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `list_clusters` | Enumerate all clusters | None |
| `create_cluster` | Provision new cluster | `cluster_name`, `spark_version`, `node_type_id`, `num_workers` |
| `get_cluster` | Retrieve cluster details | `cluster_id` |
| `start_cluster` | Start terminated cluster | `cluster_id` |
| `terminate_cluster` | Terminate running cluster | `cluster_id` |

### Job Management (7 tools)

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `list_jobs` | List all jobs | None |
| `get_job` | Get job configuration | `job_id` |
| `run_job` | Execute existing job | `job_id`, `notebook_params` (optional) |
| `submit_single_run` | Submit one-time notebook run | `run_config` |
| `get_run` | Get run details | `run_id` |
| `get_run_output` | Retrieve run output | `run_id` |
| `wait_for_run_completion` | Poll until run completes | `run_id`, `poll_interval`, `max_wait_seconds` |

### Notebook Operations (2 tools)

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `list_notebooks` | List workspace notebooks | `path` |
| `export_notebook` | Export notebook content | `path`, `format` (SOURCE/HTML/JUPYTER/DBC) |

### File System (1 tool)

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `list_files` | Browse DBFS contents | `dbfs_path` |

### SQL Execution (1 tool)

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `execute_sql` | Run SQL statements | `statement`, `warehouse_id` (optional), `catalog`, `schema` |

### Command Execution (7 tools)

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

## Usage Examples

### Example 1: Cluster Lifecycle Management

```python
# Create a new cluster
cluster = await server.call_tool('create_cluster', {
    'params': {
        'cluster_name': 'analytics-cluster',
        'spark_version': '11.3.x-scala2.12',
        'node_type_id': 'i3.xlarge',
        'num_workers': 3,
        'autotermination_minutes': 120
    }
})
cluster_id = cluster['cluster_id']

# Monitor cluster status
status = await server.call_tool('get_cluster', {
    'params': {'cluster_id': cluster_id}
})

# Terminate when done
await server.call_tool('terminate_cluster', {
    'params': {'cluster_id': cluster_id}
})
```

### Example 2: Notebook Execution Workflow

```python
# Submit notebook for execution
run = await server.call_tool('submit_single_run', {
    'params': {
        'run_config': {
            'run_name': 'Data Processing Pipeline',
            'tasks': [{
                'task_key': 'etl_task',
                'notebook_task': {
                    'notebook_path': '/Shared/ETL/ProcessData',
                    'base_parameters': {
                        'date': '2025-01-07',
                        'environment': 'production'
                    }
                },
                'existing_cluster_id': 'your-cluster-id'
            }],
            'timeout_seconds': 3600
        }
    }
})

# Wait for completion with automatic polling
final_state = await server.call_tool('wait_for_run_completion', {
    'params': {
        'run_id': run['run_id'],
        'poll_interval': 10,
        'max_wait_seconds': 3600
    }
})

# Retrieve output if successful
if final_state['state']['result_state'] == 'SUCCESS':
    output = await server.call_tool('get_run_output', {
        'params': {'run_id': run['run_id']}
    })
    print(f"Processing complete: {output['notebook_output']['result']}")
```

### Example 3: Interactive Command Execution

```python
# Create persistent execution context
context = await server.call_tool('create_execution_context', {
    'params': {
        'cluster_id': 'your-cluster-id',
        'language': 'python'
    }
})
context_id = context['id']

# Execute data loading
cmd1 = await server.call_tool('execute_command', {
    'params': {
        'cluster_id': 'your-cluster-id',
        'context_id': context_id,
        'command': 'df = spark.read.parquet("/data/source.parquet")',
        'language': 'python'
    }
})

# Wait for completion (auto-polls)
await server.call_tool('get_command_status', {
    'params': {
        'cluster_id': 'your-cluster-id',
        'context_id': context_id,
        'command_id': cmd1['id']
    }
})

# Execute transformation (uses 'df' from previous command)
cmd2 = await server.call_tool('execute_command', {
    'params': {
        'cluster_id': 'your-cluster-id',
        'context_id': context_id,
        'command': 'df.groupBy("category").count().show()',
        'language': 'python'
    }
})

# Clean up
await server.call_tool('destroy_execution_context', {
    'params': {
        'cluster_id': 'your-cluster-id',
        'context_id': context_id
    }
})
```

### Example 4: SQL Analytics

```python
# Execute analytical query
result = await server.call_tool('execute_sql', {
    'params': {
        'statement': '''
            SELECT 
                department,
                COUNT(*) as employee_count,
                AVG(salary) as avg_salary
            FROM hr.employees
            WHERE status = 'active'
            GROUP BY department
            ORDER BY avg_salary DESC
        ''',
        'catalog': 'production',
        'schema': 'hr'
    }
})

# Process results
for row in result['result']['data_array']:
    dept, count, avg_sal = row
    print(f"{dept}: {count} employees, avg salary: ${avg_sal:,.2f}")
```

## Project Structure

```
databricks-mcp-toolkit/
├── src/
│   ├── main.py                          # Application entry point
│   ├── server/
│   │   └── databricks_mcp_server.py     # MCP server implementation
│   ├── api/                             # Databricks API clients
│   │   ├── clusters.py
│   │   ├── jobs.py
│   │   ├── notebooks.py
│   │   ├── sql.py
│   │   ├── command_execution.py
│   │   └── dbfs.py
│   ├── core/                            # Core services
│   │   ├── auth.py
│   │   ├── config.py
│   │   └── utils.py
│   └── cli/                             # CLI interface
│       └── commands.py
├── tests/                               # Test suite
├── examples/                            # Usage examples
├── scripts/                             # Utility scripts
├── docs/                                # Documentation
├── pyproject.toml                       # Project configuration
└── README.md
```

## Testing

### Running Tests

```bash
# Install development dependencies
uv pip install -e ".[dev]"

# Run all tests
pytest tests/ -v

# Run with coverage
pytest --cov=src tests/ --cov-report=term-missing

# Run specific test file
pytest tests/test_mcp_server.py -v
```

### Test Categories

- **Unit Tests**: Test individual functions and classes in isolation
- **Integration Tests**: Test component interactions
- **MCP Protocol Tests**: Verify MCP specification compliance

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
Solution: Ensure dependencies are installed: uv pip install -e .
```

### Debug Mode

Enable detailed logging for troubleshooting:

```bash
LOG_LEVEL=DEBUG python -m src.main
```

## Performance Considerations

- **Connection Pooling**: HTTP client uses connection pooling for efficiency
- **Async Operations**: All API calls are asynchronous for better throughput
- **Rate Limiting**: Built-in retry logic handles API rate limits
- **Timeouts**: Configurable timeouts prevent hanging operations

## Security Best Practices

1. **Credential Management**: Never commit `.env` files or expose tokens
2. **Token Rotation**: Regularly rotate Databricks access tokens
3. **Least Privilege**: Use tokens with minimum required permissions
4. **Network Security**: Use VPN/private endpoints for sensitive workloads

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Standards

- Follow PEP 8 style guidelines
- Include type hints for all functions
- Write comprehensive docstrings (Google style)
- Add tests for new functionality
- Maintain 80%+ code coverage

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
