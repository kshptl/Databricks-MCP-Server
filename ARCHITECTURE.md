# Architecture Documentation

## System Overview

The Databricks MCP Toolkit is designed as a multi-layered architecture that provides a robust, scalable interface between AI agents and Databricks services through the Model Context Protocol (MCP).

## Architectural Layers

### 1. MCP Protocol Interface Layer
**Location**: `src/server/databricks_mcp_server.py`

This layer implements the MCP specification and exposes Databricks functionality as standardized MCP tools.

**Responsibilities**:
- MCP protocol compliance
- Tool registration and discovery
- Request/response marshaling
- Error handling and standardization

**Key Components**:
- `DatabricksMCPServer`: Main server class implementing MCP handlers
- Tool definitions with JSON schemas
- Protocol-level error handling

### 2. API Client Abstraction Layer
**Location**: `src/api/`

Provides high-level abstractions over Databricks REST APIs with consistent error handling and async support.

**Modules**:
```
src/api/
├── clusters.py           # Cluster lifecycle management
├── jobs.py              # Job and run operations
├── notebooks.py         # Notebook management
├── sql.py              # SQL execution
├── command_execution.py # Command execution contexts
└── dbfs.py             # File system operations
```

**Design Patterns**:
- Async/await for non-blocking I/O
- Consistent error handling with custom exceptions
- Type hints for better IDE support
- Retry logic for transient failures

### 3. Core Services Layer
**Location**: `src/core/`

Provides foundational services used across the application.

**Components**:
- `auth.py`: Authentication and token management
- `config.py`: Configuration management with Pydantic
- `utils.py`: HTTP client, logging, and utilities

**Features**:
- Environment-based configuration
- Connection pooling for HTTP requests
- Structured logging
- Token validation and refresh

### 4. CLI Interface Layer
**Location**: `src/cli/`

Optional command-line interface for direct server interaction.

**Capabilities**:
- Server startup and shutdown
- Configuration validation
- Health checks
- Tool invocation

## Data Flow

### Tool Invocation Flow

```
AI Agent Request
       ↓
MCP Protocol Handler
       ↓
Parameter Validation
       ↓
API Client Method
       ↓
HTTP Request (with retry)
       ↓
Databricks REST API
       ↓
Response Processing
       ↓
Result Marshaling
       ↓
MCP Response to Agent
```

### Execution Context Flow

```
Create Context
       ↓
Initialize on Cluster
       ↓
Execute Command(s) ←─┐
       ↓              │
Get Status (polling) │
       ↓              │
More Commands? ──────┘
       ↓
Destroy Context
```

## Key Design Decisions

### 1. Async-First Architecture

**Rationale**: Databricks operations are I/O-bound, making async operations ideal for performance.

**Implementation**:
```python
async def execute_sql(params: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient() as client:
        response = await client.post(...)
        return await process_response(response)
```

### 2. Layered Error Handling

**Approach**: Each layer has specific error handling responsibilities.

**Layers**:
1. **MCP Layer**: Protocol-level errors (malformed requests)
2. **API Layer**: Service-level errors (authentication, permissions)
3. **HTTP Layer**: Network-level errors (timeouts, retries)

### 3. Configuration Management

**Strategy**: Environment-based with validation.

```python
class DatabricksConfig(BaseSettings):
    host: str
    token: SecretStr
    warehouse_id: Optional[str] = None
    
    class Config:
        env_prefix = "DATABRICKS_"
        case_sensitive = False
```

### 4. Type Safety

**Approach**: Comprehensive type hints throughout codebase.

**Benefits**:
- Compile-time error detection
- Better IDE support
- Self-documenting code

## Scalability Considerations

### Connection Pooling

HTTP client maintains persistent connections:
```python
client = httpx.AsyncClient(
    limits=httpx.Limits(max_keepalive_connections=20),
    timeout=httpx.Timeout(60.0)
)
```

### Async Concurrency

Multiple tools can execute concurrently:
```python
results = await asyncio.gather(
    server.call_tool('list_clusters', ...),
    server.call_tool('list_jobs', ...),
    server.call_tool('execute_sql', ...)
)
```

### Resource Management

Context managers ensure proper cleanup:
```python
async with create_execution_context(...) as context:
    await execute_commands(context)
    # Context automatically destroyed on exit
```

## Security Architecture

### Authentication Flow

```
Environment Variables
       ↓
Configuration Loader
       ↓
Token Validator
       ↓
Request Authenticator
       ↓
Databricks API
```

### Credential Handling

1. **Storage**: Environment variables only, never in code
2. **Transmission**: HTTPS only, token in Authorization header
3. **Validation**: Token format and expiration checks
4. **Scope**: Minimum required permissions

## Extension Points

### Adding New Tools

1. **Define tool schema** in `DatabricksMCPServer`
2. **Implement API method** in appropriate `src/api/` module
3. **Register tool handler** in server
4. **Add tests** in `tests/`

Example:
```python
# In src/api/new_feature.py
async def new_operation(params: Dict[str, Any]) -> Dict[str, Any]:
    """Implement new Databricks operation."""
    pass

# In src/server/databricks_mcp_server.py
@server.list_tools()
async def handle_list_tools():
    return [{
        "name": "new_operation",
        "description": "Description of operation",
        "inputSchema": {...}
    }]
```

### Adding New API Clients

Create new module in `src/api/`:
```python
# src/api/new_service.py
from src.core.utils import get_http_client
from src.core.config import get_config

async def service_operation(**kwargs):
    """Implement service-specific logic."""
    config = get_config()
    client = get_http_client()
    # Implementation
```

## Testing Architecture

### Test Organization

```
tests/
├── test_mcp_server.py      # MCP protocol compliance
├── test_clusters.py        # Cluster API tests
├── test_jobs.py           # Job API tests
├── test_sql.py            # SQL execution tests
└── conftest.py            # Shared fixtures
```

### Mocking Strategy

Mock external dependencies:
```python
@pytest.fixture
def mock_databricks_api(mocker):
    return mocker.patch('httpx.AsyncClient.post')

async def test_list_clusters(mock_databricks_api):
    mock_databricks_api.return_value.json.return_value = {...}
    result = await list_clusters()
    assert result is not None
```

## Performance Metrics

### Key Performance Indicators

- **Tool Invocation Latency**: Time from MCP request to response
- **API Call Duration**: Time for Databricks API calls
- **Connection Pool Utilization**: Active vs. idle connections
- **Error Rate**: Failed requests per total requests

### Monitoring Points

1. MCP tool execution time
2. HTTP request/response time
3. Retry attempts and success rate
4. Memory usage for large result sets

## Future Enhancements

### Planned Improvements

1. **Caching Layer**: Cache frequently accessed data (cluster lists, job definitions)
2. **Batch Operations**: Support for bulk operations where applicable
3. **Webhook Integration**: Event-driven notifications for long-running operations
4. **Metrics Export**: Prometheus-compatible metrics endpoint
5. **Rate Limit Management**: Intelligent request throttling

### API Evolution

Following semantic versioning:
- **Patch**: Bug fixes, no API changes
- **Minor**: New tools, backward compatible
- **Major**: Breaking changes to existing tools

## Dependencies

### Core Dependencies

- **mcp**: MCP protocol implementation
- **httpx**: Async HTTP client
- **pydantic**: Data validation and settings
- **databricks-sdk**: Official Databricks SDK (for reference)

### Development Dependencies

- **pytest**: Testing framework
- **pytest-asyncio**: Async test support
- **black**: Code formatting
- **pylint**: Static analysis
- **mypy**: Type checking

## Deployment Patterns

### Standalone Server

```bash
python -m src.main
```

### Docker Container

```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install -e .
CMD ["python", "-m", "src.main"]
```

### Service Integration

Embed in larger applications:
```python
from src.server.databricks_mcp_server import DatabricksMCPServer

async def main():
    server = DatabricksMCPServer()
    # Use server in application
```

---

**Document Version**: 1.0  
**Last Updated**: January 2025  
**Maintainer**: Kush Patel
