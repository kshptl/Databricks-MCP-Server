# Contributing to Databricks MCP Toolkit

Thank you for your interest in contributing to the Databricks MCP Toolkit! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Documentation](#documentation)
- [Submitting Changes](#submitting-changes)
- [Review Process](#review-process)

## Code of Conduct

### Our Standards

- Be respectful and inclusive
- Welcome newcomers and help them get started
- Focus on constructive feedback
- Accept criticism gracefully
- Prioritize the community's best interests

### Unacceptable Behavior

- Harassment or discriminatory language
- Personal attacks
- Publishing private information
- Unprofessional conduct

## Getting Started

### Prerequisites

- Python 3.10 or higher
- Git
- uv package manager (recommended)
- Databricks workspace access for testing
- Familiarity with async Python and MCP protocol

### Find an Issue

1. Browse [open issues](https://github.com/kshptl/databricks-mcp-toolkit/issues)
2. Look for issues labeled `good first issue` for beginner-friendly tasks
3. Check for issues labeled `help wanted` for more complex contributions
4. Comment on the issue to indicate you're working on it

### Ask Questions

- Use [GitHub Discussions](https://github.com/kshptl/databricks-mcp-toolkit/discussions) for questions
- Open an issue for bug reports or feature requests
- Be specific and provide context

## Development Setup

### 1. Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/databricks-mcp-toolkit.git
cd databricks-mcp-toolkit
```

### 2. Set Up Development Environment

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Unix/Linux/macOS
.venv\Scripts\activate     # Windows

# Install with development dependencies
uv pip install -e ".[dev]"

# Or with pip
pip install -e ".[dev]"
```

### 3. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your Databricks credentials
# DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
# DATABRICKS_TOKEN=your_token
# DATABRICKS_WAREHOUSE_ID=your_warehouse_id
```

### 4. Verify Setup

```bash
# Run tests to verify setup
pytest tests/ -v

# Check code formatting
black --check src/ tests/

# Run linter
pylint src/

# Type checking
mypy src/
```

## Development Workflow

### 1. Create a Branch

```bash
# Update main branch
git checkout main
git pull upstream main

# Create feature branch
git checkout -b feature/your-feature-name
# OR
git checkout -b fix/bug-description
```

### Branch Naming Conventions

- `feature/` - New features or enhancements
- `fix/` - Bug fixes
- `docs/` - Documentation updates
- `refactor/` - Code refactoring
- `test/` - Test additions or modifications

### 2. Make Changes

- Write clean, readable code
- Follow the coding standards below
- Add tests for new functionality
- Update documentation as needed
- Keep commits focused and atomic

### 3. Commit Changes

```bash
# Stage changes
git add .

# Commit with descriptive message
git commit -m "Add feature: brief description"
```

### Commit Message Guidelines

Format:
```
<type>: <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Test additions or modifications
- `chore`: Maintenance tasks

Example:
```
feat: Add support for Unity Catalog operations

- Implement catalog and schema browsing
- Add table listing functionality
- Include comprehensive tests

Closes #123
```

### 4. Keep Branch Updated

```bash
# Fetch latest changes
git fetch upstream

# Rebase on main
git rebase upstream/main
```

## Coding Standards

### Python Style Guide

Follow [PEP 8](https://peps.python.org/pep-0008/) with these specifics:

**Line Length**: Maximum 100 characters

**Indentation**: 4 spaces (no tabs)

**Quotes**: Double quotes by default
```python
message = "Hello, world!"
```

**Naming Conventions**:
```python
# Variables and functions: snake_case
user_name = "John"
def calculate_total(): pass

# Constants: UPPER_SNAKE_CASE
MAX_RETRIES = 3

# Classes: PascalCase
class DatabricksClient: pass

# Private: Leading underscore
_internal_function()
```

### Type Hints

Use type hints for all functions:

```python
from typing import Dict, List, Optional, Any

async def execute_query(
    statement: str,
    warehouse_id: Optional[str] = None,
    timeout: int = 60
) -> Dict[str, Any]:
    """Execute SQL statement.
    
    Args:
        statement: SQL query to execute
        warehouse_id: Optional warehouse ID
        timeout: Query timeout in seconds
        
    Returns:
        Query results as dictionary
        
    Raises:
        ValueError: If statement is empty
        TimeoutError: If query exceeds timeout
    """
    pass
```

### Docstrings

Use Google-style docstrings:

```python
def complex_function(param1: str, param2: int) -> bool:
    """One-line summary of function.
    
    More detailed description if needed. Explain the purpose,
    important behavior, or edge cases.
    
    Args:
        param1: Description of first parameter
        param2: Description of second parameter
        
    Returns:
        Description of return value
        
    Raises:
        ValueError: When param2 is negative
        RuntimeError: When operation fails
        
    Example:
        >>> result = complex_function("test", 42)
        >>> print(result)
        True
    """
    pass
```

### Async/Await

Use async properly:

```python
# Good: Async all the way
async def fetch_data() -> Dict:
    async with httpx.AsyncClient() as client:
        response = await client.get(...)
        return await response.json()

# Avoid: Mixing sync and async unnecessarily
async def bad_example():
    time.sleep(1)  # Blocks event loop!
    # Use: await asyncio.sleep(1)
```

### Error Handling

```python
# Specific exceptions
try:
    result = await api_call()
except httpx.TimeoutException:
    logger.error("Request timed out")
    raise TimeoutError("API request timed out")
except httpx.HTTPStatusError as e:
    logger.error(f"HTTP error: {e.response.status_code}")
    raise APIError(f"Request failed: {e}")

# Avoid bare except
# except:  # Bad!
#     pass
```

### Import Organization

```python
# 1. Standard library
import asyncio
import json
from typing import Dict, Optional

# 2. Third-party packages
import httpx
from pydantic import BaseModel

# 3. Local imports
from src.core.config import get_config
from src.core.utils import get_http_client
```

## Testing Guidelines

### Test Structure

```python
import pytest
from unittest.mock import AsyncMock, patch

class TestClusterOperations:
    """Test suite for cluster operations."""
    
    @pytest.fixture
    def mock_client(self):
        """Create mock Databricks client."""
        client = AsyncMock()
        client.clusters.list.return_value = [...]
        return client
    
    @pytest.mark.asyncio
    async def test_list_clusters_success(self, mock_client):
        """Test successful cluster listing."""
        # Arrange
        expected_count = 3
        
        # Act
        result = await list_clusters(mock_client)
        
        # Assert
        assert len(result) == expected_count
        mock_client.clusters.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_clusters_empty(self, mock_client):
        """Test cluster listing with no clusters."""
        # Arrange
        mock_client.clusters.list.return_value = []
        
        # Act
        result = await list_clusters(mock_client)
        
        # Assert
        assert result == []
```

### Test Coverage

- Aim for 80%+ code coverage
- Test happy paths and error cases
- Test edge cases and boundary conditions
- Mock external dependencies

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_clusters.py -v

# Run with coverage
pytest --cov=src tests/ --cov-report=term-missing

# Run tests matching pattern
pytest -k "test_list" -v
```

## Documentation

### Code Documentation

- Add docstrings to all public functions and classes
- Include examples for complex functionality
- Document parameters, return values, and exceptions
- Keep documentation up to date with code changes

### README Updates

Update README.md when:
- Adding new MCP tools
- Changing configuration requirements
- Adding new features or capabilities
- Modifying installation/setup process

### API Documentation

- Document new API endpoints
- Include request/response examples
- Explain error responses
- Document rate limits or constraints

## Submitting Changes

### 1. Prepare Submission

```bash
# Ensure tests pass
pytest tests/ -v

# Check code formatting
black src/ tests/

# Run linter
pylint src/

# Type check
mypy src/
```

### 2. Push Changes

```bash
# Push to your fork
git push origin feature/your-feature-name
```

### 3. Create Pull Request

1. Go to your fork on GitHub
2. Click "New Pull Request"
3. Select your branch
4. Fill out the PR template:
   - Clear description of changes
   - Link to related issues
   - Testing performed
   - Screenshots if applicable

### Pull Request Template

```markdown
## Description
Brief description of changes

## Related Issues
Fixes #123

## Changes Made
- Added feature X
- Updated documentation
- Added tests for Y

## Testing
- [ ] All tests pass
- [ ] Added new tests
- [ ] Tested manually with Databricks

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No breaking changes (or documented)
```

## Review Process

### What to Expect

1. **Initial Review**: Maintainer reviews within 1-3 days
2. **Feedback**: Constructive feedback on changes
3. **Iterations**: Address feedback and update PR
4. **Approval**: Once approved, PR will be merged

### Review Criteria

- Code quality and style compliance
- Test coverage and quality
- Documentation completeness
- Performance implications
- Breaking changes assessment

### Responding to Feedback

```bash
# Make requested changes
git add .
git commit -m "Address review feedback"
git push origin feature/your-feature-name
```

## Additional Guidelines

### Performance Considerations

- Use async operations for I/O
- Avoid blocking the event loop
- Consider memory usage for large datasets
- Profile performance-critical code

### Security

- Never commit credentials or tokens
- Validate all user inputs
- Use environment variables for sensitive data
- Follow least privilege principle

### Backwards Compatibility

- Avoid breaking changes when possible
- Deprecate before removing functionality
- Document migration paths
- Maintain semantic versioning

## Questions or Issues?

- **Technical Questions**: [GitHub Discussions](https://github.com/kshptl/databricks-mcp-toolkit/discussions)
- **Bug Reports**: [GitHub Issues](https://github.com/kshptl/databricks-mcp-toolkit/issues)
- **Feature Requests**: [GitHub Issues](https://github.com/kshptl/databricks-mcp-toolkit/issues)

Thank you for contributing to the Databricks MCP Toolkit!

---

**Maintainer**: Kush Patel  
**Last Updated**: January 2025
