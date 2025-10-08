# Databricks MCP Server vs API Discrepancy Analysis

## Executive Summary

After analyzing the MCP server implementation (`src/server/databricks_mcp_server.py`) and the API modules (`src/api/`), I found significant discrepancies between what tools are exposed in the server versus what functionality is available in the API modules.

## Critical Issue: Function Name Mismatch

**BREAKING BUG**: The server calls `sql.execute_sql()` but the SQL API module (`src/api/sql.py`) does not contain this function. The API module has:
- `execute_statement()`
- `execute_and_wait()` 
- `get_statement_status()`
- `cancel_statement()`

But the server tool calls `await sql.execute_sql(statement, warehouse_id, catalog, schema)` which will cause an AttributeError at runtime.

## Missing Tools in MCP Server

### Cluster Management
**Available in API but NOT exposed in server:**
- `resize_cluster()` - Resize cluster by changing number of workers
- `restart_cluster()` - Restart a Databricks cluster

### Job Management  
**Available in API but NOT exposed in server:**
- `create_job()` - Create a new Databricks job
- `get_job()` - Get information about a specific job
- `update_job()` - Update an existing job
- `delete_job()` - Delete a job
- `get_run()` - Get information about a job run
- `cancel_run()` - Cancel a job run

### Notebook Management
**Available in API but NOT exposed in server:**
- `import_notebook()` - Import a notebook into workspace
- `delete_notebook()` - Delete a notebook or directory
- `create_directory()` - Create a directory in workspace

### DBFS Management
**Available in API but NOT exposed in server:**
- `put_file()` - Upload a file to DBFS
- `upload_large_file()` - Upload large files in chunks
- `get_file()` - Get contents of a file from DBFS  
- `delete_file()` - Delete a file or directory from DBFS
- `get_status()` - Get status of a file or directory
- `create_directory()` - Create a directory in DBFS

### SQL Execution
**Available in API but NOT exposed in server:**
- `execute_and_wait()` - Execute SQL and wait for completion with polling
- `get_statement_status()` - Get status of a SQL statement
- `cancel_statement()` - Cancel a running SQL statement

## Currently Exposed Tools (Working)

### Cluster Management ✅
- `list_clusters` → `clusters.list_clusters()`
- `create_cluster` → `clusters.create_cluster(params)`
- `terminate_cluster` → `clusters.terminate_cluster(cluster_id)`
- `get_cluster` → `clusters.get_cluster(cluster_id)`
- `start_cluster` → `clusters.start_cluster(cluster_id)`

### Job Management ✅
- `list_jobs` → `jobs.list_jobs()`
- `run_job` → `jobs.run_job(job_id, notebook_params)`

### Notebook Management ✅
- `list_notebooks` → `notebooks.list_notebooks(path)`
- `export_notebook` → `notebooks.export_notebook(path, format)`

### DBFS Management ✅
- `list_files` → `dbfs.list_files(dbfs_path)`

### SQL Execution ❌
- `execute_sql` → **BROKEN** - calls `sql.execute_sql()` which doesn't exist

## Recommendations

### 1. Fix Critical Bug (HIGH PRIORITY)
Fix the SQL execution tool by either:
- **Option A**: Rename `execute_statement()` to `execute_sql()` in `src/api/sql.py`
- **Option B**: Update the server to call `sql.execute_statement()` instead

### 2. Add Missing Core Tools (MEDIUM PRIORITY)
Consider adding these commonly needed tools:
- `resize_cluster` and `restart_cluster` for cluster management
- `create_job`, `get_job`, `delete_job` for job lifecycle management
- `import_notebook`, `delete_notebook` for notebook management
- `put_file`, `get_file`, `delete_file` for basic DBFS operations

### 3. Add Advanced Tools (LOW PRIORITY)
For power users, consider adding:
- `upload_large_file` for handling large file uploads
- `execute_and_wait`, `get_statement_status`, `cancel_statement` for advanced SQL operations
- `get_run`, `cancel_run` for job run management

### 4. Improve Parameter Handling
The server tools should validate required parameters and provide better error messages for missing parameters rather than passing None values to API functions.

## Impact Assessment

**High Impact Issues:**
- SQL execution is completely broken due to function name mismatch
- Limited DBFS functionality (only listing, no upload/download/delete)
- Basic job management only (can't create, update, or delete jobs)

**Medium Impact Issues:**
- Missing cluster resize/restart operations
- No notebook import/delete capabilities
- No advanced SQL execution features

The server currently exposes only about 30% of the available API functionality.
