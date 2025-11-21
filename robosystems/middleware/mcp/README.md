# MCP Middleware

This middleware provides Model Context Protocol (MCP) integration for AI-powered graph database interactions, enabling natural language queries and intelligent data access through the RoboSystems platform.

## Overview

The MCP middleware:
- Provides MCP client implementation for graph database access
- Manages connection pooling for efficient resource usage
- Implements specialized MCP tools for graph operations
- Handles query validation and complexity management
- Integrates with the Graph API for backend communication
- Supports both shared repositories (SEC) and user graphs
- Enables workspace management for isolated development environments
- Provides data operation tools for staging, querying, and graph materialization

## Architecture

```
mcp/
├── __init__.py              # Module exports and public API
├── client.py                # KuzuMCPClient implementation
├── factory.py               # Client factory and pooling
├── pool.py                  # Connection pool management
├── query_validator.py       # Query validation and complexity checks
├── exceptions.py            # MCP-specific exception classes
└── tools/                   # MCP tool implementations
    ├── base_tool.py        # Base tool interface
    ├── manager.py          # Tool management and registry
    ├── cypher_tool.py      # Cypher query execution
    ├── schema_tool.py      # Schema introspection
    ├── structure_tool.py   # Graph structure exploration
    ├── elements_tool.py    # Element/taxonomy queries
    ├── facts_tool.py       # Financial fact queries
    ├── properties_tool.py  # Property discovery
    ├── example_queries_tool.py # Query examples and templates
    ├── workspace.py        # Workspace/subgraph management
    └── data_tools.py       # Data operation tools (staging, materialization)
```

## Key Components

### 1. KuzuMCPClient (`client.py`)

Main MCP client for interacting with graph databases through the RoboSystems API.

**Features:**
- HTTP-based communication with Graph API
- Automatic timeout and retry handling
- Query complexity validation
- Streaming support for large results
- Schema caching for performance

**Usage:**
```python
from robosystems.middleware.mcp import create_kuzu_mcp_client

# Create client with automatic endpoint discovery
client = await create_kuzu_mcp_client(graph_id="sec")

# Execute Cypher query
result = await client.execute_query(
    "MATCH (c:Company) WHERE c.ticker = 'AAPL' RETURN c"
)

# Get schema information
schema = await client.get_schema()
```

### 2. Connection Pooling (`pool.py`, `factory.py`)

Efficient connection pooling to reduce initialization overhead and improve performance.

**Features:**
- Per-graph connection pools
- Configurable pool sizes and lifetimes
- Automatic cleanup of idle connections
- Connection recycling based on age
- Thread-safe pool management

**Configuration:**
```python
# Default pool settings
max_connections_per_graph: 10
max_idle_time: 300 seconds (5 minutes)
max_lifetime: 3600 seconds (1 hour)
```

**Usage:**
```python
from robosystems.middleware.mcp import acquire_kuzu_mcp_client

# Acquire client from pool (recommended)
async with acquire_kuzu_mcp_client(graph_id="kg1a2b3c") as client:
    result = await client.execute_query("MATCH (n) RETURN count(n)")
    # Client automatically returned to pool
```

### 3. MCP Tools (`tools/`)

Specialized tools for different graph database operations.

**Available Tools:**

#### Cypher Tool (`cypher_tool.py`)
Execute Cypher queries with validation and result processing.

```python
from robosystems.middleware.mcp.tools import CypherTool

tool = CypherTool(client)
result = await tool.execute({
    "query": "MATCH (c:Company) RETURN c.name LIMIT 10"
})
```

#### Schema Tool (`schema_tool.py`)
Introspect graph schema and structure.

```python
from robosystems.middleware.mcp.tools import SchemaTool

tool = SchemaTool(client)
schema = await tool.get_schema()
# Returns node types, relationships, and properties
```

#### Structure Tool (`structure_tool.py`)
Explore graph structure and relationships.

```python
from robosystems.middleware.mcp.tools import StructureTool

tool = StructureTool(client)
structure = await tool.get_structure({
    "node_type": "Company",
    "depth": 2
})
```

#### Facts Tool (`facts_tool.py`)
Query financial facts from SEC XBRL data.

```python
from robosystems.middleware.mcp.tools import FactsTool

tool = FactsTool(client)
facts = await tool.query({
    "entity": "AAPL",
    "element": "Assets",
    "period": "2023"
})
```

#### Properties Tool (`properties_tool.py`)
Discover available properties on nodes.

```python
from robosystems.middleware.mcp.tools import PropertiesTool

tool = PropertiesTool(client)
properties = await tool.discover({
    "node_type": "Company"
})
```

#### Example Queries Tool (`example_queries_tool.py`)
Provide query templates and examples.

```python
from robosystems.middleware.mcp.tools import ExampleQueriesTool

tool = ExampleQueriesTool(client)
examples = await tool.get_examples({
    "category": "financial_analysis"
})
```

#### Workspace Tools (`workspace.py`)
Manage workspaces (subgraphs) for isolated development and testing environments.

**Available Operations:**
- `create-workspace` - Create new workspace/subgraph
- `delete-workspace` - Delete existing workspace
- `list-workspaces` - List all workspaces for parent graph
- `switch-workspace` - Switch active workspace context

```python
from robosystems.middleware.mcp.tools import CreateWorkspaceTool

tool = CreateWorkspaceTool(client)
result = await tool.execute({
    "name": "dev",
    "description": "Development workspace",
    "fork_parent": False
})
```

#### Data Operation Tools (`data_tools.py`)
Tools for data ingestion, staging, and graph materialization workflows.

##### Build Fact Grid Tool
Construct multidimensional fact grids from graph data for analysis.

```python
from robosystems.middleware.mcp.tools import BuildFactGridTool

tool = BuildFactGridTool(client)
result = await tool.execute({
    "elements": ["us-gaap:Assets", "us-gaap:Liabilities"],
    "periods": ["2023-12-31", "2024-12-31"],
    "dimensions": {},
    "rows": [{"dimension": "element"}],
    "columns": [{"dimension": "period"}]
})
```

##### Ingest File Tool
Upload and stage files in DuckDB for immediate querying before graph materialization.

```python
from robosystems.middleware.mcp.tools import IngestFileTool

tool = IngestFileTool(client)
result = await tool.execute({
    "file_path": "/path/to/data.csv",
    "table_name": "financial_data",
    "ingest_to_graph": False
})
```

##### Map Elements Tool
Map Chart of Accounts elements to XBRL taxonomy elements (US-GAAP).

```python
from robosystems.middleware.mcp.tools import MapElementsTool

tool = MapElementsTool(client)
result = await tool.execute({
    "structure_id": "mapping_123",
    "source_elements": ["Revenue", "COGS"],
    "target_taxonomy": "us-gaap"
})
```

##### Query Staging Tool
Execute SQL queries against DuckDB staging tables before materialization.

```python
from robosystems.middleware.mcp.tools import QueryStagingTool

tool = QueryStagingTool(client)
result = await tool.execute({
    "sql": "SELECT * FROM financial_data WHERE amount > 1000",
    "limit": 100
})
```

##### Materialize Graph Tool
Trigger materialization from DuckDB staging to Kuzu graph database.

```python
from robosystems.middleware.mcp.tools import MaterializeGraphTool

tool = MaterializeGraphTool(client)
result = await tool.execute({
    "table_name": "financial_data",
    "file_id": "optional_specific_file_id"
})
```

### 4. Query Validation (`query_validator.py`)

Validates query complexity and enforces limits to prevent resource exhaustion.

**Features:**
- Query length validation (max 50KB)
- Complexity scoring based on query patterns
- Timeout enforcement (30 seconds default)
- Result size limits
- Protection against expensive operations

**Configuration:**
```bash
GRAPH_MAX_QUERY_LENGTH=50000      # Maximum query size (bytes)
GRAPH_QUERY_TIMEOUT=30            # Query timeout (seconds)
MCP_MAX_COMPLEXITY_SCORE=100      # Complexity threshold
```

### 5. Exception Handling (`exceptions.py`)

Comprehensive exception hierarchy for MCP operations.

**Exception Classes:**
- `KuzuAPIError` - Base exception for all MCP errors
- `KuzuQueryTimeoutError` - Query exceeded timeout
- `KuzuQueryComplexityError` - Query too complex
- `KuzuValidationError` - Invalid query or parameters
- `KuzuAuthenticationError` - Authentication failed
- `KuzuAuthorizationError` - Insufficient permissions
- `KuzuConnectionError` - Connection to Graph API failed
- `KuzuResourceNotFoundError` - Resource not found
- `KuzuRateLimitError` - Rate limit exceeded
- `KuzuSchemaError` - Schema validation failed

**Usage:**
```python
from robosystems.middleware.mcp import (
    KuzuQueryTimeoutError,
    KuzuValidationError
)

try:
    result = await client.execute_query(query)
except KuzuQueryTimeoutError:
    # Handle timeout
    logger.warning("Query timed out")
except KuzuValidationError as e:
    # Handle validation error
    logger.error(f"Invalid query: {e}")
```

## Configuration

### Environment Variables

```bash
# Graph API Connectivity
GRAPH_API_URL=http://localhost:8001    # Base URL (auto-discovered in prod)
GRAPH_HTTP_TIMEOUT=60                  # HTTP request timeout
GRAPH_QUERY_TIMEOUT=30                 # Query execution timeout

# Query Limits
GRAPH_MAX_QUERY_LENGTH=50000           # Max query size (bytes)
MCP_MAX_COMPLEXITY_SCORE=100           # Max complexity score

# Connection Pooling
MCP_POOL_MAX_CONNECTIONS=10            # Connections per graph
MCP_POOL_IDLE_TIMEOUT=300              # Idle timeout (seconds)
MCP_POOL_LIFETIME=3600                 # Connection lifetime (seconds)

# Feature Flags
MCP_ENABLE_POOLING=true                # Enable connection pooling
MCP_ENABLE_CACHING=true                # Enable schema caching
MCP_ENABLE_VALIDATION=true             # Enable query validation
```

## Integration Patterns

### With FastAPI Routes

```python
from fastapi import APIRouter, Depends
from robosystems.middleware.mcp import acquire_kuzu_mcp_client

router = APIRouter()

@router.post("/query")
async def execute_mcp_query(
    graph_id: str,
    query: str
):
    async with acquire_kuzu_mcp_client(graph_id) as client:
        result = await client.execute_query(query)
        return {"results": result}
```

### With Agent System

```python
from robosystems.middleware.mcp import KuzuMCPTools

# Initialize tools for agent
tools = KuzuMCPTools(graph_id="sec")

# Agent uses tools for natural language queries
response = await agent.execute({
    "prompt": "What were Apple's total assets in 2023?",
    "tools": tools.get_tool_definitions()
})
```

### With Celery Tasks

```python
from celery import shared_task
from robosystems.middleware.mcp import create_kuzu_mcp_client

@shared_task
async def analyze_financials(graph_id: str, company: str):
    client = await create_kuzu_mcp_client(graph_id)
    try:
        result = await client.execute_query(
            f"MATCH (c:Company {{ticker: '{company}'}}) RETURN c"
        )
        return process_results(result)
    finally:
        await client.close()
```

## Performance Optimization

### Connection Pooling Benefits

- **Reduced Latency**: Reuse existing connections (saves ~100ms per request)
- **Lower Overhead**: Avoid repeated client initialization
- **Resource Efficiency**: Limit concurrent connections
- **Automatic Cleanup**: Remove idle connections

### Best Practices

1. **Use Connection Pool**: Always use `acquire_kuzu_mcp_client` for pooled connections
2. **Set Appropriate Timeouts**: Match timeouts to query complexity
3. **Validate Queries**: Enable query validation to prevent expensive operations
4. **Cache Schema**: Enable schema caching for repeated introspection
5. **Handle Errors**: Implement proper error handling for timeouts and failures

## Monitoring

### Key Metrics

- Connection pool utilization per graph
- Query execution times (P50, P95, P99)
- Timeout rates by graph
- Error rates by exception type
- Schema cache hit rates

### Health Checks

```python
# Check MCP client health
health = await client.health_check()
print(f"Status: {health['status']}")
print(f"Latency: {health['latency_ms']}ms")
```

## Troubleshooting

### Common Issues

**1. Query Timeouts**
- Increase `GRAPH_QUERY_TIMEOUT` for complex queries
- Optimize query patterns (use LIMIT clauses)
- Check Graph API instance health

**2. Connection Pool Exhausted**
- Increase `MCP_POOL_MAX_CONNECTIONS`
- Check for connection leaks (missing context manager exits)
- Review pool lifetime settings

**3. Validation Errors**
- Check query syntax (must be valid Cypher)
- Verify query length is within limits
- Review complexity score (simplify query if needed)

**4. Authentication Failures**
- Verify graph access permissions
- Check API key validity
- Ensure user has access to graph

### Debug Mode

```python
import logging
logging.getLogger("robosystems.middleware.mcp").setLevel(logging.DEBUG)

# Provides detailed logs for:
# - Connection pool operations
# - Query execution and timing
# - Tool invocations
# - Error stack traces
```

## Security Considerations

1. **Query Validation**: Always enabled to prevent injection attacks
2. **Timeout Enforcement**: Prevents resource exhaustion
3. **Access Control**: Integration with auth middleware
4. **Audit Logging**: All queries logged for compliance
5. **Rate Limiting**: Integration with rate limiting middleware

## Related Documentation

- **[Graph API](/robosystems/graph_api/README.md)** - Underlying Graph API system
- **[Graph Middleware](/robosystems/middleware/graph/README.md)** - Graph routing layer
- **[Authentication](/robosystems/middleware/auth/README.md)** - Auth integration
- **[Configuration](/robosystems/config/README.md)** - Configuration system

## Support

For MCP-specific issues:
- Check Graph API health and connectivity
- Review query validation errors for syntax issues
- Monitor connection pool metrics
- Consult Graph API logs for backend errors
