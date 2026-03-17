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
├── client.py                # GraphMCPClient implementation
├── factory.py               # Client factory and pooling
├── pool.py                  # Connection pool management
├── query_validator.py       # Query validation and complexity checks
├── exceptions.py            # MCP-specific exception classes
└── tools/                   # MCP tool implementations
    ├── base_tool.py         # Base tool interface
    ├── manager.py           # Tool management and registry
    ├── cypher_tool.py       # Cypher query execution
    ├── schema_tool.py       # Schema introspection
    ├── resolve_element_tool.py   # Concept → XBRL element resolution (canonical matching)
    ├── example_queries_tool.py   # Query examples and templates
    ├── financial_statement_tool.py  # Get financial statements (auto-resolve reports)
    ├── data_tools.py        # Build fact grid (cross-company comparisons)
    ├── workspace.py         # Workspace/subgraph management
    └── memory.py            # Write operations (subgraph-only)
```

## Tool Layers

Tools are organized into three availability layers:

### Layer 1: Core (always available)

| Tool | Description |
|------|-------------|
| `read-graph-cypher` | Execute read-only Cypher queries with validation |
| `get-graph-schema` | Get complete database schema (cached 60s) |

### Layer 2: Schema Extensions (require `roboledger` in `schema_extensions`)

| Tool | Description |
|------|-------------|
| `get-example-queries` | Working query patterns tailored to the graph schema |
| `resolve-element` | Map concepts ("revenue") to XBRL element qnames via canonical matching |
| `get-financial-statement` | Structured statement data with auto-resolve and dedup |
| `build-fact-grid` | Cross-company comparisons via canonical concepts |

`resolve-element` additionally requires `has_semantic_enrichment=True` on the manifest.

### Layer 3: Infrastructure (feature-flag gated)

| Tool | Flag | Description |
|------|------|-------------|
| `create-workspace` | `MCP_WORKSPACE_ENABLED` | Create subgraph workspace |
| `delete-workspace` | `MCP_WORKSPACE_ENABLED` | Delete workspace |
| `list-workspaces` | `MCP_WORKSPACE_ENABLED` | List available workspaces |
| `switch-workspace` | `MCP_WORKSPACE_ENABLED` | Switch active workspace context |
| `write-graph-cypher` | `MCP_MEMORY_ENABLED` | Execute write Cypher (subgraphs only) |
| `add-node-table` | `MCP_MEMORY_ENABLED` | Create staging table for nodes |
| `add-relationship-table` | `MCP_MEMORY_ENABLED` | Create staging table for relationships |

## Key Components

### 1. GraphMCPClient (`client.py`)

Main MCP client for interacting with graph databases through the RoboSystems API.

**Features:**

- HTTP-based communication with Graph API
- Automatic timeout and retry handling
- Query complexity validation
- Streaming support for large results
- Schema caching for performance

**Usage:**

```python
from robosystems.middleware.mcp import create_graph_mcp_client

# Create client with automatic endpoint discovery
client = await create_graph_mcp_client(graph_id="sec")

# Execute Cypher query
result = await client.execute_query(
    "MATCH (e:Entity) WHERE e.ticker = 'AAPL' RETURN e"
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
from robosystems.middleware.mcp import acquire_graph_mcp_client

# Acquire client from pool (recommended)
async with acquire_graph_mcp_client(graph_id="kg1a2b3c") as client:
    result = await client.execute_query("MATCH (n) RETURN count(n)")
    # Client automatically returned to pool
```

### 3. MCP Tools (`tools/`)

All tools use `self.client.execute_query()` for consistent routing, auth, and error handling.

**Key patterns:**

- **Auto-resolve**: `get-financial-statement` automatically finds the latest relevant report when no `report_id` is provided
- **Canonical matching**: `resolve-element` uses in-memory embedding match to map concepts to XBRL elements
- **Deduplication**: Financial tools deduplicate facts that appear in multiple filings (comparative periods)
- **Parameterized queries**: All tools use `$param` syntax to prevent injection

### 4. Query Validation (`query_validator.py`)

Validates query complexity and enforces limits to prevent resource exhaustion.

**Features:**

- Query length validation (max 50KB)
- Complexity scoring based on query patterns
- Timeout enforcement (30 seconds default)
- Result size limits
- Protection against expensive operations

### 5. Exception Handling (`exceptions.py`)

Comprehensive exception hierarchy for MCP operations.

**Exception Classes:**

- `GraphAPIError` - Base exception for all MCP errors
- `GraphQueryTimeoutError` - Query exceeded timeout
- `GraphQueryComplexityError` - Query too complex
- `GraphValidationError` - Invalid query or parameters
- `GraphAuthenticationError` - Authentication failed
- `GraphAuthorizationError` - Insufficient permissions
- `GraphConnectionError` - Connection to Graph API failed
- `GraphResourceNotFoundError` - Resource not found
- `GraphRateLimitError` - Rate limit exceeded
- `LadybugDBSchemaError` - Schema validation failed

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
MCP_WORKSPACE_ENABLED=false            # Enable workspace tools
MCP_MEMORY_ENABLED=false               # Enable write/memory tools
FACT_GRID_ENABLED=false                # Enable build-fact-grid tool
```

## Integration Patterns

### With FastAPI Routes

```python
from fastapi import APIRouter, Depends
from robosystems.middleware.mcp import acquire_graph_mcp_client

router = APIRouter()

@router.post("/query")
async def execute_mcp_query(
    graph_id: str,
    query: str
):
    async with acquire_graph_mcp_client(graph_id) as client:
        result = await client.execute_query(query)
        return {"results": result}
```

### With Agent System

```python
from robosystems.middleware.mcp import GraphMCPTools

# Initialize tools for agent
tools = GraphMCPTools(graph_id="sec")

# Agent uses tools for natural language queries
response = await agent.execute({
    "prompt": "What were Apple's total assets in 2023?",
    "tools": tools.get_tool_definitions()
})
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

### Debug Mode

```python
import logging
logging.getLogger("robosystems.middleware.mcp").setLevel(logging.DEBUG)
```

## Related Documentation

- **[Graph API](/robosystems/graph_api/README.md)** - Underlying Graph API system
- **[Graph Middleware](/robosystems/middleware/graph/README.md)** - Graph routing layer
- **[Authentication](/robosystems/middleware/auth/README.md)** - Auth integration
- **[Configuration](/robosystems/config/README.md)** - Configuration system
