# Neo4j Backend Service

Optional Neo4j backend support for RoboSystems Graph API, providing external graph database connectivity as an alternative to the embedded LadybugDB backend.

## Overview

The Neo4j service provides a compatibility layer for organizations that prefer Neo4j's enterprise features or have existing Neo4j infrastructure. It wraps the backend abstraction layer with service orchestration, health monitoring, and query execution.

**Status**: Optional (disabled by default)

LadybugDB is the primary backend for all standard subscription tiers. Neo4j support is available on request for enterprise customers with specific requirements.

## Architecture

```
┌────────────────────────────────────────┐
│         Neo4jService                   │  ← Service orchestration
│  (Query routing, health, monitoring)   │
└──────────────┬─────────────────────────┘
               │
┌──────────────▼─────────────────────────┐
│         Backend Abstraction            │  ← Unified interface
│  (Neo4j, LadybugDB, etc.)             │
└──────────────┬─────────────────────────┘
               │
┌──────────────▼─────────────────────────┐
│         Neo4j Driver                   │  ← Native Neo4j client
│  (Connection pool, Bolt protocol)      │
└────────────────────────────────────────┘
```

## Components

### Neo4jService (`service.py`)

High-level service orchestrator for Neo4j backend operations.

**Key Features**:
- Query execution with performance tracking
- Health monitoring with system metrics
- Cluster topology discovery
- Uptime tracking and activity logging
- Backend abstraction integration

**Core Methods**:
```python
async execute_query(request: QueryRequest) -> QueryResponse
async get_cluster_health() -> ClusterHealthResponse
async get_cluster_info() -> ClusterInfoResponse
```

## Usage

### Service Initialization

```python
from robosystems.graph_api.core.neo4j import Neo4jService

service = Neo4jService()
```

The service automatically:
1. Initializes the backend via `get_backend()`
2. Starts uptime tracking
3. Logs backend type for diagnostics

### Query Execution

```python
from robosystems.graph_api.models.database import QueryRequest

request = QueryRequest(
    database="graph123",
    cypher="MATCH (n:Entity) WHERE n.type = $type RETURN n",
    parameters={"type": "Company"}
)

response = await service.execute_query(request)

print(f"Returned {response.row_count} rows")
print(f"Execution time: {response.execution_time_ms}ms")
print(f"Columns: {response.columns}")

for row in response.data:
    print(row)
```

**Response Structure**:
```python
QueryResponse(
    data=[...],                    # List of result rows
    columns=["n"],                 # Column names
    execution_time_ms=45.67,       # Query execution time
    row_count=10,                  # Number of rows returned
    database="graph123"            # Database name
)
```

### Health Monitoring

```python
health = await service.get_cluster_health()

print(f"Status: {health.status}")              # healthy, warning, critical, unhealthy
print(f"Uptime: {health.uptime_seconds}s")
print(f"CPU: {health.cpu_percent}%")
print(f"Memory: {health.memory_percent}%")
print(f"Last activity: {health.last_activity}")
```

**Health Status Logic**:
- `unhealthy` - Backend health check failed
- `critical` - CPU > 90% or Memory > 90%
- `warning` - CPU > 75% or Memory > 75%
- `healthy` - All checks passed

### Cluster Information

```python
info = await service.get_cluster_info()

print(f"Node ID: {info.node_id}")              # backend-Neo4jBackend
print(f"Node Type: {info.node_type}")          # backend
print(f"Databases: {info.databases}")          # List of database names
print(f"Version: {info.cluster_version}")      # 1.0.0
print(f"Uptime: {info.uptime_seconds}s")
```

## Configuration

The Neo4j backend is configured through the backend abstraction layer, not directly in Neo4jService. See `/robosystems/graph_api/backends/` for backend configuration.

### Environment Variables

```bash
# Neo4j connection (when using Neo4j backend)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# Or for Aura
NEO4J_URI=neo4j+s://xxxxx.databases.neo4j.io
NEO4J_USER=neo4j
NEO4J_PASSWORD=your-password
```

## When to Use Neo4j Backend

### Use Neo4j When:

1. **Enterprise Requirements**
   - Need Neo4j's commercial support
   - Require specific Neo4j plugins (APOC, Graph Data Science)
   - Have compliance requirements for Neo4j

2. **Existing Infrastructure**
   - Already running Neo4j in production
   - Have operational expertise with Neo4j
   - Integration with Neo4j ecosystem tools

3. **Specific Features**
   - Graph algorithms from Neo4j GDS
   - Full-text search with Lucene
   - Custom Neo4j procedures

### Use LadybugDB Instead When:

1. **Standard Use Cases**
   - Graph queries, relationships, pattern matching
   - Standard Cypher operations
   - Cost-sensitive deployments

2. **Embedded Benefits**
   - Lower latency (no network overhead)
   - Simpler deployment (no separate server)
   - Better resource utilization

3. **Multi-Tenant Architecture**
   - Isolated databases per tenant
   - Resource limits per database
   - Better cost efficiency

## Performance Characteristics

### Query Execution

**Timing Components**:
```python
start_time = time.time()
result = await backend.execute_query(...)
execution_time = (time.time() - start_time) * 1000  # milliseconds
```

**Performance Factors**:
- Network latency (Bolt protocol overhead)
- Neo4j server response time
- Result serialization
- Backend connection pool efficiency

### Health Checks

**System Metrics**:
- CPU usage via `psutil.cpu_percent(interval=0.1)`
- Memory usage via `psutil.virtual_memory().percent`
- Backend connectivity via `backend.health_check()`

**Overhead**: ~100ms per health check (includes 0.1s CPU sampling)

## Error Handling

### Query Errors

```python
try:
    response = await service.execute_query(request)
except HTTPException as e:
    if e.status_code == 500:
        # Backend error, connection failure, or query execution error
        print(f"Query failed: {e.detail}")
    raise
```

**Error Types**:
- Connection failures → 500 Internal Server Error
- Query syntax errors → 500 Internal Server Error
- Backend unavailable → 500 Internal Server Error

### Health Check Errors

```python
try:
    health = await service.get_cluster_health()
except HTTPException as e:
    # Health check failed - backend is unhealthy
    print(f"Health check failed: {e.detail}")
```

## Monitoring

### Uptime Tracking

```python
uptime = service.get_uptime()  # Seconds since service initialization
```

### Last Activity

```python
if service.last_activity:
    last_query_time = service.last_activity.isoformat()
else:
    # No queries executed yet
    pass
```

### Query Metrics

Each query execution logs:
- Database name
- Query preview (first 100 chars)
- Row count returned
- Execution time in milliseconds

## Testing

### Unit Tests

```python
import pytest
from robosystems.graph_api.core.neo4j import Neo4jService
from robosystems.graph_api.models.database import QueryRequest

@pytest.mark.asyncio
async def test_query_execution():
    service = Neo4jService()

    request = QueryRequest(
        database="test",
        cypher="RETURN 1 as result",
        parameters={}
    )

    response = await service.execute_query(request)

    assert response.row_count == 1
    assert response.columns == ["result"]
    assert response.data[0]["result"] == 1
```

### Integration Tests

```python
@pytest.mark.integration
@pytest.mark.asyncio
async def test_health_monitoring():
    service = Neo4jService()

    health = await service.get_cluster_health()

    assert health.status in ["healthy", "warning", "critical", "unhealthy"]
    assert health.uptime_seconds > 0
    assert 0 <= health.cpu_percent <= 100
    assert 0 <= health.memory_percent <= 100
```

## Comparison with LadybugDB Service

| Feature | Neo4jService | LadybugService |
|---------|--------------|----------------|
| Backend | Neo4j (external) | LadybugDB (embedded) |
| Connection | Network (Bolt) | In-process |
| Latency | Higher (~5-20ms overhead) | Lower (microseconds) |
| Deployment | Separate server | Embedded library |
| Multi-tenant | Via Neo4j databases | Per-database files |
| Capacity | Limited by Neo4j license | Limited by disk space |
| Database Mgmt | Via Neo4j Admin | Direct file operations |
| Schema DDL | Via Neo4j Cypher | Direct SQL DDL |
| Cost | License + infrastructure | Infrastructure only |

## Migration Between Backends

The service layer provides a unified interface, making backend switching transparent to API consumers:

```python
# Application code is identical regardless of backend
from robosystems.graph_api.core.neo4j import Neo4jService
# OR
from robosystems.graph_api.core.ladybug import LadybugService

# Same interface, different implementations
service = Neo4jService()  # or LadybugService()
response = await service.execute_query(request)
```

## Best Practices

1. **Service Initialization**
   - Initialize once at application startup
   - Reuse service instance across requests
   - Don't create new services per request

2. **Query Execution**
   - Always use parameterized queries
   - Avoid string interpolation (security risk)
   - Handle HTTPException for errors

3. **Health Monitoring**
   - Check health periodically (every 30-60s)
   - Use status for load balancing decisions
   - Monitor uptime for service stability

4. **Error Handling**
   - Catch HTTPException for graceful failures
   - Log errors with context (database, query)
   - Retry transient failures (connection errors)

## Logging

The service uses structured logging with the `robosystems.logger` module:

```python
# Startup
logger.info("Neo4jService initialized with backend type: Neo4jBackend")

# Query execution
logger.debug("Executing query on graph123: MATCH (n)...")
logger.info("Query executed successfully on graph123: 10 rows in 45.67ms")

# Errors
logger.error("Query execution failed on graph123: Connection timeout")
logger.error("Health check failed: Neo4j server unreachable")
```

## Related Documentation

- **[Core Services README](../README.md)** - Overview of all core services
- **[LadybugDB Service](../ladybug/README.md)** - Primary embedded backend
- **[Backend Abstraction](/robosystems/graph_api/backends/README.md)** - Backend interface
- **[Graph API README](/robosystems/graph_api/README.md)** - Complete API overview

## Support

- **Source Code**: `/robosystems/graph_api/core/neo4j/`
- **Backend Code**: `/robosystems/graph_api/backends/neo4j/`
- **Issues**: [robosystems/issues](https://github.com/RoboFinSystems/robosystems/issues)
- **API Docs**: http://localhost:8001/docs
