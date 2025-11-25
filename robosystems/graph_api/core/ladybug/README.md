# LadybugDB Core Services

Embedded graph database services for LadybugDB, providing low-level engine access, connection pooling, database lifecycle management, and service orchestration.

## Overview

The `ladybug/` directory contains all LadybugDB-specific functionality organized in four layers:

1. **Engine** (`engine.py`) - Low-level database driver
2. **Connection Pool** (`pool.py`) - Connection lifecycle management
3. **Database Manager** (`manager.py`) - Database operations and schema
4. **Service** (`service.py`) - High-level orchestration

## Architecture

```
┌─────────────────────────────────────────────┐
│           LadybugService                    │  ← High-level orchestration
│  (Query execution, health, cluster info)    │
└──────────────┬──────────────────────────────┘
               │
┌──────────────▼──────────────────────────────┐
│      LadybugDatabaseManager                 │  ← Database lifecycle
│  (Create, delete, schema, query routing)    │
└──────────────┬──────────────────────────────┘
               │
┌──────────────▼──────────────────────────────┐
│      LadybugConnectionPool                  │  ← Connection management
│  (Pooling, TTL, health checks, cleanup)     │
└──────────────┬──────────────────────────────┘
               │
┌──────────────▼──────────────────────────────┐
│            Engine                           │  ← Low-level driver
│  (Cypher execution, DDL, transactions)      │
└─────────────────────────────────────────────┘
```

## Components

### 1. Engine (`engine.py`)

Low-level database driver providing direct access to LadybugDB.

**Classes**:
- `Engine` - Main database connection and query execution
- `Repository` - High-level repository abstraction
- `ConnectionError` - Connection-related exceptions
- `QueryError` - Query execution exceptions

**Key Features**:
- Direct embedded database access
- Cypher query execution with parameters
- DDL schema operations
- Transaction support
- Error handling and logging

**Usage**:
```python
from robosystems.graph_api.core.ladybug import Engine, ConnectionError, QueryError

# Create engine for specific database
engine = Engine("/data/lbug-dbs/kg123.lbug")

try:
    # Execute parameterized query
    result = engine.execute_query(
        "MATCH (n:Entity) WHERE n.id = $id RETURN n",
        {"id": "entity-123"}
    )

    # Process results
    schema = result.get_schema()
    while result.has_next():
        row = result.get_next()
        print(row)

except QueryError as e:
    print(f"Query failed: {e}")
finally:
    engine.close()
```

**Best Practices**:
- Always close engines when done
- Use parameterized queries to prevent injection
- Handle `ConnectionError` and `QueryError` separately
- Don't create engines directly in request handlers (use connection pool)

### 2. Connection Pool (`pool.py`)

Efficient connection pooling with automatic lifecycle management.

**Classes**:
- `LadybugConnectionPool` - Main connection pool
- `ConnectionInfo` - Connection metadata and stats

**Key Features**:
- Per-database connection pools
- Configurable max connections, idle timeout, TTL
- Automatic connection cleanup
- Thread-safe with proper locking
- LRU eviction policy
- Health checks before use

**Configuration**:
```python
from robosystems.graph_api.core.ladybug import (
    LadybugConnectionPool,
    initialize_connection_pool
)

# Initialize global pool (recommended)
pool = initialize_connection_pool(
    base_path="/data/lbug-dbs",
    max_connections_per_db=10,      # Max connections per database
    idle_timeout_minutes=15,         # Close idle connections after 15min
    connection_ttl_minutes=60        # Force close after 1 hour
)

# Use connection (context manager auto-returns to pool)
with pool.get_connection("kg123") as conn:
    result = conn.execute("MATCH (n) RETURN n LIMIT 10")
```

**Connection Lifecycle**:
1. **Request** - Application requests connection for database
2. **Check Pool** - Pool checks for available connections
3. **Reuse or Create** - Returns existing or creates new connection
4. **Health Check** - Validates connection before returning
5. **Use** - Application uses connection
6. **Return** - Context manager returns connection to pool
7. **Cleanup** - Background thread closes idle/expired connections

**Monitoring**:
```python
# Get pool statistics
stats = pool.get_stats()
print(f"Total databases: {stats['total_databases']}")
print(f"Connections created: {stats['connections_created']}")
print(f"Connections reused: {stats['connections_reused']}")
print(f"Current active: {stats['current_active']}")
```

**Best Practices**:
- Always use context managers for connections
- Set `max_connections_per_db` based on expected concurrency
- Configure TTL to prevent stale connections
- Monitor pool stats for sizing decisions
- Close all connections before deleting database

### 3. Database Manager (`manager.py`)

Complete database lifecycle management including creation, deletion, and schema operations.

**Classes**:
- `LadybugDatabaseManager` - Main database manager

**Key Features**:
- Database creation with schema initialization
- Database deletion with cleanup
- Multi-database support per instance
- Schema DDL execution
- Database info and health checks
- Connection pool integration

**Database Operations**:
```python
from robosystems.graph_api.core.ladybug import LadybugDatabaseManager
from robosystems.graph_api.models.database import DatabaseCreateRequest

manager = LadybugDatabaseManager(
    base_path="/data/lbug-dbs",
    max_databases=100,
    read_only=False
)

# Create database
response = manager.create_database(
    graph_id="kg123",
    schema_type="entity",      # or "shared", "custom"
    read_only=False,
    custom_schema_ddl=None     # Optional custom DDL
)

# Get database information
info = manager.get_database_info("kg123")
print(f"Size: {info.size_bytes} bytes")
print(f"Healthy: {info.is_healthy}")

# List all databases
databases = manager.list_databases()

# Delete database
result = manager.delete_database("kg123", force=True)
```

**Schema Types**:
- `entity` - Standard entity-relationship schema
- `shared` - Shared repository schema (SEC, industry data)
- `custom` - User-provided custom DDL

**Database Limits**:
```python
# Check capacity
capacity = manager.get_node_capacity()
print(f"Max databases: {capacity['max_databases']}")
print(f"Current: {capacity['current_databases']}")
print(f"Remaining: {capacity['capacity_remaining']}")
```

**Best Practices**:
- Validate schema before creation
- Use `force=True` only when necessary for deletion
- Close connections before deleting database
- Monitor capacity and adjust `max_databases`
- Use read-only mode for reader instances

### 4. Service (`service.py`)

High-level service orchestration coordinating all LadybugDB operations.

**Classes**:
- `LadybugService` - Main service orchestrator

**Key Features**:
- Unified API for all operations
- Query execution with metrics
- Health monitoring and resource tracking
- Cluster information and topology
- Service discovery and registration
- Integration with database manager and connection pool

**Service Initialization**:
```python
from robosystems.graph_api.core.ladybug import (
    init_ladybug_service,
    get_ladybug_service
)
from robosystems.middleware.graph.types import NodeType, RepositoryType

# Initialize service (once at startup)
service = init_ladybug_service(
    base_path="/data/lbug-dbs",
    max_databases=100,
    read_only=False,
    node_type=NodeType.WRITER,           # or READER, SHARED_MASTER
    repository_type=RepositoryType.ENTITY # or SHARED
)

# Get service instance (anywhere in app)
service = get_ladybug_service()
```

**Query Execution**:
```python
from robosystems.graph_api.models.database import QueryRequest

# Execute Cypher query
response = service.execute_query(QueryRequest(
    database="kg123",
    cypher="MATCH (n:Entity) WHERE n.type = $type RETURN n",
    parameters={"type": "Company"}
))

print(f"Returned {response.row_count} rows")
print(f"Columns: {response.columns}")
print(f"Execution time: {response.execution_time_ms}ms")

for row in response.data:
    print(row)
```

**Health Monitoring**:
```python
# Get cluster health
health = service.get_cluster_health()
print(f"Status: {health.status}")            # healthy, warning, critical
print(f"CPU: {health.cpu_percent}%")
print(f"Memory: {health.memory_percent}%")
print(f"Databases: {health.current_databases}/{health.max_databases}")
print(f"Uptime: {health.uptime_seconds}s")
```

**Cluster Information**:
```python
# Get cluster info
info = service.get_cluster_info()
print(f"Node ID: {info.node_id}")
print(f"Node Type: {info.node_type}")
print(f"Version: {info.cluster_version}")
print(f"Databases: {info.databases}")
print(f"Read-only: {info.read_only}")
```

**Best Practices**:
- Initialize service once at startup
- Use `get_ladybug_service()` to access service instance
- Monitor health regularly for capacity planning
- Configure appropriate node type and repository type
- Use read-only mode for reader instances

## Configuration

### Environment Variables

```bash
# Database directory
LBUG_DATABASE_DIR=/data/lbug-dbs

# Instance limits
LBUG_MAX_DATABASES_PER_NODE=100

# Connection pooling
LBUG_MAX_CONNECTIONS_PER_DB=10
LBUG_IDLE_TIMEOUT_MINUTES=15
LBUG_CONNECTION_TTL_MINUTES=60

# Node configuration
LBUG_NODE_TYPE=writer                 # writer, reader, shared_master
LBUG_REPOSITORY_TYPE=entity           # entity, shared
LBUG_READ_ONLY=false

# Performance tuning
LBUG_QUERY_TIMEOUT_SECONDS=300
LBUG_MAX_QUERY_RESULT_SIZE=10000      # Max rows returned
```

### Programmatic Configuration

```python
from robosystems.graph_api.core.ladybug import (
    init_ladybug_service,
    initialize_connection_pool
)

# Initialize connection pool first
pool = initialize_connection_pool(
    base_path="/data/lbug-dbs",
    max_connections_per_db=10,
    idle_timeout_minutes=15,
    connection_ttl_minutes=60
)

# Initialize service
service = init_ladybug_service(
    base_path="/data/lbug-dbs",
    max_databases=100,
    read_only=False,
    node_type=NodeType.WRITER,
    repository_type=RepositoryType.ENTITY
)
```

## Performance Tuning

### Connection Pool Sizing

```python
# Low concurrency (< 10 concurrent requests)
max_connections_per_db = 5

# Medium concurrency (10-50 concurrent requests)
max_connections_per_db = 10

# High concurrency (50+ concurrent requests)
max_connections_per_db = 20
```

### Query Optimization

```python
# Use parameterized queries
good = engine.execute_query(
    "MATCH (n:Entity) WHERE n.id = $id RETURN n",
    {"id": entity_id}
)

# Avoid string interpolation (SQL injection risk + no caching)
bad = engine.execute_query(
    f"MATCH (n:Entity) WHERE n.id = '{entity_id}' RETURN n"
)

# Use LIMIT for large result sets
query = "MATCH (n:Entity) RETURN n LIMIT 1000"

# Use indexes for common queries
schema_ddl = """
CREATE NODE TABLE Entity (
    id STRING PRIMARY KEY,
    name STRING,
    type STRING
);
CREATE INDEX entity_type_idx ON Entity(type);
"""
```

## Error Handling

### Connection Errors

```python
from robosystems.graph_api.core.ladybug import ConnectionError

try:
    with pool.get_connection("kg123") as conn:
        result = conn.execute(query)
except ConnectionError as e:
    logger.error(f"Connection failed: {e}")
    # Retry or return 503 Service Unavailable
```

### Query Errors

```python
from robosystems.graph_api.core.ladybug import QueryError

try:
    result = engine.execute_query(cypher, params)
except QueryError as e:
    logger.error(f"Query failed: {e}")
    # Return 400 Bad Request or 500 Internal Server Error
```

### Database Not Found

```python
from fastapi import HTTPException

if graph_id not in manager.list_databases():
    raise HTTPException(
        status_code=404,
        detail=f"Database {graph_id} not found"
    )
```

## Testing

### Unit Tests

```python
from robosystems.graph_api.core.ladybug import Engine

def test_engine_query_execution():
    engine = Engine("/tmp/test.lbug")
    result = engine.execute_query("MATCH (n) RETURN count(n) as count")
    assert result is not None
    engine.close()
```

### Integration Tests

```python
from robosystems.graph_api.core.ladybug import (
    LadybugConnectionPool,
    LadybugDatabaseManager
)

def test_connection_pool_integration():
    pool = LadybugConnectionPool("/tmp/test-dbs", max_connections_per_db=5)
    manager = LadybugDatabaseManager("/tmp/test-dbs", 10)

    # Create database
    manager.create_database("test", "entity", False)

    # Get connection
    with pool.get_connection("test") as conn:
        result = conn.execute("MATCH (n) RETURN count(n)")
        assert result is not None

    # Cleanup
    pool.close_all_connections()
    manager.delete_database("test", force=True)
```

## Monitoring

### Connection Pool Metrics

```python
stats = pool.get_stats()
metrics = {
    "total_databases": stats["total_databases"],
    "connections_created": stats["connections_created"],
    "connections_reused": stats["connections_reused"],
    "current_active": stats["current_active"],
    "reuse_rate": stats["connections_reused"] / stats["connections_created"]
}
```

### Database Metrics

```python
info = manager.get_database_info(graph_id)
metrics = {
    "size_bytes": info.size_bytes,
    "is_healthy": info.is_healthy,
    "created_at": info.created_at,
    "last_accessed": info.last_accessed
}
```

### Service Metrics

```python
health = service.get_cluster_health()
metrics = {
    "status": health.status,
    "cpu_percent": health.cpu_percent,
    "memory_percent": health.memory_percent,
    "database_utilization": health.current_databases / health.max_databases,
    "uptime_seconds": health.uptime_seconds
}
```

## Related Documentation

- **[Core Services README](../README.md)** - Overview of all core services
- **[Neo4j Backend](../neo4j/README.md)** - Neo4j backend documentation
- **[DuckDB Staging](../duckdb/README.md)** - DuckDB staging system
- **[Graph API README](/robosystems/graph_api/README.md)** - Complete API overview

## Support

- **Source Code**: `/robosystems/graph_api/core/ladybug/`
- **Issues**: [robosystems/issues](https://github.com/RoboFinSystems/robosystems/issues)
- **API Docs**: http://localhost:8001/docs
