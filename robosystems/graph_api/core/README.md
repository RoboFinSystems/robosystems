# Graph API Core Services

Core services layer for Graph API operations, providing database management, connection pooling, data staging, admission control, and observability.

## Overview

The `core/` directory contains the foundational services that power the Graph API, organized by database technology:

- **LadybugDB Services** - Embedded graph database management, connection pooling, and service orchestration
- **Neo4j Services** - Backend Neo4j cluster management (optional)
- **DuckDB Services** - High-performance data staging via intermediate tables
- **Shared Services** - Admission control, task management, and metrics collection

## Directory Structure

```
core/
├── README.md                       # This file
├── __init__.py                    # Core service exports
│
├── ladybug/                       # LadybugDB embedded graph database
│   ├── __init__.py               # LadybugDB exports
│   ├── engine.py                 # Low-level database driver (18KB)
│   ├── pool.py                   # Connection pooling (26KB)
│   ├── manager.py                # Database lifecycle and schema (35KB)
│   └── service.py                # Service orchestration (35KB)
│
├── neo4j/                         # Neo4j backend (optional)
│   ├── __init__.py               # Neo4j exports
│   └── service.py                # Neo4j cluster management (4KB)
│
├── duckdb/                        # DuckDB data staging
│   ├── __init__.py               # DuckDB exports
│   ├── pool.py                   # DuckDB connection pooling (21KB)
│   └── manager.py                # Staging table management (20KB)
│
└── [shared services]              # Technology-agnostic services
    ├── admission_control.py      # CPU/memory-based backpressure (6KB)
    ├── metrics_collector.py      # Performance monitoring (13KB)
    ├── task_manager.py           # Async task coordination (5KB)
    ├── task_sse.py               # Server-Sent Events (7KB)
    └── utils.py                  # Shared utilities (4KB)
```

## Technology Stack

### LadybugDB (Primary Backend)

**Location**: `core/ladybug/`

LadybugDB is our primary embedded graph database, providing high-performance graph operations with direct local access:

**Architecture Layers**:
1. **Engine** (`engine.py`) - Low-level driver interfacing with the embedded database
2. **Connection Pool** (`pool.py`) - Efficient connection reuse and lifecycle management
3. **Database Manager** (`manager.py`) - Database lifecycle, schema management, and queries
4. **Service** (`service.py`) - High-level orchestration and cluster coordination

**Key Features**:
- **Embedded Architecture** - No network overhead, direct file system access
- **Multi-Database Support** - Multiple independent databases per instance
- **Connection Pooling** - Automatic resource management and cleanup
- **Schema Management** - DDL execution and validation
- **Thread Safety** - Safe concurrent access with proper locking

**Usage**:
```python
from robosystems.graph_api.core.ladybug import (
    Engine,
    LadybugConnectionPool,
    LadybugDatabaseManager,
    LadybugService,
    get_ladybug_service
)

# High-level service access (recommended)
service = get_ladybug_service()
response = service.execute_query(QueryRequest(
    database="kg123",
    cypher="MATCH (n) RETURN n LIMIT 10"
))

# Low-level engine access (for advanced use cases)
engine = Engine("/data/lbug-dbs/kg123.lbug")
result = engine.execute_query("MATCH (n:Entity) RETURN n.name")
```

### Neo4j (Optional Backend)

**Location**: `core/neo4j/`

Optional Neo4j backend support for clusters requiring distributed graph capabilities:

**Primary Class**: `Neo4jService`

**Key Features**:
- **Backend Abstraction** - Unified interface for Neo4j clusters
- **Health Monitoring** - Continuous health checks
- **Cluster Topology** - Discover and route to cluster members

**Usage**:
```python
from robosystems.graph_api.core.neo4j import Neo4jService

service = Neo4jService()
health = service.get_cluster_health()
info = service.get_cluster_info()
```

### DuckDB (Data Staging)

**Location**: `core/duckdb/`

High-performance data staging system for validating and preparing data before graph ingestion:

**Primary Classes**:
- `DuckDBConnectionPool` - Connection pooling for DuckDB
- `DuckDBTableManager` - Staging table creation and querying

**Key Features**:
- **S3 Integration** - Direct access to S3 Parquet files via httpfs
- **SQL Validation** - Query and validate data before ingestion
- **Automatic Deduplication** - Node and relationship table deduplication
- **Materialized Tables** - Fast querying of staged data

**Workflow**:
```
User Uploads → S3 Storage → DuckDB Staging → Validation → Graph Database
```

**Usage**:
```python
from robosystems.graph_api.core.duckdb import (
    DuckDBTableManager,
    DuckDBConnectionPool
)

manager = DuckDBTableManager(staging_path="./data/staging")
manager.create_table(
    graph_id="kg123",
    table_name="entities",
    s3_pattern="s3://bucket/data/*.parquet"
)

# Validate data with SQL
result = manager.query(
    graph_id="kg123",
    sql="SELECT COUNT(*) FROM entities WHERE name IS NOT NULL"
)
```

## Core Services

### 1. LadybugDB Engine

**File**: `ladybug/engine.py`

Low-level driver for LadybugDB database operations:

**Key Classes**:
- `Engine` - Main database connection and query execution
- `Repository` - High-level repository abstraction

**Key Features**:
- **Direct Database Access** - Embedded database without network overhead
- **Cypher Query Execution** - Full Cypher query language support
- **Parameterized Queries** - Safe query execution with parameters
- **Error Handling** - Comprehensive exception handling
- **Schema Operations** - DDL execution for schema management

**Usage**:
```python
from robosystems.graph_api.core.ladybug import Engine

engine = Engine("/data/lbug-dbs/kg123.lbug")
result = engine.execute_query(
    "MATCH (n:Entity) WHERE n.id = $id RETURN n",
    {"id": "entity-123"}
)
```

### 2. Connection Pooling

**File**: `ladybug/pool.py`

Efficient connection pooling for LadybugDB with automatic lifecycle management:

**Primary Class**: `LadybugConnectionPool`

**Key Features**:
- **Resource Efficiency** - Reuse connections across requests
- **Automatic Cleanup** - Idle connection cleanup with configurable TTL
- **Thread Safety** - Safe concurrent access with proper locking
- **Health Checks** - Connection validation before use
- **Per-Database Pools** - Separate connection pools per database
- **LRU Eviction** - Least-recently-used connection eviction

**Configuration**:
```python
from robosystems.graph_api.core.ladybug import LadybugConnectionPool

pool = LadybugConnectionPool(
    base_path="/data/lbug-dbs",
    max_connections_per_db=5,
    idle_timeout_minutes=15,
    connection_ttl_minutes=60
)

with pool.get_connection("kg123") as conn:
    result = conn.execute("MATCH (n) RETURN n LIMIT 10")
```

### 3. Database Management

**File**: `ladybug/manager.py`

Complete database lifecycle management including creation, deletion, schema management, and querying:

**Primary Class**: `LadybugDatabaseManager`

**Key Features**:
- **Database Lifecycle** - Create, delete, and manage graph databases
- **Schema Management** - DDL execution and validation
- **Multi-Database Support** - Handle multiple databases per instance
- **Connection Pool Integration** - Automatic connection management
- **Database Info** - Size, health, and metadata queries

**Usage**:
```python
from robosystems.graph_api.core.ladybug import LadybugDatabaseManager

manager = LadybugDatabaseManager(
    base_path="/data/lbug-dbs",
    max_databases=100
)

# Create database with schema
response = manager.create_database(
    graph_id="kg123",
    schema_type="entity",
    read_only=False
)

# Get database information
info = manager.get_database_info("kg123")
```

### 4. LadybugDB Service

**File**: `ladybug/service.py`

High-level service orchestration coordinating all LadybugDB operations:

**Primary Class**: `LadybugService`

**Key Features**:
- **Unified Interface** - Single entry point for all operations
- **Query Execution** - Cypher query execution with metrics
- **Health Monitoring** - System health and resource tracking
- **Cluster Information** - Node metadata and topology
- **Service Discovery** - Node identification and registration

**Usage**:
```python
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.models.database import QueryRequest

service = get_ladybug_service()

# Execute query
response = service.execute_query(QueryRequest(
    database="kg123",
    cypher="MATCH (n:Entity) RETURN n.name, n.id LIMIT 10",
    parameters={}
))

# Get cluster health
health = service.get_cluster_health()

# Get cluster information
info = service.get_cluster_info()
```

### 5. DuckDB Staging System

**Files**: `duckdb/manager.py`, `duckdb/pool.py`

High-performance data ingestion via intermediate DuckDB staging tables:

**Primary Classes**:
- `DuckDBTableManager` - Staging table creation and query execution
- `DuckDBConnectionPool` - DuckDB connection pooling

**Key Features**:
- **S3 Integration** - Direct S3 file access via httpfs extension
- **SQL Validation** - Query and validate data before graph ingestion
- **Materialized Tables** - Fast querying of staged Parquet files
- **Automatic Deduplication** - Node and relationship table deduplication
- **Connection Pooling** - Efficient DuckDB connection management

**Usage**:
```python
from robosystems.graph_api.core.duckdb import DuckDBTableManager

manager = DuckDBTableManager(staging_path="./data/staging")

# Create staging table from S3
manager.create_table(
    graph_id="kg123",
    table_name="Entity",
    s3_pattern="s3://bucket/entities/*.parquet",
    columns=["id", "name", "type"]
)

# Query staging table
result = manager.query(
    graph_id="kg123",
    sql="SELECT type, COUNT(*) as count FROM Entity GROUP BY type"
)
```

### 6. Admission Control

**File**: `admission_control.py`

CPU and memory-based backpressure management to prevent system overload:

**Primary Class**: `AdmissionController`

**Key Features**:
- **Resource Monitoring** - Track CPU and memory utilization
- **Adaptive Throttling** - Reject requests when resources are constrained
- **Configurable Thresholds** - CPU and memory warning/critical levels
- **Graceful Degradation** - Return 503 Service Unavailable during overload

**Configuration**:
```python
from robosystems.graph_api.core.admission_control import AdmissionController

admission = AdmissionController(
    cpu_warning=70.0,
    cpu_critical=85.0,
    memory_warning=75.0,
    memory_critical=90.0
)

# Check before processing request
if not admission.should_admit():
    raise HTTPException(status_code=503, detail="Service overloaded")
```

### 7. Task Management

**Files**: `task_manager.py`, `task_sse.py`

Async operation coordination with Server-Sent Events for real-time progress:

**Primary Classes**:
- `TaskManager` - Task coordination and tracking
- `TaskSSE` - Server-Sent Events for progress streaming

**Key Features**:
- **Task Tracking** - Manage long-running operations
- **Progress Updates** - Real-time status via Server-Sent Events
- **Redis-Backed** - Distributed task state storage
- **Automatic Cleanup** - Task lifecycle management

**Usage**:
```python
from robosystems.graph_api.core.task_manager import (
    backup_task_manager,
    restore_task_manager
)

# Create backup task
task_id = await backup_task_manager.create_task(
    task_type="backup",
    metadata={"database": "kg123"}
)

# Update progress
await backup_task_manager.update_task(
    task_id,
    status="running",
    metadata={"progress": 50, "message": "Compressing backup..."}
)

# Complete task
await backup_task_manager.complete_task(
    task_id,
    result={"backup_size": 1024000, "location": "s3://..."}
)
```

### 8. Metrics Collection

**File**: `metrics_collector.py`

Performance monitoring and observability for graph operations:

**Primary Class**: `LadybugMetricsCollector`

**Key Features**:
- **Query Metrics** - Execution time, row counts, error rates
- **Database Metrics** - Size, table counts, connection stats
- **System Metrics** - CPU, memory, disk usage
- **Time-Series Data** - Historical performance tracking

**Usage**:
```python
from robosystems.graph_api.core.metrics_collector import LadybugMetricsCollector

metrics = LadybugMetricsCollector()
metrics.record_query("kg123", cypher_query, execution_time, row_count)
stats = metrics.get_database_stats("kg123")
```

## Architecture Patterns

### Service Initialization

Core services are initialized at application startup:

```python
from robosystems.graph_api.core import (
    init_ladybug_service,
    initialize_connection_pool
)
from robosystems.middleware.graph.types import NodeType, RepositoryType

# Initialize LadybugDB service
ladybug_service = init_ladybug_service(
    base_path="/data/lbug-dbs",
    max_databases=100,
    read_only=False,
    node_type=NodeType.WRITER,
    repository_type=RepositoryType.ENTITY
)

# Initialize connection pool
connection_pool = initialize_connection_pool(
    base_path="/data/lbug-dbs",
    max_connections_per_db=10,
    idle_timeout_minutes=15
)
```

### Request Flow

1. **API Request** → Routers receive HTTP request
2. **Authentication** → Middleware validates JWT/API key
3. **Authorization** → Verify graph access permissions
4. **Service Routing** → `LadybugService` coordinates operation
5. **Admission Check** → `AdmissionController` verifies resources
6. **Connection Acquisition** → `ConnectionPool` provides connection
7. **Query Execution** → `Engine` executes Cypher query
8. **Metrics Collection** → `MetricsCollector` records performance
9. **Response** → Results returned to client

### Data Ingestion Flow

1. **Upload** → Parquet files to S3 (via API)
2. **Staging** → `DuckDBTableManager` creates materialized table from S3
3. **Validation** → User queries staging table with SQL
4. **Transformation** → Optional data cleanup and validation
5. **Ingestion** → Copy from DuckDB staging to LadybugDB graph
6. **Verification** → Confirm data integrity in graph
7. **Cleanup** → Staging table remains for incremental updates

### Multi-Tenancy Pattern

Each graph database is isolated and independently managed:

```python
# Each tenant gets their own database
tenant_a_db = "kg1a2b3c4d5e6f7g8"  # 16-char hex ID
tenant_b_db = "kg9h8i7j6k5l4m3n"  # Separate database

# Databases are isolated at file system level
# /data/lbug-dbs/kg1a2b3c4d5e6f7g8.lbug
# /data/lbug-dbs/kg9h8i7j6k5l4m3n.lbug

# Connection pools are per-database
with pool.get_connection(tenant_a_db) as conn:
    # Tenant A operations
    pass

with pool.get_connection(tenant_b_db) as conn:
    # Tenant B operations (completely isolated)
    pass
```

## Configuration

Core services are configured via environment variables:

### LadybugDB Configuration
```bash
# Database directory
LBUG_DATABASE_DIR=/data/lbug-dbs

# Instance limits
LBUG_MAX_DATABASES_PER_NODE=100

# Node configuration
LBUG_NODE_TYPE=writer                    # writer, reader, shared_master
LBUG_REPOSITORY_TYPE=entity              # entity, shared
LBUG_READ_ONLY=false
```

### Connection Pooling
```bash
# Pool configuration
LBUG_MAX_CONNECTIONS_PER_DB=10
LBUG_IDLE_TIMEOUT_MINUTES=15
LBUG_CONNECTION_TTL_MINUTES=60

# Health checks
LBUG_HEALTH_CHECK_INTERVAL=30
```

### DuckDB Staging
```bash
# Staging configuration
DUCKDB_STAGING_PATH=./data/staging
DUCKDB_MAX_MEMORY=4GB
DUCKDB_THREADS=4

# S3 integration
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
```

### Admission Control
```bash
# Resource thresholds
CPU_WARNING_THRESHOLD=70
CPU_CRITICAL_THRESHOLD=85
MEMORY_WARNING_THRESHOLD=75
MEMORY_CRITICAL_THRESHOLD=90
```

### Task Management
```bash
# Redis configuration for task state
VALKEY_URL=redis://localhost:6379
TASK_MANAGER_DB=3
TASK_TTL_SECONDS=3600
```

## Best Practices

### LadybugDB Operations
- **Always use connection pooling** - Never create direct Engine instances in request handlers
- **Set appropriate database limits** - Configure `max_databases` based on instance capacity
- **Monitor resource usage** - Track CPU, memory, and disk I/O
- **Use read-only mode** - Enable for reader instances to prevent writes

### Connection Pooling
- **Configure pool sizes** - Set `max_connections_per_db` based on expected concurrency
- **Set reasonable TTLs** - Balance between connection reuse and resource cleanup
- **Monitor pool metrics** - Track connection creation, reuse, and eviction rates
- **Handle connection errors** - Implement retry logic for transient failures

### Database Lifecycle
- **Validate schema before creation** - Use schema validation before creating databases
- **Close connections on deletion** - Always close all connections before deleting databases
- **Use atomic operations** - Ensure database operations are atomic and recoverable
- **Implement proper cleanup** - Clean up resources on database deletion

### DuckDB Staging
- **Validate before ingestion** - Always query and validate staged data before graph ingestion
- **Use materialized tables** - Don't use views for ingestion (poor performance)
- **Keep staging tables** - Retain tables for incremental loading and reprocessing
- **Monitor staging disk usage** - Clean up old staging tables periodically

### Admission Control
- **Set appropriate thresholds** - Configure based on actual instance capacity
- **Monitor continuously** - Track resource trends to adjust thresholds
- **Implement graceful degradation** - Return meaningful errors during overload
- **Test under load** - Validate admission control under realistic load conditions

### Error Handling
- **Catch specific exceptions** - Handle `ConnectionError`, `QueryError` separately
- **Log errors with context** - Include graph_id, query, and operation details
- **Implement retries** - Retry transient failures with exponential backoff
- **Return user-friendly errors** - Sanitize error messages before returning to client

## Migration Guide

### Upgrading from Old Structure

If you're migrating from the old flat structure, update your imports:

**Old imports (deprecated)**:
```python
from robosystems.graph_api.core.ladybug_service import LadybugService
from robosystems.graph_api.core.connection_pool import LadybugConnectionPool
from robosystems.graph_api.core.database_manager import LadybugDatabaseManager
from robosystems.graph_api.core.backend_cluster_manager import BackendClusterService
```

**New imports (current)**:
```python
from robosystems.graph_api.core.ladybug import (
    LadybugService,
    LadybugConnectionPool,
    LadybugDatabaseManager
)
from robosystems.graph_api.core.neo4j import Neo4jService  # formerly BackendClusterService
```

**Backward compatibility**: The old imports still work via aliases in `core/__init__.py`, but should be updated to use the new structure.

## Related Documentation

- **[Graph API README](/robosystems/graph_api/README.md)** - Complete Graph API overview
- **[LadybugDB README](ladybug/README.md)** - Detailed LadybugDB documentation
- **[Neo4j Backend README](neo4j/README.md)** - Neo4j backend documentation
- **[DuckDB Staging README](duckdb/README.md)** - DuckDB staging system documentation
- **[Backends](/robosystems/graph_api/backends/README.md)** - Backend abstraction layer
- **[Client Factory](/robosystems/graph_api/client/README.md)** - Client routing system

## Support

- **Issues**: [robosystems/issues](https://github.com/RoboFinSystems/robosystems/issues)
- **Source Code**: `/robosystems/graph_api/core/`
- **API Docs**: http://localhost:8001/docs
