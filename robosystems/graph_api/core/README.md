# Graph API Core Services

Core services layer for Graph API operations, providing cluster management, database lifecycle, connection pooling, data staging, admission control, and observability.

## Overview

The `core/` directory contains the foundational services that power the Graph API:

- **Cluster Management** - Graph database cluster orchestration and routing
- **Database Management** - Database lifecycle operations and schema management
- **Connection Pooling** - Efficient resource management for graph and DuckDB connections
- **DuckDB Staging** - High-performance data ingestion via intermediate staging
- **Admission Control** - Backpressure management to prevent overload
- **Task Management** - Async operations with Server-Sent Events
- **Metrics Collection** - Performance monitoring and observability

## Directory Structure

```
core/
├── README.md                       # This file
├── __init__.py                    # Core service exports
├── ladybug_service.py             # LadybugDB service orchestration (35KB)
├── database_manager.py            # Database lifecycle and schema management (35KB)
├── connection_pool.py             # Graph database connection pooling (26KB)
├── duckdb_manager.py              # DuckDB staging table management (20KB)
├── duckdb_pool.py                 # DuckDB connection pooling (21KB)
├── admission_control.py           # CPU/memory-based backpressure (6KB)
├── metrics_collector.py           # Performance metrics and monitoring (13KB)
├── task_manager.py                # Async task coordination (5KB)
├── task_sse.py                    # Server-Sent Events for tasks (7KB)
├── backend_cluster_manager.py     # Backend abstraction layer (4KB)
└── utils.py                       # Shared utilities (4KB)
```

## Core Services

### 1. LadybugDB Service

**Files**: `ladybug_service.py`, `backend_cluster_manager.py`

Orchestrates LadybugDB database services across multiple instances with intelligent routing:

**Key Features**:
- **Multi-Instance Management** - Tracks and routes to multiple graph database instances
- **DynamoDB Registry** - Instance discovery and registration
- **Health Monitoring** - Continuous health checks and failover
- **Intelligent Routing** - Route queries to appropriate instances based on database location
- **Auto-Scaling Support** - Integrates with EC2 auto-scaling groups

**Primary Classes**:
- `LadybugService` - Main LadybugDB service orchestration
- `BackendClusterManager` - Backend-agnostic cluster management

**Usage**:
```python
from robosystems.graph_api.core import get_ladybug_service

ladybug_service = get_ladybug_service()
instance_url = ladybug_service.get_database_instance(graph_id)
```

### 2. Database Management

**File**: `database_manager.py`

Manages database lifecycle operations including creation, deletion, schema management, and querying:

**Key Features**:
- **Database Lifecycle** - Create, delete, and manage graph databases
- **Schema Management** - DDL execution and schema validation
- **Query Execution** - Cypher query execution with streaming support
- **Backup/Restore** - Database snapshot and recovery operations
- **Multi-Database Support** - Handle multiple databases per instance

**Primary Class**: `LadybugDBDatabaseManager`

**Usage**:
```python
from robosystems.graph_api.core import LadybugDBDatabaseManager

db_manager = LadybugDBDatabaseManager(database_dir="/data/lbug-dbs")
db_manager.create_database(graph_id, schema_ddl)
result = db_manager.execute_query(graph_id, "MATCH (n) RETURN n LIMIT 10")
```

### 3. Connection Pooling

**File**: `connection_pool.py`

Efficient connection pooling for graph databases with automatic lifecycle management:

**Key Features**:
- **Resource Efficiency** - Reuse connections across requests
- **Automatic Cleanup** - Idle connection cleanup and lifecycle management
- **Thread Safety** - Safe concurrent access with proper locking
- **Configurable Limits** - Max connections, idle timeout, TTL
- **Health Checks** - Connection validation before use

**Primary Class**: `LadybugDBConnectionPool`

**Usage**:
```python
from robosystems.graph_api.core import LadybugDBConnectionPool

pool = LadybugDBConnectionPool(database_dir="/data/lbug-dbs", max_connections=10)
with pool.get_connection(graph_id) as conn:
    result = conn.execute("MATCH (n) RETURN n LIMIT 10")
```

### 4. DuckDB Staging System

**Files**: `duckdb_manager.py`, `duckdb_pool.py`

High-performance data ingestion via intermediate DuckDB staging tables:

**Key Features**:
- **Staging Tables** - Materialized tables from S3 Parquet files
- **SQL Validation** - Query and validate data before graph ingestion
- **S3 Integration** - Direct S3 file access via httpfs extension
- **Automatic Deduplication** - Node and relationship table deduplication
- **Connection Pooling** - Efficient DuckDB connection management

**Primary Classes**:
- `DuckDBTableManager` - Staging table creation and query execution
- `DuckDBConnectionPool` - DuckDB connection pooling

**Workflow**:
```
User Uploads → S3 Storage → DuckDB Staging → Validation → Graph Database
```

**Usage**:
```python
from robosystems.graph_api.core.duckdb_manager import DuckDBTableManager

manager = DuckDBTableManager(staging_path="./data/staging")
manager.create_table(graph_id, table_name, s3_files)
result = manager.query(graph_id, "SELECT COUNT(*) FROM Entity")
```

### 5. Admission Control

**File**: `admission_control.py`

CPU and memory-based backpressure management to prevent system overload:

**Key Features**:
- **Resource Monitoring** - Track CPU and memory utilization
- **Adaptive Throttling** - Reject requests when resources are constrained
- **Configurable Thresholds** - CPU and memory warning/critical levels
- **Graceful Degradation** - Return 503 Service Unavailable during overload

**Primary Class**: `AdmissionController`

**Configuration**:
```python
admission = AdmissionController(
    cpu_warning=70.0,
    cpu_critical=85.0,
    memory_warning=75.0,
    memory_critical=90.0
)
```

### 6. Task Management

**Files**: `task_manager.py`, `task_sse.py`

Async operation coordination with Server-Sent Events for real-time progress:

**Key Features**:
- **Task Tracking** - Manage long-running operations
- **Progress Updates** - Real-time status via Server-Sent Events
- **Redis-Backed** - Distributed task state storage
- **Automatic Cleanup** - Task lifecycle management

**Primary Classes**:
- `TaskManager` - Task coordination and tracking
- `TaskSSE` - Server-Sent Events for progress streaming

**Usage**:
```python
from robosystems.graph_api.core import TaskManager

task_manager = TaskManager(redis_url="redis://localhost:6379/3")
task_id = task_manager.create_task("ingest", {"graph_id": "kg123"})
task_manager.update_progress(task_id, 50, "Processing records...")
```

### 7. Metrics Collection

**File**: `metrics_collector.py`

Performance monitoring and observability for graph operations:

**Key Features**:
- **Query Metrics** - Execution time, row counts, error rates
- **Database Metrics** - Size, table counts, connection stats
- **System Metrics** - CPU, memory, disk usage
- **Time-Series Data** - Historical performance tracking

**Primary Class**: `LadybugDBMetricsCollector`

**Usage**:
```python
from robosystems.graph_api.core import LadybugDBMetricsCollector

metrics = LadybugDBMetricsCollector()
metrics.record_query(graph_id, query, execution_time, row_count)
stats = metrics.get_database_stats(graph_id)
```

## Architecture Patterns

### Service Initialization

Core services are initialized at application startup:

```python
from robosystems.graph_api.core import (
    init_ladybug_service,
    initialize_connection_pool
)

# Initialize LadybugDB service with DynamoDB registry
ladybug_service = init_ladybug_service(
    registry_table="graph-instances",
    health_check_interval=30
)

# Initialize connection pool
connection_pool = initialize_connection_pool(
    database_dir="/data/lbug-dbs",
    max_connections=10
)
```

### Request Flow

1. **API Request** → Routers
2. **Service Routing** → `LadybugService` finds appropriate instance
3. **Connection Acquisition** → `ConnectionPool` provides connection
4. **Admission Check** → `AdmissionController` verifies resources
5. **Query Execution** → `DatabaseManager` executes query
6. **Metrics Collection** → `MetricsCollector` records performance
7. **Response** → Results returned to client

### Data Ingestion Flow

1. **Upload** → Parquet files to S3 (via API)
2. **Staging** → `DuckDBTableManager` creates materialized table
3. **Validation** → User queries staging table with SQL
4. **Ingestion** → `DatabaseManager` copies from DuckDB to graph
5. **Cleanup** → Staging table remains for incremental updates

## Configuration

Core services are configured via environment variables:

### Cluster Management
```bash
DYNAMODB_REGISTRY_TABLE=graph-instances
HEALTH_CHECK_INTERVAL=30
```

### Database Management
```bash
LBUG_DATABASE_DIR=/data/lbug-dbs
MAX_DATABASES_PER_INSTANCE=10
```

### Connection Pooling
```bash
MAX_CONNECTIONS=10
IDLE_TIMEOUT=300
CONNECTION_TTL=3600
```

### DuckDB Staging
```bash
DUCKDB_STAGING_PATH=./data/staging
DUCKDB_MAX_MEMORY=4GB
```

### Admission Control
```bash
CPU_WARNING_THRESHOLD=70
CPU_CRITICAL_THRESHOLD=85
MEMORY_WARNING_THRESHOLD=75
MEMORY_CRITICAL_THRESHOLD=90
```

## Best Practices

### LadybugDB Service
- Monitor instance health continuously
- Use DynamoDB registry for service discovery
- Implement graceful failover for instance failures

### Database Lifecycle
- Always validate schema DDL before creation
- Use connection pooling for all database operations
- Implement proper cleanup on database deletion

### Connection Pooling
- Set appropriate max connections based on workload
- Configure idle timeout to prevent resource leaks
- Monitor connection pool metrics

### DuckDB Staging
- Validate data with SQL queries before ingestion
- Use materialized tables (not views) for ingestion
- Keep staging tables for incremental loading

### Admission Control
- Set thresholds based on instance capacity
- Monitor resource utilization trends
- Implement graceful degradation strategies

## Related Documentation

- **[Graph API README](/robosystems/graph_api/README.md)** - Complete Graph API overview
- **[Backends](/robosystems/graph_api/backends/README.md)** - Backend abstraction layer
- **[Client Factory](/robosystems/graph_api/client/README.md)** - Client routing system
- **[Architecture Overview](https://github.com/RoboFinSystems/robosystems/wiki/Architecture-Overview)** - System architecture

## Support

- **Issues**: [robosystems/issues](https://github.com/RoboFinSystems/robosystems/issues)
- **Source Code**: `/robosystems/graph_api/core/`
- **API Docs**: http://localhost:8001/docs
