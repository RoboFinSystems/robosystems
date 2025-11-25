# Graph Middleware

This middleware layer provides the core graph database abstraction and routing logic for the RoboSystems platform with support for multiple backend types.

## Overview

The graph middleware:

- Routes graph operations to appropriate clusters (LadybugDB or Neo4j)
- Provides backend-agnostic database abstraction
- Manages database connections and pooling
- Handles query execution with caching and queuing
- Provides admission control and backpressure management
- Integrates with the credit system for usage tracking

**Supported Backends:**

- **LadybugDB**: Embedded graph database (primary backend for all main subscription tiers)
  - Multi-tenant shared instances (ladybug-standard)
  - Dedicated instances (ladybug-large, ladybug-xlarge)
  - Subgraph support on dedicated tiers
  - Core services: `/robosystems/graph_api/core/ladybug/`

- **Neo4j** (optional, available on request):
  - External graph database for enterprise requirements
  - Core services: `/robosystems/graph_api/core/neo4j/`

- **DuckDB Staging**: Data transformation layer for all backends
  - Parquet file reading from S3
  - Data validation and transformation
  - Core services: `/robosystems/graph_api/core/duckdb/`

## Architecture

The middleware layer sits above the core services, providing routing, orchestration, and multi-tenant management:

```
middleware/graph/                         # Middleware layer (this module)
├── __init__.py                          # Module exports
├── router.py                            # Main routing logic
├── clusters.py                          # Cluster configuration and management
├── repository.py                        # Repository pattern implementation
├── dependencies.py                      # FastAPI dependency injection
├── types.py                             # Type definitions and enums
├── base.py                              # Base classes and interfaces
├── query_queue.py                       # Query queue with admission control
├── admission_control.py                 # System resource monitoring
├── schema_installer.py                  # Schema installation utilities
├── allocation_manager.py                # DynamoDB-based database allocation
├── multitenant_utils.py                 # Multi-tenant utilities and validation
└── utils/                               # Utility modules
    ├── validation.py                    # Input validation
    ├── database.py                      # Database resolution
    ├── identity.py                      # Graph identity management
    └── subgraph.py                      # Subgraph utilities

graph_api/core/                          # Core services layer (database access)
├── ladybug/                             # LadybugDB embedded database
│   ├── engine.py                       # Low-level driver
│   ├── pool.py                         # Connection pooling
│   ├── manager.py                      # Database lifecycle
│   └── service.py                      # Service orchestration
├── neo4j/                               # Neo4j backend (optional)
│   └── service.py                      # Neo4j service layer
└── duckdb/                              # DuckDB staging layer
    ├── pool.py                          # Connection pooling
    └── manager.py                       # Table management
```

**Layer Separation**:
- **Middleware** (this module): Routing, multi-tenancy, orchestration
- **Core Services**: Database access, connection management, query execution

## Key Components

### 1. Graph Router (`router.py`)

The central routing component that determines where graph operations should be executed.

**Key Features:**

- **Intelligent Routing**: Routes based on graph type, operation, and tier
- **Cluster Selection**: Chooses optimal cluster for each operation
- **API Endpoint Resolution**: Determines correct endpoints
- **Fallback Handling**: Graceful degradation when clusters unavailable

**Routing Logic:**

```python
# Shared repositories (SEC, etc.) → Shared master/replica clusters
# Entity graphs → Entity writer clusters based on tier
# Read operations → Can use replica endpoints if available
# Write operations → Always use master/writer endpoints
```

**Usage:**

```python
router = GraphRouter()
repo = router.get_repository(
    graph_id="kg1a2b3c",
    operation_type="write",
    tier=GraphTier.LBUG_STANDARD
)
result = await repo.execute_query("MATCH (n) RETURN n LIMIT 10")
```

### 2. Cluster Management (`clusters.py`)

Defines cluster configurations and node types.

**Node Types:**

- `NodeType.WRITER`: Entity database writers
- `NodeType.SHARED_MASTER`: Shared repository master (e.g., SEC)
- `NodeType.SHARED_REPLICA`: Read-only replicas for shared data

**Repository Types:**

- `RepositoryType.ENTITY`: User/entity-specific graphs
- `RepositoryType.SHARED`: Shared repositories (SEC, industry, etc.)

**Cluster Configuration:**

```python
@dataclass
class ClusterConfig:
    cluster_id: str
    repository_type: RepositoryType
    writer_node: NodeConfig
    reader_nodes: List[NodeConfig]
    alb_endpoint: Optional[str]
    region: str
    tier: InstanceTier
```

### 3. Core Services Integration

The middleware integrates with the core services layer for database access.

**LadybugDB Service** (`graph_api/core/ladybug/`):
- Connection pooling via `LadybugConnectionPool`
- Database lifecycle via `LadybugDatabaseManager`
- Query execution via `LadybugService`
- Direct engine access via `Engine` (for low-level operations)

**Neo4j Service** (`graph_api/core/neo4j/`):
- Backend abstraction integration
- Query execution and health monitoring

**DuckDB Staging** (`graph_api/core/duckdb/`):
- Staging table management via `DuckDBTableManager`
- Connection pooling via `DuckDBConnectionPool`

**Usage:**

```python
# Via middleware routing (recommended)
router = GraphRouter()
repo = router.get_repository("kg1a2b3c")
result = await repo.execute_query("MATCH (c:Entity) RETURN c")

# Direct core service access (when needed)
from robosystems.graph_api.core.ladybug import get_ladybug_service

service = get_ladybug_service()
response = await service.execute_query(QueryRequest(
    database="kg1a2b3c",
    cypher="MATCH (c:Entity) RETURN c"
))
```

See the [Core Services README](/robosystems/graph_api/core/README.md) for detailed documentation.

### 4. Repository Pattern (`repository.py`)

Provides a unified interface for graph operations.

**Features:**

- **Abstraction Layer**: Hides implementation details
- **Async Support**: Full async/await support
- **Error Handling**: Consistent error handling
- **Metrics Collection**: Automatic performance tracking

**Interface:**

```python
class Repository(Protocol):
    async def execute_query(self, query: str) -> List[Dict[str, Any]]
    async def execute_transaction(self, queries: List[str]) -> bool
    async def get_schema(self) -> Dict[str, Any]
    async def health_check(self) -> Dict[str, Any]
```

### 5. Query Queue with Admission Control (`query_queue.py`)

Advanced query queue with admission control and long polling.

**Features:**

- **Admission Control**: CPU/memory-based rejection
- **Load Shedding**: Probabilistic rejection under load
- **Priority Queue**: Higher priority queries execute first
- **Long Polling**: Efficient result waiting
- **Transparent Queuing**: Executes immediately when capacity available

**Configuration:**

```python
QUERY_QUEUE_MAX_SIZE = 1000          # Max queries in queue
QUERY_QUEUE_MAX_CONCURRENT = 50      # Max simultaneous executions
ADMISSION_MEMORY_THRESHOLD = 85      # Memory usage limit (%)
ADMISSION_CPU_THRESHOLD = 90         # CPU usage limit (%)
```

**Usage:**

```python
queue = QueryQueue()
query_id = await queue.submit_query(
    graph_id="kg1a2b3c",
    query="MATCH (n) RETURN n",
    priority=5
)
result = await queue.get_result(query_id, timeout=30)
```

### 6. Admission Control (`admission_control.py`)

Monitors system resources and controls admission.

**Features:**

- **Resource Monitoring**: Tracks CPU, memory, disk usage
- **Admission Decisions**: Accepts/rejects based on thresholds
- **Load Shedding**: Probabilistic rejection under high load
- **Metrics Export**: Exports system metrics

**Decision Logic:**

1. Check if system is under memory pressure (>85%)
2. Check if system is under CPU pressure (>90%)
3. Apply probabilistic rejection based on load
4. Track rejection metrics

### 7. FastAPI Dependencies (`dependencies.py`)

Dependency injection for FastAPI routes.

**Provided Dependencies:**

- `get_graph_repository`: Returns repository for graph operations
- `get_query_queue`: Returns query queue instance
- `validate_graph_access`: Validates user access to graph
- `track_credits`: Tracks credit consumption

**Usage:**

```python
@router.post("/query")
async def execute_query(
    repo: Repository = Depends(get_graph_repository),
    credits: CreditTracker = Depends(track_credits)
):
    result = await repo.execute_query(query)
    await credits.consume("query", len(result))
    return result
```

### 8. Type Definitions (`types.py`)

Core type definitions and enums.

**Key Types:**

- `GraphType`: Enum for graph types (ENTITY, SHARED_REPOSITORY)
- `GraphTier`: Enum for graph tiers (LBUG_STANDARD, LBUG_LARGE, LBUG_XLARGE)
- `OperationType`: Enum for operations (READ, WRITE, ADMIN)
- `QueryPriority`: Priority levels (1-10)

### 9. Database Allocation (`allocation_manager.py`)

DynamoDB-based allocation manager for graph databases across instances (LadybugDB-specific).

**Features:**

- **DynamoDB Registry**: Persistent state storage for allocations
- **Instance Management**: Tracks capacity and health of LadybugDB instances
- **Atomic Allocation**: Race-condition-free database assignment
- **Auto-scaling Integration**: Triggers capacity increases when needed
- **Multi-tier Support**: ladybug-standard, ladybug-large, ladybug-xlarge instance tiers
- **Instance Protection**: Automatically enables scale-in protection for instances with allocated databases

**Usage:**

```python
from robosystems.middleware.graph.allocation_manager import LadybugDBAllocationManager
from robosystems.config.graph_tier import GraphTier

manager = LadybugDBAllocationManager(environment="prod")
location = await manager.allocate_database(
    entity_id="kg1a2b3c",
    instance_tier=GraphTier.LBUG_STANDARD
)
print(f"Database allocated to {location.instance_id}")
```

### 10. Multi-tenant Utilities (`multitenant_utils.py`)

Core utilities for multi-tenant database operations and validation.

**Features:**

- **Database Name Resolution**: Maps graph IDs to database names
- **Access Pattern Management**: Determines routing strategies
- **Shared Repository Support**: Handles SEC, industry, economic data
- **Validation**: Input validation and security checks
- **Graph Type Detection**: Identifies user vs shared vs system graphs

**Usage:**

```python
from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils

# Validate and get database name
graph_id = MultiTenantUtils.validate_graph_id("kg1a2b3c")
db_name = MultiTenantUtils.get_database_name(graph_id)

# Check if shared repository
is_shared = MultiTenantUtils.is_shared_repository("sec")

# Get routing information
routing = MultiTenantUtils.get_graph_routing("kg1a2b3c")
```

### 11. Subgraph Support (`types.py`, `allocation_manager.py`)

Subgraph functionality allows users on dedicated tiers to create isolated databases on their parent instance.

**Key Functions:**

```python
from robosystems.middleware.graph.types import (
    is_subgraph_id,
    parse_graph_id,
    construct_subgraph_id,
)

# Check if ID is a subgraph
if is_subgraph_id("kg1234567890abcdef_dev"):
    print("This is a subgraph")

# Parse subgraph ID to get parent
parent_id, subgraph_name = parse_graph_id("kg1234567890abcdef_dev")
# Returns: ("kg1234567890abcdef", "dev")

# Construct subgraph ID
subgraph_id = construct_subgraph_id("kg1234567890abcdef", "staging")
# Returns: "kg1234567890abcdef_staging"
```

**Allocation Manager Integration:**

The `LadybugDBAllocationManager.find_database_location()` automatically resolves subgraphs to their parent's location:

```python
manager = LadybugDBAllocationManager(environment="prod")

# Requesting location for subgraph returns parent's instance location
location = await manager.find_database_location("kg1234567890abcdef_dev")
# Returns location with subgraph_id but parent's instance details
```

**Validation:**

- Parent graph ID: Must match `kg[a-f0-9]{16,}` (16+ hex chars)
- Subgraph name: Must match `[a-zA-Z0-9]{1,20}` (alphanumeric only)
- Format: `{parent_id}_{subgraph_name}`

**Limitations:**

- Subgraphs inherit parent's tier and instance
- No DynamoDB registry entries for subgraphs (resolved via parent)
- Cannot create subgraphs of subgraphs (single-level only)
- Shared repositories cannot have subgraphs

## Configuration

Key environment variables:

```bash
# Backend Configuration
GRAPH_BACKEND_TYPE=ladybug             # ladybug (primary) | neo4j (optional)

# LadybugDB Configuration (core/ladybug/)
LBUG_DATABASE_DIR=/data/lbug-dbs       # Database directory
LBUG_MAX_DATABASES_PER_NODE=100        # Instance capacity
LBUG_MAX_CONNECTIONS_PER_DB=10         # Connection pool size
LBUG_ACCESS_PATTERN=api_writer         # Access pattern for routing
GRAPH_API_URL=                         # Graph API endpoint (dynamic in prod)

# Neo4j Configuration (core/neo4j/ - optional)
NEO4J_URI=bolt://neo4j-db:7687         # Neo4j Bolt connection
NEO4J_USER=neo4j                       # Neo4j username
NEO4J_PASSWORD=password                # Neo4j password

# DuckDB Staging Configuration (core/duckdb/)
DUCKDB_STAGING_DIR=/data/duckdb-staging  # Staging database directory
DUCKDB_MAX_CONNECTIONS_PER_DB=3          # DuckDB connection pool size
DUCKDB_MAX_THREADS=4                     # DuckDB processing threads
DUCKDB_MEMORY_LIMIT=2GB                  # DuckDB memory limit

# Queue Configuration
QUERY_QUEUE_MAX_SIZE=1000           # Maximum queries in queue
QUERY_QUEUE_MAX_CONCURRENT=50       # Max concurrent executions
LONG_POLL_TIMEOUT=30                # Long polling timeout (seconds)

# Admission Control
ADMISSION_MEMORY_THRESHOLD=85       # Memory threshold (%)
ADMISSION_CPU_THRESHOLD=90          # CPU threshold (%)
LOAD_SHEDDING_ENABLED=true         # Enable load shedding

# Performance
QUERY_TIMEOUT=300                   # Query timeout (seconds)

# Multi-tenant Configuration
MULTITENANT_MODE=true              # Enable multi-tenant database support
```

## Usage Patterns

### Basic Query Execution

```python
# Via middleware router (recommended for multi-tenant routing)
from robosystems.middleware.graph import GraphRouter

router = GraphRouter()
repo = router.get_repository("kg1a2b3c")
result = await repo.execute_query("MATCH (c:Entity) RETURN c")

# Via core service (direct access when routing not needed)
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.models.database import QueryRequest

service = get_ladybug_service()
response = await service.execute_query(QueryRequest(
    database="kg1a2b3c",
    cypher="MATCH (c:Entity) RETURN c"
))
```

### With Query Queue

```python
from robosystems.middleware.graph import QueryQueue

queue = QueryQueue()
query_id = await queue.submit_query(
    graph_id="kg1a2b3c",
    query="MATCH (n) RETURN count(n)",
    priority=8
)
result = await queue.wait_for_result(query_id)
```

### Transaction Execution

```python
repo = router.get_repository("kg1a2b3c", operation_type="write")
success = await repo.execute_transaction([
    "CREATE (e:Entity {identifier: 'entity-123', name: 'New Corp'})",
    "CREATE (el:Element {uri: 'http://example.com/element/Cash', qname: 'Cash'})",
    "CREATE (e)-[:ENTITY_HAS_ELEMENT]->(el)"
])
```

## Integration Points

### 1. Credit System

The middleware integrates with the credit system to track usage:

- **AI Operations**: Anthropic/OpenAI API calls consume credits (token-based billing)
- **Database Operations**: All graph queries, imports, backups are FREE (included in subscription)
- **Storage**: Optional billing mechanism (10 credits/GB/day)
- Credit tracking happens at the middleware layer before queries reach core services

### 2. Authentication

All operations require authentication:

- API key validation
- User context injection
- Graph access validation

### 3. Monitoring

Comprehensive monitoring integration:

- Query performance metrics
- Queue depth and wait times
- System resource utilization
- Error rates and types

## Best Practices

1. **Use the Router**: Always use GraphRouter for database access
2. **Handle Errors**: Implement proper error handling for queries
3. **Set Priorities**: Use appropriate priorities for queries
4. **Monitor Queues**: Watch queue depth and adjust capacity
5. **Close Connections**: Always close repository connections

## Performance Considerations

1. **Connection Pooling**: Reuse connections via the pool
2. **Query Optimization**: Use indexes and limit result sets
3. **Batch Operations**: Batch multiple operations when possible
4. **Caching**: Leverage result caching for read-heavy workloads
5. **Load Distribution**: Use read replicas for read operations

## Troubleshooting

Common issues and solutions:

1. **High Queue Depth**

   - Increase concurrent execution limit
   - Add more worker instances
   - Optimize slow queries

2. **Admission Rejections**

   - Check system resources
   - Scale infrastructure
   - Implement backoff in clients

3. **Connection Errors**

   - Verify network connectivity
   - Check instance health
   - Review security groups

4. **Slow Queries**
   - Add appropriate indexes
   - Limit result sets
   - Use query profiling

## Related Documentation

### Core Services Layer

- **[Core Services Overview](/robosystems/graph_api/core/README.md)** - Complete overview of the core services architecture
- **[LadybugDB Service](/robosystems/graph_api/core/ladybug/README.md)** - Embedded database services (Engine, Pool, Manager, Service)
- **[Neo4j Service](/robosystems/graph_api/core/neo4j/README.md)** - Optional Neo4j backend integration
- **[DuckDB Staging](/robosystems/graph_api/core/duckdb/README.md)** - Data staging and transformation layer

### Middleware Components

- **[Subgraph Utilities](/robosystems/middleware/graph/utils/subgraph.py)** - Subgraph ID parsing and validation
- **[Multi-tenant Utilities](/robosystems/middleware/graph/utils/)** - Database resolution and access patterns
- **[Allocation Manager](/robosystems/middleware/graph/allocation_manager.py)** - DynamoDB-based database allocation

### Configuration

- **[Billing Plans](/robosystems/config/billing.py)** - Subscription tiers and features
- **[Rate Limiting](/robosystems/config/rate_limits.py)** - Burst-focused rate limiting
- **[Graph Tier Configuration](/.github/configs/graph.yml)** - Infrastructure tier specifications

### API Documentation

- **[Graph API README](/robosystems/graph_api/README.md)** - Complete Graph API overview
- **[API Routers](/robosystems/graph_api/routers/)** - FastAPI endpoint implementations
- **[API Models](/robosystems/graph_api/models/)** - Request/response schemas
