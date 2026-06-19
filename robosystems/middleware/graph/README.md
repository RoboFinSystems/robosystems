# Graph Middleware

This middleware layer provides the core graph database abstraction and routing logic for the RoboSystems platform.

## Overview

The graph middleware:

- Routes graph operations to appropriate LadybugDB clusters
- Manages database connections and pooling
- Handles query execution with caching and queuing
- Provides admission control and backpressure management
- Integrates with the credit system for usage tracking

**Backend:**

- **LadybugDB**: Embedded graph database (all subscription tiers)
  - Multi-tenant shared instances (ladybug-standard)
  - Dedicated instances (ladybug-large, ladybug-xlarge)
  - Subgraph support on dedicated tiers
  - Core services: `/robosystems/graph_api/core/ladybug/`

- **DuckDB Staging**: Data transformation layer
  - Parquet file reading from S3
  - Data validation and transformation
  - Core services: `/robosystems/graph_api/core/duckdb/`

## Architecture

The middleware layer sits above the core services, providing routing, orchestration, and multi-tenant management:

```
middleware/graph/                         # Middleware layer (this module)
├── __init__.py                          # Module exports
├── router.py                            # GraphRouter — main routing logic
├── repository.py                        # UniversalRepository wrapper (sync/async unification)
├── base.py                              # Base abstractions (GraphEngineInterface, GraphOperation)
├── types.py                             # Type definitions, enums, graph-id helpers
├── query_queue.py                       # QueryQueueManager — admission control + long polling
├── admission_control.py                 # System resource monitoring
├── execution_strategies.py             # Strategy selection (query vs MCP, load-aware)
├── ingestion_limits.py                 # Tier storage caps / materialization write-path limits
├── instance_busy.py                    # DynamoDB busy counter for destructive ops
├── streaming_wrapper.py                # Streaming query support for Graph API clients
├── allocation_manager.py                # DynamoDB-based database allocation
├── dependencies/                        # FastAPI dependency injection
│   ├── auth.py                         # Auth-checked repository/database dependencies
│   ├── repositories.py                 # Repository dependencies
│   └── helpers.py                      # require_entity / require_user_graph / etc.
└── utils/                               # Utility modules (MultiTenantUtils lives here)
    ├── __init__.py                     # MultiTenantUtils class
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
repo = await router.get_repository(   # get_repository is async
    graph_id="kg1a2b3c",
    operation_type="write",
    tier=GraphTier.LADYBUG_STANDARD,
)
result = await repo.execute_query("MATCH (n) RETURN n LIMIT 10")
```

`GraphRouter.get_repository()` delegates routing to
`graph_api.client.factory.GraphClientFactory` (or returns a direct-file
`Repository` when `LBUG_ACCESS_PATTERN=direct_file`).

### 2. Graph Type Definitions (`types.py`)

`types.py` is the canonical source for graph-routing enums and graph-id
helpers (there is no separate `clusters.py`).

**Enums:**

- `NodeType`: node roles (e.g. writer / shared master / shared replica)
- `RepositoryType`: entity vs shared repository
- `GraphCategory`: `USER`, `SHARED`, `SYSTEM`
- `AccessPattern`: `READ_ONLY`, `READ_WRITE`, `RESTRICTED`
- `ConnectionPattern`: connection routing strategy

Graph tiers (`LADYBUG_STANDARD`, `LADYBUG_LARGE`, `LADYBUG_XLARGE`) come
from `config/graph_tier.py:GraphTier`, re-exported by this package.

### 3. Core Services Integration

The middleware integrates with the core services layer for database access.

**LadybugDB Service** (`graph_api/core/ladybug/`):
- Connection pooling via `LadybugConnectionPool`
- Database lifecycle via `LadybugDatabaseManager`
- Query execution via `LadybugService`
- Direct engine access via `Engine` (for low-level operations)

**DuckDB Staging** (`graph_api/core/duckdb/`):
- Staging table management via `DuckDBTableManager`
- Connection pooling via `DuckDBConnectionPool`

**Usage:**

```python
# Via middleware routing (recommended)
router = GraphRouter()
repo = await router.get_repository("kg1a2b3c")
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

### 4. Repository Wrapper (`repository.py`)

`UniversalRepository` wraps either a direct `Repository` or a Graph API
`GraphClient` and exposes a unified async (and sync) interface so callers
don't branch on the underlying type.

**Features:**

- **Unification**: Same interface for sync direct repos and async API clients
- **Async + sync methods**: `execute_query` / `execute_query_sync`, etc.
- **Streaming**: `execute_query_streaming`
- **Context managers**: both `async with` and `with`

**Selected methods:**

```python
class UniversalRepository:
    async def execute_query(self, cypher, parameters=None, ...) -> Any
    async def execute_query_streaming(self, cypher, ...) -> Any
    async def execute_transaction(self, queries) -> Any
    async def get_schema(self) -> list[dict[str, Any]]
    async def health_check(self) -> dict[str, Any]
    async def close(self) -> None
```

Construct via `create_universal_repository(...)` /
`get_universal_repository(...)` rather than instantiating directly.

### 5. Query Queue with Admission Control (`query_queue.py`)

`QueryQueueManager` provides admission control and long polling.

**Features:**

- **Admission Control**: CPU/memory-based rejection
- **Load Shedding**: Probabilistic rejection under load
- **Priority Queue**: Higher priority queries execute first
- **Long Polling**: Efficient result waiting
- **Transparent Queuing**: Executes immediately when capacity available

**Usage:**

```python
from robosystems.middleware.graph.query_queue import get_query_queue

queue = get_query_queue()
query_id = await queue.submit_query(
    graph_id="kg1a2b3c",
    query="MATCH (n) RETURN n",
    priority=5,
)
# Poll status, then fetch the result:
status = await queue.get_query_status(query_id)
result = await queue.get_query_result(query_id)
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

### 7. FastAPI Dependencies (`dependencies/`)

Dependency injection for FastAPI routes lives in the `dependencies/`
package (`auth.py`, `repositories.py`, `helpers.py`), exported from
`dependencies/__init__.py`.

**Provided Dependencies (selected):**

- `get_graph_database`: Resolves + auth-checks the graph for the request
- `get_graph_repository_with_auth` / `get_universal_repository_with_auth`: Auth-checked repository
- `get_user_graph_repository` / `get_shared_repository` / `get_main_repository`: Repository by graph kind
- `get_graph_repository_dependency`: Generic repository dependency
- `require_entity` / `require_user_graph` / `require_graph_category` (+ `optional_*` variants): Path-param helpers

```python
from robosystems.middleware.graph.dependencies import get_graph_repository_with_auth

@router.post("/v1/graphs/{graph_id}/query")
async def execute_query(
    repo = Depends(get_graph_repository_with_auth),
):
    return await repo.execute_query(query)
```

### 8. Type Definitions (`types.py`)

Core type definitions and enums (see §2 for the enum list). `types.py`
also exposes graph-id helpers (`is_subgraph_id`, `parse_graph_id`,
`construct_subgraph_id`) and the `GraphIdentity` / `GraphTypeRegistry`
models. `GraphTier` is imported from `config/graph_tier.py`.

### 9. Database Allocation (`allocation_manager.py`)

`LadybugAllocationManager` — DynamoDB-based allocation of graph databases
across instances.

**Features:**

- **DynamoDB Registry**: Persistent state storage for allocations
- **Instance Management**: Tracks capacity and health of LadybugDB instances
- **Atomic Allocation**: Race-condition-free database assignment
- **Auto-scaling Integration**: Triggers capacity increases when needed
- **Multi-tier Support**: ladybug-standard, ladybug-large, ladybug-xlarge instance tiers
- **Instance Protection**: Automatically enables scale-in protection for instances with allocated databases

**Usage:**

```python
from robosystems.middleware.graph.allocation_manager import LadybugAllocationManager
from robosystems.config.graph_tier import GraphTier

manager = LadybugAllocationManager(environment="prod")
location = await manager.allocate_database(
    entity_id="kg1a2b3c",
    instance_tier=GraphTier.LADYBUG_STANDARD,
)
print(f"Database allocated to {location.instance_id}")
```

### 10. Multi-tenant Utilities (`utils/`)

`MultiTenantUtils` (in `middleware/graph/utils/__init__.py`) aggregates
static methods from the `utils/` submodules (`validation.py`,
`database.py`, `identity.py`, `subgraph.py`) for multi-tenant database
operations and validation.

**Capabilities:**

- **Database Name Resolution**: Maps graph IDs to database names
- **Access Pattern Management**: Determines routing strategies
- **Shared Repository Support**: Routes to shared repository infrastructure
- **Validation**: Input validation and security checks
- **Graph Type Detection**: Identifies user vs shared vs system graphs

**Usage:**

```python
from robosystems.middleware.graph.utils import MultiTenantUtils

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

The `LadybugAllocationManager.find_database_location()` automatically resolves subgraphs to their parent's location:

```python
manager = LadybugAllocationManager(environment="prod")

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
# LadybugDB Configuration (core/ladybug/)
LBUG_DATABASE_DIR=/data/lbug-dbs       # Database directory
LBUG_MAX_DATABASES_PER_NODE=100        # Instance capacity
LBUG_MAX_CONNECTIONS_PER_DB=10         # Connection pool size
LBUG_ACCESS_PATTERN=api_writer         # Access pattern for routing
GRAPH_API_URL=                         # Graph API endpoint (dynamic in prod)

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
repo = await router.get_repository("kg1a2b3c")
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
from robosystems.middleware.graph.query_queue import get_query_queue

queue = get_query_queue()
query_id = await queue.submit_query(
    graph_id="kg1a2b3c",
    query="MATCH (n) RETURN count(n)",
    priority=8,
)
result = await queue.get_query_result(query_id)
```

### Transaction Execution

```python
repo = await router.get_repository("kg1a2b3c", operation_type="write")
success = await repo.execute_transaction([
    "CREATE (e:Entity {identifier: 'entity-123', name: 'New Corp'})",
    "CREATE (el:Element {uri: 'http://example.com/element/Cash', qname: 'Cash'})",
    "CREATE (e)-[:ENTITY_HAS_ELEMENT]->(el)"
])
```

## Integration Points

### 1. Credit System

Credit consumption is intentionally narrow:

- **AI Operations**: Anthropic/OpenAI API calls consume credits (token-based billing) — the only credit-consuming path
- **Database Operations**: All graph queries, imports, backups are free (included in the subscription tier)
- **Storage**: Currently limit-enforced, not metered into credits (see
  `middleware/billing/README.md`); if usage-based storage billing is added it
  would be a separate credit line, independent of the AI compute path.

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
- **[DuckDB Staging](/robosystems/graph_api/core/duckdb/README.md)** - Data staging and transformation layer

### Middleware Components

- **[Subgraph Utilities](/robosystems/middleware/graph/utils/subgraph.py)** - Subgraph ID parsing and validation
- **[Multi-tenant Utilities](/robosystems/middleware/graph/utils/)** - Database resolution and access patterns
- **[Allocation Manager](/robosystems/middleware/graph/allocation_manager.py)** - DynamoDB-based database allocation

### Configuration

- **[Billing Plans](/robosystems/config/billing/core.py)** - Subscription tiers and features (the `config/billing/` package)
- **[Rate Limiting](/robosystems/config/rate_limits.py)** - Burst-focused rate limiting
- **[Graph Tier Configuration](/.github/configs/graph.yml)** - Infrastructure tier specifications

### API Documentation

- **[Graph API README](/robosystems/graph_api/README.md)** - Complete Graph API overview
- **[API Routers](/robosystems/graph_api/routers/)** - FastAPI endpoint implementations
- **[API Models](/robosystems/graph_api/models/)** - Request/response schemas
