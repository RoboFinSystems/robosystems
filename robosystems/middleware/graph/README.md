# Graph Middleware

This middleware layer provides the core graph database abstraction and routing logic for the RoboSystems platform with support for multiple backend types.

## Overview

The graph middleware:

- Routes graph operations to appropriate clusters (Kuzu or Neo4j)
- Provides backend-agnostic database abstraction
- Manages database connections and pooling
- Handles query execution with caching and queuing
- Provides admission control and backpressure management
- Integrates with the credit system for usage tracking

**Supported Backends:**

- **Kuzu**: Embedded graph database with EC2-based clusters
- **Neo4j Community**: Client-server architecture for kuzu-large tier
- **Neo4j Enterprise**: Multi-database support for kuzu-xlarge tier

## Architecture

```
graph/
├── __init__.py              # Module exports
├── router.py                # Main routing logic
├── clusters.py              # Cluster configuration and management
├── engine.py                # Direct Kuzu database access
├── repository.py            # Repository pattern implementation
├── dependencies.py          # FastAPI dependency injection
├── types.py                 # Type definitions and enums
├── base.py                  # Base classes and interfaces
├── query_queue.py           # Query queue with admission control
├── admission_control.py     # System resource monitoring
├── schema_installer.py      # Schema installation utilities
├── allocation_manager.py    # DynamoDB-based database allocation
└── multitenant_utils.py     # Multi-tenant utilities and validation
```

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
    tier=GraphTier.KUZU_STANDARD
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

### 3. Graph Engine (`engine.py`)

Direct graph database access with connection management (Kuzu-specific).

**Features:**

- **Connection Pooling**: Reuses connections for performance
- **Query Execution**: Executes Cypher queries with proper error handling
- **Transaction Support**: Full ACID transaction support
- **Schema Management**: Creates and updates graph schemas

**Usage:**

```python
# Kuzu-specific direct access (development/legacy)
engine = GraphEngine(database_path="/data/kuzu-dbs/kg1a2b3c")
result = engine.execute_query("MATCH (c:Entity) RETURN c")
engine.close()

# Note: For production use the backend abstraction layer instead
```

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

### 5. Query Queue (`query_queue.py`)

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
- `GraphTier`: Enum for graph tiers (KUZU_STANDARD, KUZU_LARGE, KUZU_XLARGE)
- `OperationType`: Enum for operations (READ, WRITE, ADMIN)
- `QueryPriority`: Priority levels (1-10)

### 9. Database Allocation (`allocation_manager.py`)

DynamoDB-based allocation manager for graph databases across instances (Kuzu-specific).

**Features:**

- **DynamoDB Registry**: Persistent state storage for allocations
- **Instance Management**: Tracks capacity and health of Kuzu instances
- **Atomic Allocation**: Race-condition-free database assignment
- **Auto-scaling Integration**: Triggers capacity increases when needed
- **Multi-tier Support**: kuzu-standard, kuzu-large, kuzu-xlarge instance tiers
- **Instance Protection**: Automatically enables scale-in protection for instances with allocated databases

**Usage:**

```python
from robosystems.middleware.graph.allocation_manager import KuzuAllocationManager
from robosystems.config.graph_tier import GraphTier

manager = KuzuAllocationManager(environment="prod")
location = await manager.allocate_database(
    entity_id="kg1a2b3c",
    instance_tier=GraphTier.KUZU_STANDARD
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

The `KuzuAllocationManager.find_database_location()` automatically resolves subgraphs to their parent's location:

```python
manager = KuzuAllocationManager(environment="prod")

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
GRAPH_BACKEND_TYPE=kuzu             # kuzu|neo4j_community|neo4j_enterprise

# Routing Configuration (Kuzu)
KUZU_ACCESS_PATTERN=api_writer      # Access pattern (api_writer/api_reader/direct_file)
GRAPH_API_URL=                       # Localhost endpoint for routing (dynamic lookup in prod)

# Routing Configuration (Neo4j)
NEO4J_URI=bolt://neo4j-db:7687      # Neo4j Bolt connection
NEO4J_ENTERPRISE=false               # Enable multi-database support

# Queue Configuration
QUERY_QUEUE_MAX_SIZE=1000           # Maximum queries in queue
QUERY_QUEUE_MAX_CONCURRENT=50       # Max concurrent executions
LONG_POLL_TIMEOUT=30                # Long polling timeout (seconds)

# Admission Control
ADMISSION_MEMORY_THRESHOLD=85       # Memory threshold (%)
ADMISSION_CPU_THRESHOLD=90          # CPU threshold (%)
LOAD_SHEDDING_ENABLED=true         # Enable load shedding

# Performance
CONNECTION_POOL_SIZE=10             # Database connection pool size
QUERY_TIMEOUT=30                    # Query timeout (seconds)
NEO4J_MAX_CONNECTION_POOL_SIZE=50   # Neo4j connection pool size

# Database Allocation (Kuzu)
KUZU_MAX_DATABASES_PER_NODE=50     # Max databases per Kuzu instance

# Multi-tenant Configuration
KUZU_ACCESS_PATTERN=api_auto       # Access pattern for graph operations
```

## Usage Patterns

### Basic Query Execution

```python
from robosystems.middleware.graph import GraphRouter

router = GraphRouter()
repo = router.get_repository("kg1a2b3c")
result = await repo.execute_query("MATCH (c:Entity) RETURN c")
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

- Query operations consume credits based on complexity
- Storage operations consume credits based on size
- AI operations (MCP) consume higher credit amounts

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
