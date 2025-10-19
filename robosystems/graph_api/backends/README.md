# Graph Database Backend Abstraction Layer

This module provides a pluggable backend architecture for RoboSystems' graph database system, supporting multiple graph database technologies through a unified interface.

## Overview

The backend abstraction layer allows RoboSystems to support different graph database technologies while maintaining a consistent API and application logic. This enables:

- **Technology Flexibility**: Switch between graph databases based on requirements
- **Multi-Tier Support**: Different backends for different subscription tiers
- **Future-Proofing**: Easy integration of new graph database technologies
- **Development Simplification**: Test with lightweight backends, deploy with production-grade systems

## Supported Backends

### Kuzu (Default)

**Type**: Embedded graph database based on columnar storage
**Best For**: Multi-tenant and dedicated configurations, high-performance deployments
**Status**: Production-ready

**Key Features:**
- High-performance embedded database
- Low latency for local access
- Columnar storage for analytics
- COPY operations for bulk data loading
- Direct file system access

**Infrastructure:**
- EC2-based writer instances
- DynamoDB registry for allocation
- EBS volumes for persistence
- Auto-scaling groups by configuration type

### Neo4j Community

**Type**: Client-server graph database
**Best For**: Dedicated configurations requiring advanced graph features
**Status**: Development/Testing

**Key Features:**
- Battle-tested graph database
- Bolt protocol for client-server communication
- APOC procedures for extended functionality
- Cypher query language
- Built-in visualization tools

**Limitations:**
- Single database per instance
- No multi-database support
- Community license restrictions

### Neo4j Enterprise

**Type**: Client-server graph database with enterprise features
**Best For**: High-performance configurations requiring clustering and advanced security
**Status**: Future implementation

**Key Features:**
- Multi-database support
- Advanced security features
- Clustering and replication
- Enterprise support
- Role-based access control

**Requirements:**
- Neo4j Enterprise license
- Additional infrastructure costs

## Architecture

### Backend Interface

All backends implement the abstract `GraphBackend` interface defined in `base.py`:

```python
class GraphBackend(ABC):
    """Abstract base class for graph database backends."""

    @abstractmethod
    async def execute_query(
        self,
        graph_id: str,
        cypher: str,
        parameters: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Execute a read query."""
        pass

    @abstractmethod
    async def execute_write(
        self,
        graph_id: str,
        cypher: str,
        parameters: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Execute a write query."""
        pass

    @abstractmethod
    async def create_database(self, database_name: str) -> bool:
        """Create a new database."""
        pass

    @abstractmethod
    async def delete_database(self, database_name: str) -> bool:
        """Delete a database."""
        pass

    @abstractmethod
    async def list_databases(self) -> List[str]:
        """List all databases."""
        pass

    @abstractmethod
    async def get_database_info(self, database_name: str) -> DatabaseInfo:
        """Get database metadata."""
        pass

    @abstractmethod
    async def get_cluster_topology(self) -> ClusterTopology:
        """Get cluster topology information."""
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """Check backend health."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close connections and cleanup."""
        pass
```

### Backend Factory

The backend factory in `__init__.py` provides singleton access to the configured backend:

```python
from robosystems.graph_api.backends import get_backend

# Get the configured backend (singleton, automatically selects Kuzu or Neo4j)
backend = get_backend()

# Use the backend (same interface regardless of backend type)
results = await backend.execute_query(
    graph_id="kg1a2b3c4d5",
    cypher="MATCH (n:Entity) RETURN n LIMIT 10"
)
```

Backend selection is controlled by the `GRAPH_BACKEND_TYPE` environment variable:
- `kuzu` (default)
- `neo4j_community`
- `neo4j_enterprise`

## Implementation Details

### Kuzu Backend (`kuzu.py`)

**Connection Management:**
- Direct file system access to database directories
- Connection pool per database (max 3 connections)
- Memory-mapped file access for performance

**Database Operations:**
- Each database stored in separate directory
- COPY operations for bulk loading from S3
- Streaming support via NDJSON

**Limitations:**
- Single writer per database
- Sequential file processing
- No network-based queries

### Neo4j Backend (`neo4j.py`)

**Connection Management:**
- Async Neo4j Python driver
- Connection pooling via driver configuration
- Bolt protocol for all communication

**Database Operations:**
- Community: Single `neo4j` database
- Enterprise: Multi-database with `kg_{graph_id}_main` naming
- Transaction support via driver

**Query Routing:**
- Automatic routing by Neo4j driver (cluster mode)
- Session-based query execution
- No manual read/write detection needed

## Configuration

### Environment Variables

```bash
# Backend Selection
GRAPH_BACKEND_TYPE=kuzu                  # kuzu|neo4j_community|neo4j_enterprise

# Kuzu Configuration
KUZU_DATABASE_PATH=/data/kuzu-dbs       # Database storage location
KUZU_CONNECTION_POOL_SIZE=3              # Connections per database
KUZU_QUERY_TIMEOUT=30                    # Query timeout (seconds)

# Neo4j Configuration
NEO4J_URI=bolt://neo4j-db:7687          # Bolt connection URI
NEO4J_USERNAME=neo4j                     # Neo4j username
NEO4J_PASSWORD=                          # Retrieved from Secrets Manager
NEO4J_ENTERPRISE=false                   # Enable multi-database support
NEO4J_MAX_CONNECTION_POOL_SIZE=50       # Connection pool size
NEO4J_CONNECTION_ACQUISITION_TIMEOUT=60 # Timeout for acquiring connection
NEO4J_MAX_CONNECTION_LIFETIME=3600      # Max connection lifetime
```

### Docker Configuration

Development environment supports both backends via profiles:

```bash
# Start with Kuzu (default)
docker-compose --profile kuzu up

# Start with Neo4j
docker-compose --profile neo4j up

# Both
docker-compose --profile kuzu --profile neo4j up
```

## Usage Patterns

### Basic Query Execution

```python
from robosystems.graph_api.backends import get_backend

# Get backend instance
backend = get_backend()

# Execute read query
results = await backend.execute_query(
    graph_id="kg1a2b3c4d5",
    cypher="MATCH (e:Entity) RETURN e.name as name",
    parameters={}
)

# Execute write query
await backend.execute_write(
    graph_id="kg1a2b3c4d5",
    cypher="CREATE (e:Entity {identifier: $id, name: $name})",
    parameters={"id": "entity-123", "name": "New Corp"}
)
```

### Database Management

```python
# Create database
await backend.create_database("kg1a2b3c4d5")

# List databases
databases = await backend.list_databases()

# Get database info
info = await backend.get_database_info("kg1a2b3c4d5")
print(f"Nodes: {info.node_count}, Relationships: {info.relationship_count}")

# Delete database
await backend.delete_database("kg1a2b3c4d5")
```

### Health Checks

```python
# Check backend health
is_healthy = await backend.health_check()

if not is_healthy:
    logger.error("Backend unhealthy!")

# Get cluster topology
topology = await backend.get_cluster_topology()
print(f"Mode: {topology.mode}")
if topology.mode == "cluster":
    print(f"Leader: {topology.leader}")
    print(f"Followers: {len(topology.followers)}")
```

## Testing

### Unit Tests

```bash
# Test backend factory
pytest tests/unit/graph_api/backends/test_backend_factory.py

# Test Kuzu backend
pytest tests/unit/graph_api/backends/test_kuzu_backend.py

# Test Neo4j backend
pytest tests/unit/graph_api/backends/test_neo4j_backend.py
```

### Integration Tests

```bash
# Requires running backend instances
pytest tests/integration/graph_api/ -m backend_integration

# Test specific backend
GRAPH_BACKEND_TYPE=kuzu pytest tests/integration/graph_api/
GRAPH_BACKEND_TYPE=neo4j_community pytest tests/integration/graph_api/
```

## Performance Characteristics

### Kuzu Backend

**Strengths:**
- Extremely fast local queries (microsecond latency)
- Efficient bulk loading via COPY
- Low memory footprint for small databases
- Columnar storage for analytics

**Limitations:**
- Single writer constraint
- File system I/O bound for large graphs
- No distributed queries
- Limited concurrent connections (max 3)

### Neo4j Backend

**Strengths:**
- Proven enterprise scalability
- Rich ecosystem (APOC, GDS, etc.)
- Advanced query optimization
- Clustering support (Enterprise)

**Limitations:**
- Network latency for all operations
- Higher memory requirements
- Licensing costs (Enterprise)
- Connection pool management complexity

## Migration Guide

### Kuzu to Neo4j

To migrate a graph from Kuzu to Neo4j:

1. Export data from Kuzu:
```python
# Export to CSV/Parquet
await kuzu_backend.export_database(
    database_name="kg1a2b3c4d5",
    output_path="/tmp/export"
)
```

2. Load into Neo4j:
```cypher
// Use LOAD CSV or neo4j-admin import
LOAD CSV WITH HEADERS FROM 'file:///entities.csv' AS row
CREATE (e:Entity {
    identifier: row.identifier,
    name: row.name
})
```

3. Update configuration:
```bash
GRAPH_BACKEND_TYPE=neo4j_community
```

### Neo4j to Kuzu

Reverse process using Neo4j export and Kuzu COPY operations.

## Troubleshooting

### Common Issues

#### Backend Factory Returns Wrong Backend

**Symptom**: Getting KuzuBackend when expecting Neo4jBackend

**Solution**:
- Verify `GRAPH_BACKEND_TYPE` environment variable
- Clear backend singleton: `_backend_instance = None`
- Restart application

#### Connection Failures

**Kuzu**:
- Check database path exists and is writable
- Verify no file system corruption
- Check available disk space

**Neo4j**:
- Verify Neo4j service is running
- Check Bolt port (7687) is accessible
- Validate credentials in Secrets Manager

#### Performance Issues

**Kuzu**:
- Monitor I/O wait times
- Check EBS volume performance
- Consider SSD-backed storage

**Neo4j**:
- Review connection pool settings
- Monitor network latency
- Check Neo4j heap memory allocation

## Future Enhancements

1. **Additional Backends**:
   - Amazon Neptune
   - ArangoDB
   - TigerGraph
   - Memgraph

2. **Features**:
   - Backend-specific query optimization
   - Automatic backend selection based on workload
   - Cross-backend data synchronization
   - Performance benchmarking framework

3. **Infrastructure**:
   - Multi-region backend support
   - Automatic failover between backends
   - Backend-specific monitoring dashboards

## Contributing

When adding a new backend:

1. Implement the `GraphBackend` interface
2. Add configuration in `env.py`
3. Update backend factory in `__init__.py`
4. Add comprehensive tests
5. Update documentation
6. Add Docker Compose profile

## Support

For backend-specific issues:
- **Kuzu**: Check `/robosystems/graph_api/README.md`
- **Neo4j**: Consult Neo4j documentation at https://neo4j.com/docs/
- **General**: Review this README and abstract interface
