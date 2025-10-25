High-performance REST API for graph database operations. Provides multi-tenant database management, query execution, data ingestion, and backup/restore capabilities with built-in monitoring and health checks.

## Core Features

- **Multi-Tenant Architecture**: Isolated database instances per graph with API key authentication
- **Cypher Query Execution**: Run graph queries with streaming results and batching support
- **Data Ingestion**: Direct Parquet/CSV imports with DuckDB staging for validation
- **Backup & Restore**: Full database backups with encryption and point-in-time recovery
- **Health & Monitoring**: Real-time health checks, metrics, and task tracking
- **Pluggable Backends**: Kuzu (primary) with optional Neo4j support

## API Operations

### Database Management

- Create and delete graph databases
- Get database metadata (size, health status, node/relationship counts)
- List all accessible databases
- Health checks and status monitoring

### Query Execution

- Execute Cypher queries with full OpenCypher support
- Streaming results via NDJSON for large datasets
- Configurable batch sizes and timeouts
- Query plan analysis and optimization

### Data Operations

- **COPY FROM**: Direct Parquet/CSV ingestion from S3 or local files
- **DuckDB Staging**: Validate and transform data before graph import
- **Batch Processing**: Chunked operations for large datasets
- **Schema Validation**: Ensure data conforms to graph schema

### Backup & Recovery

- On-demand full database backups
- Automated backup scheduling
- Point-in-time recovery
- Backup encryption and compression

## Architecture

### Deployment Model

**Writer Nodes**: Full read/write access for user databases and shared repositories (port 8001)

**Backend Support**:

- **Kuzu** (default): Embedded graph database, optimal for most workloads
- **Neo4j** (optional): Available on port 8002, disabled by default

### Multi-Tenancy

Each graph database is isolated with dedicated:

- Database files and storage
- Connection pools
- Query execution contexts
- Resource limits and quotas

### Shared Repositories

Shared data repositories (SEC filings, industry data) are available as read-only shared databases accessible through user subscriptions.

### Infrastructure Tiers

The API supports multiple infrastructure tiers optimized for different workload requirements:

- **kuzu-standard**: Multi-tenant shared instances (r7g.xlarge)
- **kuzu-large**: Dedicated instances with subgraph support (r7g.large)
- **kuzu-xlarge**: High-performance dedicated instances (r7g.xlarge)
- **kuzu-shared**: Dedicated infrastructure for shared repositories

Each tier provides different performance characteristics, resource allocations, and feature sets. Configuration details are managed centrally and may vary by environment.

## Authentication

API key authentication required for all database operations (production/staging only):

```
X-Graph-API-Key: graph_api_64_character_random_string
```

Development environments bypass authentication for ease of testing.

## Performance Features

- **Connection Pooling**: Reusable database connections for reduced latency
- **Query Batching**: Configurable batch sizes for optimal memory usage
- **Streaming Results**: NDJSON streaming for large result sets
- **Async Operations**: Non-blocking task processing with progress tracking
- **Result Caching**: Automatic caching of frequently accessed data

## Monitoring & Observability

- Health check endpoints for orchestration and load balancing
- Detailed error messages and stack traces (development mode)
- Task tracking for long-running operations
- Metrics export for monitoring systems
- Audit logging for security and compliance
