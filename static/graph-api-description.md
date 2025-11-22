High-performance REST API for graph database operations with pluggable backend support. Provides multi-tenant database management with isolated instances, OpenCypher query execution, DuckDB-powered data ingestion from S3 and Parquet sources, and comprehensive backup/restore capabilities. Features tiered infrastructure (shared, dedicated, and enterprise instances), optional subgraph support for data partitioning, and shared repositories for public datasets (SEC filings, industry, economic data). Built-in health monitoring, streaming query results, and flexible deployment with LadybugDB (primary) or optional Neo4j backends.

## Core Features

- **Multi-Tenant Architecture**: Isolated database instances per graph with API key authentication
- **Cypher Query Execution**: Run graph queries with streaming results and batching support
- **Data Ingestion**: Direct Parquet imports from S3, DuckDB ingestion from queries and tables
- **Backup & Restore**: Full database backups with encryption and point-in-time recovery
- **Health & Monitoring**: Real-time health checks, metrics, and task tracking
- **Pluggable Backends**: LadybugDB (primary) with optional Neo4j support

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

- **COPY FROM**: Direct Parquet ingestion from S3
- **DuckDB Staging**: Validate and transform data before graph import
- **DuckDB Ingestion**: Direct ingestion from DuckDB queries and tables
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

- **LadybugDB** (default): Embedded graph database, optimal for most workloads
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

**LadybugDB Tiers** (default, always available):

- **ladybug-standard**: Multi-tenant shared instances (r7g.large, 10 databases per instance)
- **ladybug-large**: Dedicated instances with subgraph support (r7g.large, 10 subgraphs)
- **ladybug-xlarge**: High-performance dedicated instances (r7g.xlarge, 25 subgraphs)
- **ladybug-shared**: Dedicated infrastructure for shared repositories (SEC, industry, economic)

**Neo4j Tiers** (optional, disabled by default):

- **neo4j-community-large**: Neo4j Community Edition on dedicated r7g.large (single database only)
- **neo4j-enterprise-xlarge**: Neo4j Enterprise Edition on dedicated r7g.xlarge ((25 databases))

Each tier provides different performance characteristics, resource allocations, and feature sets. Configuration details are managed centrally and may vary by environment.

## Authentication

API key authentication required for all database operations (production/staging only):

```
X-Graph-API-Key: graph_api_64_character_random_string
```
