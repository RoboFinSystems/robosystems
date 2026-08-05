High-performance REST API for LadybugDB graph database operations. Provides multi-tenant database management with isolated instances, OpenCypher query execution, DuckDB-powered data ingestion from S3 and Parquet sources, and comprehensive backup/restore capabilities. Features tiered infrastructure with dedicated instances and subgraph support, plus shared repositories for public datasets (SEC filings). Built-in health monitoring, streaming query results, and vector search.

## Core Features

- **Multi-Tenant Architecture**: Isolated database instances per graph with API key authentication
- **Cypher Query Execution**: Run graph queries with streaming results and batching support
- **Data Ingestion**: DuckDB staging from S3 Parquet, PostgreSQL (postgres_scanner), and queries
- **Backup & Restore**: On-demand full database backups with optional encryption and compression
- **Health & Monitoring**: Real-time health checks, metrics, and task tracking
- **Vector Search**: LadybugDB-native HNSW indexes built at materialization and searched in Cypher (`QUERY_VECTOR_INDEX`)
- **Semantic Memory**: Per-graph LanceDB memory store backing the platform AI-memory surface (writer/master only)

## API Operations

### Database Management

- Create and delete graph databases
- Get database metadata (size, health status, node/relationship counts)
- List all accessible databases
- Health checks and status monitoring
- LadybugDB engine version migrations

### Query Execution

- Execute Cypher queries with full OpenCypher support
- Streaming results via NDJSON for large datasets
- Configurable batch sizes and timeouts
- Parameterized queries with admission-control backpressure

### Data Operations

- **DuckDB Staging**: Validate and transform data before graph import
- **Materialization**: Stage DuckDB tables into LadybugDB graph
- **Blue-Green Swap**: Materialize a WIP database beside the active one, then promote it atomically (one-way)
- **Batch Processing**: Chunked operations for large datasets
- **Schema Validation**: Ensure data conforms to graph schema

### Backup & Recovery

- On-demand full database backups via multipart S3 upload
- Multiple backup formats: encrypted ZIP, raw `.lbug` for the replica fleet, and zstd-compressed downloads to Cloudflare R2 for subscribers
- DuckDB staging snapshots for analytics and local development
- Restore from S3 backups with optional pre-restore safety backup

## Architecture

### Deployment Model

**Writer Nodes**: Full read/write access for user (entity) databases (port 8001)

**Shared Master**: Read/write node for shared-repository ingestion and publishing (SEC, etc.)

**Shared Replicas**: Read-only fleet that downloads published `.lbug` / `.duckdb` databases from S3 on boot

**Backend**: LadybugDB — embedded columnar graph database

### Multi-Tenancy

Each graph database is isolated with dedicated:

- Database files and storage
- Connection pools
- Query execution contexts
- Resource limits and quotas

### Shared Repositories

Shared data repositories (currently SEC filings) are available as read-only databases accessible through user subscriptions. Served by a dedicated replica fleet that downloads published databases from S3 on boot.

### Infrastructure Tiers

The API supports multiple infrastructure tiers optimized for different workload requirements:

- **ladybug-standard**: Dedicated m7g.large instances (8 GB, 3 subgraphs)
- **ladybug-large**: Dedicated r7g.large instances (16 GB, 10 subgraphs)
- **ladybug-xlarge**: Dedicated r7g.xlarge instances (32 GB, 25 subgraphs)
- **ladybug-shared**: Platform infrastructure for shared repositories

Each tier provides different performance characteristics, resource allocations, and feature sets. Configuration details are managed centrally and may vary by environment.

## Authentication

API key authentication required for all database operations (production/staging only):

```
X-Graph-API-Key: graph_api_64_character_random_string
```
