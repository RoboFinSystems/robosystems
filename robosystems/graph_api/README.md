# Graph API

HTTP API server for graph database cluster management. FastAPI-based microservice that provides REST endpoints for multi-tenant graph operations with connection pooling, circuit breaking, and admission control.

**Backend:**

- **LadybugDB**: Embedded graph database built on columnar storage

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Deployment Infrastructure](#deployment-infrastructure)
- [API Endpoints](#api-endpoints)
- [Client Libraries](#client-libraries)
- [Configuration](#configuration)
- [Security](#security)
- [Monitoring & Observability](#monitoring--observability)
- [Development](#development)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

## Architecture Overview

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Main Application Layer                   │
│                  (RoboSystems FastAPI App)                  │
├─────────────────────────────────────────────────────────────┤
│                     GraphRouter Layer                       │
│                 (Intelligent Routing Logic)                 │
├─────────────────────────────────────────────────────────────┤
│                   GraphClientFactory Layer                  │
│              (Circuit Breakers, Retry Logic)                │
├─────────────────────────────────────────────────────────────┤
│                      Graph API Layer                        │
│                    (FastAPI on Port 8001)                   │
├─────────────────────────────────────────────────────────────┤
│                   Graph Database Engine                     │
│                    (LadybugDB Embedded)                     │
└─────────────────────────────────────────────────────────────┘
```

### Core Components

```
graph_api/
├── app.py                      # FastAPI application factory
├── main.py                     # Server entry point
├── __main__.py                 # Module entry point
│
├── client/                     # Python clients
│   ├── base.py                # Shared client base (BaseGraphClient)
│   ├── client.py              # Async client implementation (GraphClient)
│   ├── factory.py             # Intelligent routing factory + sync helpers
│   ├── config.py              # Client configuration
│   └── exceptions.py          # Custom exceptions
│
├── core/                      # Core services (organized by database technology)
│   ├── ladybug/              # LadybugDB embedded graph database
│   │   ├── engine.py         # Low-level database driver
│   │   ├── pool.py           # Connection pooling
│   │   ├── manager.py        # Database lifecycle and schema
│   │   ├── service.py        # Service orchestration
│   │   ├── config.py         # LadybugDB configuration
│   │   └── materialization_lock.py  # Single-writer materialization lock
│   ├── duckdb/               # DuckDB data staging
│   │   ├── pool.py           # DuckDB connection pooling
│   │   └── manager.py        # Staging table management
│   ├── lance/                # LanceDB vector storage
│   │   └── manager.py        # Vector table management
│   ├── admission_control.py  # CPU/memory backpressure management
│   ├── backup_service.py     # Backup/restore service
│   ├── memory_manager.py     # Memory budget management
│   ├── migration_service.py  # Graph schema migration service
│   ├── metrics_collector.py  # Performance metrics
│   ├── task_manager.py       # Async task coordination
│   └── task_sse.py           # Server-Sent Events for task progress
│
├── routers/                   # API endpoints
│   ├── databases/
│   │   ├── management.py     # Create/delete databases
│   │   ├── query.py          # Cypher query execution
│   │   ├── tables/           # DuckDB staging table management
│   │   │   ├── management.py # Create/list staging tables
│   │   │   ├── materialize.py # DuckDB → LadybugDB materialization
│   │   │   └── query.py      # DuckDB SQL queries on tables
│   │   ├── schema.py         # Schema management
│   │   ├── metrics.py        # Database metrics
│   │   ├── backup.py         # Backup operations
│   │   ├── restore.py        # Restore operations
│   │   ├── memory.py         # Per-database memory operations
│   │   ├── swap.py           # Blue/green database swap
│   │   └── vector_search.py  # LanceDB vector search
│   ├── health.py             # Health checks
│   ├── info.py               # Node information
│   ├── metrics.py            # Node-level metrics
│   ├── migration.py          # Schema migration endpoints
│   └── tasks.py              # Background task tracking
│
├── middleware/
│   ├── auth.py               # API key authentication
│   └── request_limits.py     # Rate limiting
│
└── models/                    # Pydantic models
    ├── database.py           # Database schemas
    ├── tables.py             # Staging table requests/responses
    ├── tasks.py              # Task tracking models
    ├── migration.py          # Migration request/response models
    ├── fork.py               # Database fork/swap models
    └── cluster.py            # Cluster configuration
```

### Node Types

The system deploys different node types:

- **Writer Nodes** (`writer`): Entity database read/write operations on EC2
- **Shared Master** (`shared_master`): Shared repository ingestion and writes on EC2

## Deployment Infrastructure

### CloudFormation Stack Architecture

The system uses a multi-stack CloudFormation architecture:

```
1. Infrastructure Stack (ladybug-infra.yaml)
   ├─ DynamoDB Tables (Instance, Graph, Volume Registry)
   ├─ Secrets Manager (API Keys with rotation)
   ├─ SNS Topics (Alerts and notifications)
   └─ Lambda Functions (Instance monitoring)

2. Volume Management Stack (ladybug-volumes.yaml)
   ├─ Volume Manager Lambda (EBS lifecycle)
   ├─ Volume Monitor Lambda (Auto-expansion)
   ├─ Snapshot Management (Backup/restore)
   └─ SNS Topics (Volume alerts)

3. Writer Stacks (ladybug-writers.yaml) - Deployed in parallel
   ├─ Multi-Tenant Writers (configurable instance types and capacity)
   ├─ Dedicated Writers (single database per instance)
   ├─ High-Performance Writers (larger instances for demanding workloads)
   └─ Shared Master (shared repository infrastructure)
```

### Infrastructure Configuration

All tiers use dedicated instances (1 database per instance). Configuration is defined in [`.github/configs/graph.yml`](/.github/configs/graph.yml).

#### Production Tiers

| Tier | Instance | Memory | Subgraphs | Use Case |
| ---- | -------- | ------ | --------- | -------- |
| **ladybug-standard** | m7g.large | 8 GB | 3 | Cost-efficient entry tier |
| **ladybug-large** | r7g.large | 16 GB | 10 | Enhanced performance |
| **ladybug-xlarge** | r7g.xlarge | 32 GB | 25 | Maximum scale |
| **ladybug-shared** | r7g.2xlarge | 64 GB | — | Public repositories (SEC) |

#### Shared Replica Fleet

Read-only replicas download `.lbug` and `.duckdb` files from S3 on boot and serve queries locally. Refreshed via rolling ASG instance refresh after new databases are published.

### DynamoDB Registry Tables

#### Instance Registry

Tracks all LadybugDB instances across the infrastructure:

```python
{
    "instance_id": "i-1234567890",      # EC2 instance ID
    "cluster_tier": "ladybug-standard", # Actual tier from deployment config
    "private_ip": "10.0.1.100",
    "status": "healthy",                # initializing|healthy|unhealthy
    "database_count": 5,                # Current databases
    "max_databases": 10,                # Configuration-based limit
    "created_at": "2024-01-01T00:00:00Z"
}
```

#### Graph Registry

Maps graph databases to instances:

```python
{
    "graph_id": "kg1a2b3c4d5",          # Unique database ID
    "instance_id": "i-1234567890",
    "entity_id": "entity_123",          # Owner entity
    "repository_type": "entity",        # entity|shared
    "status": "active",
    "created_at": "2024-01-01T00:00:00Z"
}
```

#### Volume Registry

Manages EBS volume persistence:

```python
{
    "volume_id": "vol-0123456789",      # EBS volume ID
    "instance_id": "i-1234567890",
    "database_id": "kg1a2b3c4d5",
    "tier": "ladybug-standard",
    "size_gb": 100,
    "status": "attached"                # available|attached|expanding
}
```

### GitHub Actions Deployment Workflow

```yaml
deploy-ladybug.yml (Orchestrator)
├── deploy-ladybug-infra.yml
│   └── Creates DynamoDB, Secrets, SNS
├── deploy-ladybug-volumes.yml
│   └── Deploys Lambda functions for volume management
├── prepare-writer-matrix
│   └── Parses .github/configs/graph.yml for tier specs
├── deploy-ladybug-writers.yml (Matrix strategy, parallel)
│   └── Deploys each tier based on configuration
└── deploy-ladybug-shared-replicas.yml
└── Creates read replica infrastructure
```

### EC2 UserData Initialization

The Graph API starts automatically on EC2 instances via userdata script:

```bash
# 1. Register instance in DynamoDB
aws dynamodb put-item --table-name instance-registry ...

# 2. Invoke Volume Manager for EBS attachment
aws lambda invoke --function-name volume-manager ...

# 3. Pull and start Docker container
docker run -d \
  -p 8001:8001 \
  -v /data/lbug-dbs:/data/lbug-dbs \
  -e LBUG_NODE_TYPE=writer \
  -e CLUSTER_TIER=ladybug-standard \
  -e GRAPH_API_KEY=${GRAPH_API_KEY} \
  ${ECR_URI}:${ECR_IMAGE_TAG} \
  /app/bin/entrypoint.sh

# 4. Signal CloudFormation
cfn-signal --success --stack ${STACK_NAME} ...
```

## API Endpoints

### Database Operations

#### Create Database

```http
POST /databases
X-Graph-API-Key: {api_key}
Content-Type: application/json

{
  "graph_id": "kg1a2b3c4d5",
  "schema_type": "entity"  // entity|shared|custom
}
```

#### Execute Query

```http
POST /databases/{graph_id}/query
X-Graph-API-Key: {api_key}
Content-Type: application/json

{
  "cypher": "MATCH (n:Entity) RETURN n LIMIT 10",
  "parameters": {},
  "timeout": 30
}
```

#### Table Operations

**DuckDB Staging Tables** provide an intermediate staging layer for data validation and transformation before graph ingestion.

**Note:** This is the low-level Graph API (port 8001). Individual file uploads and tracking are handled by the main API layer (port 8000):
- `POST /v1/graphs/{graph_id}/tables/{table_name}/files` - Get presigned S3 upload URL
- `PATCH /v1/graphs/{graph_id}/tables/files/{file_id}` - Mark upload complete (automatically calls create table here)

**Create Table:**

```http
POST /databases/{graph_id}/tables
X-Graph-API-Key: {api_key}
Content-Type: application/json

{
  "table_name": "Entity",
  "s3_pattern": "s3://bucket/path/*.parquet"
}

Response: {
  "status": "success",
  "graph_id": "kg1a2b3c4d5",
  "table_name": "Entity",
  "execution_time_ms": 1250.5
}
```

**List Tables:**

```http
GET /databases/{graph_id}/tables
X-Graph-API-Key: {api_key}

Response: [
  {
    "graph_id": "kg1a2b3c4d5",
    "table_name": "Entity",
    "row_count": 1523,
    "size_bytes": 45678912,
    "s3_location": "s3://bucket/path/*.parquet"
  }
]
```

**Query Staging Table:**

```http
POST /databases/{graph_id}/tables/query
X-Graph-API-Key: {api_key}
Content-Type: application/json

{
  "sql": "SELECT * FROM Entity WHERE status = 'active' LIMIT 10"
}

Response: {
  "graph_id": "kg1a2b3c4d5",
  "columns": ["identifier", "name", "status"],
  "rows": [
    ["entity-1", "Company A", "active"],
    ["entity-2", "Company B", "active"]
  ],
  "row_count": 2,
  "execution_time_ms": 45.2
}

Note: Table name is specified in the SQL query, not the path.
Supports streaming via Accept: application/x-ndjson or text/event-stream headers.
```

**Materialize Table to Graph:**

```http
POST /databases/{graph_id}/tables/{table_name}/materialize
X-Graph-API-Key: {api_key}
Content-Type: application/json

{
  "ignore_errors": true,
  "rebuild": false
}

Response: {
  "status": "success",
  "graph_id": "kg1a2b3c4d5",
  "table_name": "Entity",
  "rows_ingested": 1523,
  "execution_time_ms": 2340.8
}

Note: This performs direct DuckDB → LadybugDB materialization via database extensions.
Use rebuild=true to regenerate the graph database from scratch (safe operation).
```

**Delete Table:**

```http
DELETE /databases/{graph_id}/tables/{table_name}
X-Graph-API-Key: {api_key}

Response: {
  "status": "success",
  "message": "Table deleted successfully"
}
```

### System Operations

#### Health Check

```http
GET /health
Response: {
  "status": "healthy",
  "node_type": "writer",
  "tier": "ladybug-standard",
  "databases": 5,
  "max_databases": 10,
  "memory_usage_mb": 2048,
  "uptime_seconds": 3600
}
```

#### Node Information

```http
GET /info
Response: {
  "instance_id": "i-1234567890",
  "cluster_tier": "ladybug-standard",
  "available_capacity": 5,
  "active_connections": 15,
  "queue_depth": 3
}
```

#### Task Status

```http
GET /tasks/{task_id}/status
Response: {
  "task_id": "task_abc123",
  "status": "in_progress",
  "progress": 75,
  "started_at": "2024-01-01T00:00:00Z",
  "error": null
}
```

## Client Libraries

`GraphClient` (`client.py`, a subclass of `BaseGraphClient`) is the single async
client. There is no separate sync client class — synchronous access is provided
by the `get_graph_client_sync` / `create_client_sync` helpers, which return a
`GraphClient` usable as a sync context manager.

### Async Client

```python
from robosystems.graph_api.client import GraphClient

async with GraphClient(
    base_url="http://graph-api:8001",
    api_key="graph_api_..."
) as client:
    # Create database
    await client.create_database(
        graph_id="kg1a2b3c4d5",
        schema_type="entity"
    )

    # Execute query
    results = await client.query(
        graph_id="kg1a2b3c4d5",
        cypher="MATCH (n) RETURN count(n) as count"
    )
```

### Client Factory with Intelligent Routing

```python
from robosystems.config.graph_tier import GraphTier
from robosystems.graph_api.client import get_graph_client

# Factory handles routing based on graph type and operation (async)
client = await get_graph_client(
    graph_id="sec",              # Routes to shared infrastructure
    operation_type="read",        # Could use replica
    environment="prod",
    tier=GraphTier.LADYBUG_STANDARD
)
```

### Sync Access

```python
from robosystems.graph_api.client import get_graph_client_sync

# Returns a GraphClient usable as a synchronous context manager
with get_graph_client_sync("kg1a2b3c4d5", operation_type="read") as client:
    data = client.query(
        graph_id="kg1a2b3c4d5",
        cypher="MATCH (n) RETURN n LIMIT 10"
    )
```

## Configuration

### Environment Variables

```bash
# Node Configuration
LBUG_NODE_TYPE=writer                    # writer|shared_master
CLUSTER_TIER=ladybug-standard            # ladybug-standard|ladybug-large|ladybug-xlarge|ladybug-shared
LBUG_DATABASE_PATH=/data/lbug-dbs        # Storage location

# Performance Settings
LBUG_DATABASES_PER_INSTANCE=10           # Databases per instance
LBUG_MAX_MEMORY_MB=14336                 # Total memory allocation
LBUG_MAX_MEMORY_PER_DB_MB=2048           # Per-database memory
LBUG_CHUNK_SIZE=1000                     # Streaming chunk size
GRAPH_QUERY_TIMEOUT=30                   # Query timeout seconds
LBUG_CONNECTION_POOL_SIZE=10             # Connections per database

# Authentication
GRAPH_API_KEY=                           # API key

# AWS Configuration
AWS_DEFAULT_REGION=us-east-1
DATABASE_URL=postgresql://...           # PostgreSQL for metadata
USER_DATA_BUCKET=robosystems-user-dev  # S3 for user data ingestion
SHARED_RAW_BUCKET=robosystems-shared-raw-dev  # S3 for shared raw data
SHARED_PROCESSED_BUCKET=robosystems-shared-processed-dev  # S3 for shared processed data

# Feature Flags
GRAPH_CIRCUIT_BREAKERS_ENABLED=true    # Enable circuit breakers
GRAPH_REDIS_CACHE_ENABLED=true         # Enable Valkey caching of instance locations
GRAPH_RETRY_LOGIC_ENABLED=true         # Enable automatic retries
GRAPH_HEALTH_CHECKS_ENABLED=true       # Enable health checking
```

### Schema Types

- **Entity**: Multi-tenant databases with accounting extensions
- **Shared**: Public repository databases (SEC)
- **Custom**: Custom schemas with custom DDL

## Security

### Authentication

All API requests require authentication via API key header:

```http
X-Graph-API-Key: graph_api_64_character_random_string
```

### API Key Management

- **Generation**: Cryptographically secure 64-character keys
- **Storage**: AWS Secrets Manager with encryption at rest
- **Rotation**: Automatic 90-day rotation via Lambda
- **Access**: IAM role-based retrieval

### Network Security

- **VPC Isolation**: All instances in private subnets
- **Security Groups**: Port 8001 restricted to VPC CIDR
- **No Public Access**: API only accessible within VPC
- **TLS Termination**: At ALB for replica traffic

### Database Isolation

- **File System**: Each database in separate directory
- **Memory**: Isolated memory allocation per database
- **Query Isolation**: No cross-database queries allowed
- **Path Validation**: Protection against directory traversal

## Monitoring & Observability

### CloudWatch Metrics

**Namespace**: `RoboSystemsLadybugDB/{Environment}`

**Key Metrics**:

- `DatabaseUtilizationPercent`: Database capacity usage
- `InstanceCapacityUsed`: Databases per instance
- `QueryResponseTime`: P50, P95, P99 latencies
- `IngestionQueueDepth`: Pending ingestion tasks
- `ConnectionPoolUtilization`: Active connections
- `VolumeUsagePercent`: EBS volume usage

### Health Checks

**Endpoint Monitoring**:

```bash
# System health
curl http://ladybug-writer:8001/health

# Node information
curl http://ladybug-writer:8001/info

# Detailed metrics
curl http://ladybug-writer:8001/metrics
```

### Logging

**CloudWatch Log Groups**:

- `/robosystems/{env}/ladybug-writer-standard`
- `/robosystems/{env}/ladybug-writer-large`
- `/robosystems/{env}/ladybug-writer-xlarge`
- `/robosystems/{env}/ladybug-shared-master`

**Log Format**:

```json
{
  "timestamp": "2024-01-01T00:00:00Z",
  "level": "INFO",
  "node_type": "writer",
  "tier": "ladybug-standard",
  "instance_id": "i-1234567890",
  "graph_id": "kg1a2b3c4d5",
  "operation": "query",
  "duration_ms": 45,
  "status": "success"
}
```

## Development

### Local Development

```bash
# Start full stack with Docker
just start robosystems

# Run API server locally
uv run python -m robosystems.graph_api \
  --base-path ./data/lbug-dbs \
  --node-type writer \
  --port 8001

# Use direct file access (bypass API)
export LBUG_ACCESS_PATTERN=direct_file
```

### Docker Development

```bash
docker run -d \
  -p 8001:8001 \
  -v lbug_data:/data/lbug-dbs \
  -e LBUG_NODE_TYPE=writer \
  -e CLUSTER_TIER=ladybug-standard \
  robosystems-api:latest \
  python -m robosystems.graph_api
```

### CLI Tools

```bash
# Server mode
python -m robosystems.graph_api --help

# Client CLI
python -m robosystems.graph_api cli health
python -m robosystems.graph_api cli query kg1a2b3c "MATCH (n) RETURN count(n)"
python -m robosystems.graph_api cli ingest kg1a2b3c /path/to/data.parquet
```

## Testing

### Unit Tests

```bash
# Run all tests
uv run pytest tests/graph_api/ -v

# Run specific test categories
uv run pytest tests/graph_api/test_client.py -v
uv run pytest tests/graph_api/test_ingestion.py -v
```

### Integration Tests

```bash
# Requires running LadybugDB instance
uv run pytest tests/graph_api/ -m integration

# Test with real S3
AWS_ENDPOINT_URL=http://localhost:4566 \
  uv run pytest tests/graph_api/test_s3_ingestion.py
```

### Load Testing

```bash
# Using locust for load testing
locust -f tests/graph_api/loadtest.py \
  --host http://localhost:8001 \
  --users 100 \
  --spawn-rate 10
```

### API Testing

```bash
# Create database
curl -X POST http://localhost:8001/databases \
  -H "X-Graph-API-Key: test-key" \
  -H "Content-Type: application/json" \
  -d '{"graph_id": "test_db", "schema_type": "entity"}'

# Execute query
curl -X POST http://localhost:8001/databases/test_db/query \
  -H "X-Graph-API-Key: test-key" \
  -H "Content-Type: application/json" \
  -d '{"cypher": "RETURN 1 as num"}'
```

## Troubleshooting

### Common Issues

#### 1. Connection Pool Exhaustion

**Symptom**: `503 Service Unavailable` with `Connection pool exhausted`
**Solution**:

- Reduce concurrent requests
- Increase `LBUG_CONNECTION_POOL_SIZE`
- Scale out instances

#### 2. Memory Pressure

**Symptom**: Slow queries, OOM errors
**Solution**:

- Monitor `DatabaseUtilizationPercent` metric
- Upgrade tier or reduce databases per instance
- Enable query result streaming

#### 3. Ingestion Queue Full

**Symptom**: `503` with `Retry-After` header
**Solution**:

- Respect backpressure signals
- Reduce ingestion rate
- Tune ingestion batch sizes

#### 4. Volume Space Issues

**Symptom**: Write failures, database corruption
**Solution**:

- Volume Monitor auto-expands at 80%
- Manual expansion via AWS Console
- Check snapshot retention policy

### Debugging Commands

```bash
# Check instance status (replace 'ladybug-standard' with your tier)
aws dynamodb scan \
  --table-name robosystems-graph-prod-instance-registry \
  --filter-expression "cluster_tier = :tier" \
  --expression-attribute-values '{":tier":{"S":"ladybug-standard"}}'

# View recent logs (replace with actual log group name for your tier)
aws logs tail /robosystems/prod/ladybug-writer-standard \
  --follow --filter-pattern ERROR

# Check volume usage
aws ec2 describe-volumes \
  --filters "Name=tag:Component,Values=LadybugDBWriter" \
  --query 'Volumes[*].[VolumeId,Size,State]'

# Force instance refresh (replace 'standard' with your tier suffix)
aws autoscaling start-instance-refresh \
  --auto-scaling-group-name ladybug-writers-standard-prod
```

### Performance Tuning

#### Query Optimization

- Use `LIMIT` clauses to reduce result sets
- Enable streaming for large results
- Create appropriate indexes
- Use parameterized queries

#### Ingestion Optimization

- Use `ignore_errors=true` for duplicate handling
- Batch multiple files in single request
- Higher priority (1-10) for urgent data
- Monitor queue depth metrics

#### Memory Optimization

- Multi-tenant configurations (standard tier) share memory across databases (e.g., 2GB per database with 10 databases per instance)
- Dedicated configurations (large/xlarge tiers) provide isolated memory per database (14GB for large, 28GB for xlarge)
- Shared repositories use memory pooling
- Monitor memory usage metrics in CloudWatch for your configuration

## Known Limitations

1. **Sequential Ingestion**: Files processed one at a time per database (LadybugDB constraint)
2. **Connection Limit**: Default 3 connections per database, configurable via `LBUG_CONNECTION_POOL_SIZE` (10 in production)
3. **Single Writer**: Only one write operation per database at a time
4. **No Cross-Database Queries**: Each query scoped to single database
5. **Volume Attachment**: One EBS volume per database (no striping)

## Contributing

1. Follow existing patterns in codebase
2. Add comprehensive tests for new endpoints
3. Update OpenAPI documentation
4. Test multi-database isolation
5. Monitor resource usage during development
6. Use `just lint` and `just format` before commits

## Support

- **Internal Documentation**: See `/docs/ladybug-architecture.md`
- **Runbooks**: Available in `/runbooks/ladybug-operations/`
- **Monitoring Dashboard**: Grafana at `https://grafana.robosystems.ai`
- **Alerts**: Via PagerDuty integration with SNS topics
