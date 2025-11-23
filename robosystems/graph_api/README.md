# Graph API

High-performance HTTP API server for graph database cluster management with pluggable backend support. FastAPI-based microservice that provides REST endpoints for multi-tenant graph operations with enterprise-grade reliability and security.

**Supported Backends:**

- **LadybugDB** (Default): High-performance embedded graph database based on columnar storage
- **Neo4j Community**: Client-server architecture with advanced features
- **Neo4j Enterprise**: TODO - Multi-database support and clustering not yet implemented

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
│          (FastAPI on Port 8001/8002 depending on backend)   │
├─────────────────────────────────────────────────────────────┤
│                Backend Abstraction Layer                    │
│         (Pluggable: LadybugDB, Neo4j Community/Enterprise)       │
├─────────────────────────────────────────────────────────────┤
│                   Graph Database Engine                     │
│              (LadybugDB Embedded or Neo4j Bolt)                  │
└─────────────────────────────────────────────────────────────┘
```

### Core Components

```
graph_api/
├── app.py                      # FastAPI application factory
├── main.py                     # Server entry point
├── __main__.py                 # Module entry point
│
├── backends/                   # Backend implementations
│   ├── base.py                # Abstract backend interface
│   ├── lbug.py                # LadybugDB backend implementation
│   ├── neo4j.py               # Neo4j backend implementation
│   └── __init__.py            # Backend factory
│
├── client/                     # Python clients
│   ├── client.py              # Async client implementation
│   ├── sync_client.py         # Synchronous client
│   ├── factory.py             # Intelligent routing factory
│   ├── config.py              # Client configuration
│   └── exceptions.py          # Custom exceptions
│
├── core/                      # Core services
│   ├── cluster_manager.py    # Cluster orchestration
│   ├── database_manager.py   # Database lifecycle management
│   ├── duckdb_manager.py     # DuckDB staging database management
│   ├── duckdb_pool.py        # DuckDB connection pooling
│   ├── connection_pool.py    # Graph connection pooling
│   ├── admission_control.py  # Backpressure management
│   └── metrics_collector.py  # Performance metrics
│
├── routers/                   # API endpoints
│   ├── databases/
│   │   ├── management.py     # Create/delete databases
│   │   ├── query.py          # Cypher query execution
│   │   ├── ingest.py         # S3 bulk copy operations
│   │   ├── tables/           # DuckDB staging table management
│   │   │   ├── management.py # Create/list staging tables
│   │   │   ├── ingest.py     # Parquet file ingestion to tables
│   │   │   └── query.py      # DuckDB SQL queries on tables
│   │   ├── schema.py         # Schema management
│   │   ├── metrics.py        # Database metrics
│   │   ├── backup.py         # Backup operations
│   │   └── restore.py        # Restore operations
│   ├── health.py             # Health checks
│   ├── info.py               # Node information
│   └── tasks.py              # Background task tracking
│
├── middleware/
│   ├── auth.py               # API key authentication (backend-agnostic)
│   └── request_limits.py     # Rate limiting
│
└── models/                    # Pydantic models
    ├── database.py           # Database schemas
    ├── ingestion.py          # Ingestion requests
    ├── streaming.py          # NDJSON streaming
    └── cluster.py            # Cluster configuration
```

### Node Types (LadybugDB Backend)

When using the LadybugDB backend, the system deploys different node types:

- **Writer Nodes** (`writer`): Entity database read/write operations on EC2
- **Shared Master** (`shared_master`): Shared repository ingestion and writes on EC2

### Backend Selection (Neo4j)

When using Neo4j backend:

- **Community Edition**: Single database instance with core graph features (currently implemented)
- **Enterprise Edition**: TODO - Multi-database support with clustering not yet implemented

## Deployment Infrastructure

### CloudFormation Stack Architecture

The Graph API deployment architecture varies by backend:

#### LadybugDB Backend Infrastructure

For LadybugDB deployments, the system uses a sophisticated multi-stack CloudFormation architecture:

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

#### Neo4j Backend Infrastructure

For Neo4j deployments:

```
1. Neo4j Database Stack (neo4j-db.yaml)
   ├─ ECS Fargate Service or EC2 instances
   ├─ EBS volumes for data persistence
   ├─ Application Load Balancer
   └─ Multi-AZ deployment for Enterprise

2. Neo4j API Stack (neo4j-api.yaml)
   ├─ ECS Fargate Service (Graph API)
   ├─ Bolt connection to Neo4j database
   └─ Health checks and auto-scaling
```

### Infrastructure Configuration

Infrastructure is configurable based on workload requirements. Example configurations:

#### Production Environment

| Configuration Type      | Instance Type | DBs/Instance | Memory/DB | Scaling | Use Case                        |
| ----------------------- | ------------- | ------------ | --------- | ------- | ------------------------------- |
| **Multi-Tenant**        | r7g.xlarge    | 10           | 2GB       | 1-10    | Cost-effective shared resources |
| **Dedicated**           | r7g.large     | 1            | 14GB      | 0-5     | Isolated workloads              |
| **High-Performance**    | r7g.xlarge    | 1            | 28GB      | 0-3     | Maximum performance             |
| **Shared Repositories** | r7g.large     | N/A          | Shared    | 1       | Public data (SEC, etc.)         |

#### Staging Environment

| Configuration Type      | Instance Type | DBs/Instance | Memory/DB | Scaling |
| ----------------------- | ------------- | ------------ | --------- | ------- |
| **Multi-Tenant**        | r7g.medium    | 10           | 700MB     | 1-5     |
| **Shared Repositories** | r7g.medium    | N/A          | Shared    | 1       |

### DynamoDB Registry Tables

#### Instance Registry

Tracks all LadybugDB instances across the infrastructure:

```python
{
    "instance_id": "i-1234567890",      # EC2 instance ID
    "cluster_tier": "standard",         # Actual tier from deployment config
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
    "tier": "standard",
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
  -e WRITER_TIER=standard \
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
Authorization: X-Graph-API-Key: {api_key}
Content-Type: application/json

{
  "graph_id": "kg1a2b3c4d5",
  "schema_type": "entity"  // entity|shared|custom
}
```

#### Execute Query

```http
POST /databases/{graph_id}/query
Authorization: X-Graph-API-Key: {api_key}
Content-Type: application/json

{
  "cypher": "MATCH (n:Entity) RETURN n LIMIT 10",
  "parameters": {},
  "timeout": 30
}
```

#### Data Ingestion

**S3 Bulk Copy:**

```http
POST /databases/{graph_id}/copy
Authorization: X-Graph-API-Key: {api_key}
Content-Type: application/json

{
  "s3_pattern": "s3://robosystems-data/path/*.parquet",
  "table_name": "Entity",
  "ignore_errors": true,
  "s3_credentials": {
    "aws_access_key_id": "AKIA...",
    "aws_secret_access_key": "...",
    "region": "us-east-1"
  }
}

Note: S3 bulk copy is LadybugDB-specific. Neo4j uses alternative data loading methods.
```

This returns a task ID that can be monitored via Server-Sent Events:

```http
GET /tasks/{task_id}/monitor
Authorization: X-Graph-API-Key: {api_key}
```

#### Table Operations

**DuckDB Staging Tables** provide an intermediate staging layer for data validation and transformation before graph ingestion.

**Note:** This is the low-level Graph API (port 8001). Individual file uploads and tracking are handled by the main API layer (port 8000):
- `POST /v1/graphs/{graph_id}/tables/{table_name}/files` - Get presigned S3 upload URL
- `PATCH /v1/graphs/{graph_id}/tables/files/{file_id}` - Mark upload complete (automatically calls create table here)

**Create Table:**

```http
POST /databases/{graph_id}/tables
Authorization: X-Graph-API-Key: {api_key}
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
Authorization: X-Graph-API-Key: {api_key}

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
Authorization: X-Graph-API-Key: {api_key}
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

**Ingest Table to Graph:**

```http
POST /databases/{graph_id}/tables/{table_name}/ingest
Authorization: X-Graph-API-Key: {api_key}
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

Note: This performs direct DuckDB → LadybugDB ingestion via database extensions.
Use rebuild=true to regenerate the graph database from scratch (safe operation).
```

**Delete Table:**

```http
DELETE /databases/{graph_id}/tables/{table_name}
Authorization: X-Graph-API-Key: {api_key}

Response: {
  "status": "success",
  "message": "Table deleted successfully"
}
```

### System Operations

#### Health Check

```http
GET /status
Response: {
  "status": "healthy",
  "node_type": "writer",
  "tier": "standard",
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
  "cluster_tier": "standard",
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

### Async Client

```python
from robosystems.graph_api.client import AsyncGraphClient

async with AsyncGraphClient(
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

    # Bulk copy from S3
    task = await client.copy_from_s3(
        graph_id="kg1a2b3c4d5",
        s3_pattern="s3://data-bucket/path/*.parquet",
        table_name="Entity",
        ignore_errors=True
    )

    # Monitor task
    status = await client.get_task_status(task.task_id)
```

### Sync Client

```python
from robosystems.graph_api.client import GraphClient

client = GraphClient(
    base_url="http://graph-api:8001",
    api_key="graph_api_..."
)

# Synchronous operations
data = client.query(
    graph_id="kg1a2b3c4d5",
    cypher="MATCH (n) RETURN n LIMIT 10"
)
```

### Client Factory with Intelligent Routing

```python
from robosystems.graph_api.client.factory import get_graph_client

# Factory handles routing based on graph type and operation
client = await get_graph_client(
    graph_id="sec",              # Routes to shared infrastructure
    operation_type="read",        # Could use replica
    environment="prod",
    tier=InstanceTier.STANDARD
)
```

## Configuration

### Environment Variables

```bash
# Backend Configuration
GRAPH_BACKEND_TYPE=ladybug                 # ladybug|neo4j_community|neo4j_enterprise

# Node Configuration (LadybugDB Backend)
LBUG_NODE_TYPE=writer                    # writer|shared_master
WRITER_TIER=standard                     # standard|large|xlarge|shared
LBUG_DATABASE_PATH=/data/lbug-dbs       # Storage location
LBUG_PORT=8001                           # API port (8001 for LadybugDB, 8002 for Neo4j)

# Neo4j Configuration (Neo4j Backend)
NEO4J_URI=bolt://neo4j-db:7687          # Neo4j Bolt connection
NEO4J_USERNAME=neo4j                     # Neo4j username
NEO4J_PASSWORD=                          # Retrieved from Secrets Manager
NEO4J_ENTERPRISE=false                   # Enable multi-database support
GRAPH_API_PORT=8002                      # API port for Neo4j backend

# Performance Settings
LBUG_MAX_DATABASES_PER_NODE=10          # Configuration-based limit (LadybugDB)
LBUG_MAX_MEMORY_MB=14336                # Total memory allocation (LadybugDB)
LBUG_MEMORY_PER_DB_MB=2048              # Per-database memory (LadybugDB)
LBUG_CHUNK_SIZE=1000                    # Streaming chunk size
LBUG_QUERY_TIMEOUT=30                   # Query timeout seconds
LBUG_MAX_QUERY_LENGTH=10000             # Max query characters
LBUG_CONNECTION_POOL_SIZE=10            # Connections per database
NEO4J_MAX_CONNECTION_POOL_SIZE=50       # Neo4j connection pool size

# Authentication
GRAPH_API_KEY=                           # Unified API key (both backends)

# AWS Configuration
AWS_DEFAULT_REGION=us-east-1
DATABASE_URL=postgresql://...           # PostgreSQL for metadata
AWS_S3_BUCKET=robosystems-data         # S3 for ingestion

# Feature Flags
LBUG_CIRCUIT_BREAKERS_ENABLED=true     # Enable circuit breakers
LBUG_REDIS_CACHE_ENABLED=true          # Enable Redis caching
LBUG_RETRY_LOGIC_ENABLED=true          # Enable automatic retries
LBUG_HEALTH_CHECKS_ENABLED=true        # Enable health checking
```

### Schema Types

- **Entity**: Multi-tenant databases with accounting extensions
- **Shared**: Repository databases (SEC, industry, economic)
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
- **Security Groups**: Port 8001/8002 restricted to VPC CIDR
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
curl http://ladybug-writer:8001/status

# Database health
curl http://ladybug-writer:8001/status/databases

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
  "tier": "standard",
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
  -e WRITER_TIER=standard \
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
# Check instance status (replace 'standard' with your actual tier: standard|large|xlarge)
aws dynamodb scan \
  --table-name robosystems-graph-prod-instance-registry \
  --filter-expression "cluster_tier = :tier" \
  --expression-attribute-values '{":tier":{"S":"standard"}}'

# View recent logs (replace with actual log group name for your tier)
aws logs tail /robosystems/prod/ladybug-writer-standard \
  --follow --filter-pattern ERROR

# Check volume usage
aws ec2 describe-volumes \
  --filters "Name=tag:Component,Values=LadybugDBWriter" \
  --query 'Volumes[*].[VolumeId,Size,State]'

# Force instance refresh (replace 'standard' with your actual tier: standard|large|xlarge)
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

### LadybugDB Backend

1. **Sequential Ingestion**: Files processed one at a time per database (LadybugDB constraint)
2. **Connection Limit**: Maximum 3 concurrent connections per database
3. **Single Writer**: Only one write operation per database at a time
4. **No Cross-Database Queries**: Each query scoped to single database
5. **Volume Attachment**: One EBS volume per database (no striping)

### Neo4j Backend

1. **Single Database**: Community edition supports single database only
2. **Connection Pooling**: Managed by Neo4j driver within Graph API
3. **Bolt Protocol**: Internal connection between Graph API and Neo4j
4. **Multi-Database**: TODO - Enterprise edition with clustering not yet implemented

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
