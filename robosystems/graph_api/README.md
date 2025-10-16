# Kuzu API

High-performance HTTP API server for Kuzu graph database cluster management. FastAPI-based microservice that runs alongside Kuzu databases on EC2 instances, providing REST endpoints for multi-tenant graph operations with enterprise-grade reliability and security.

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
│                   KuzuClientFactory Layer                   │
│              (Circuit Breakers, Retry Logic)                │
├─────────────────────────────────────────────────────────────┤
│                      Kuzu API Layer                         │
│                  (FastAPI on Port 8001)                     │
├─────────────────────────────────────────────────────────────┤
│                   Kuzu Database Engine                      │
│                  (Native Graph Database)                    │
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
│   ├── client.py              # Async client implementation
│   ├── sync_client.py         # Synchronous client
│   ├── factory.py             # Intelligent routing factory
│   ├── config.py              # Client configuration
│   └── exceptions.py          # Custom exceptions
│
├── core/                      # Core services
│   ├── cluster_manager.py    # Cluster orchestration
│   ├── database_manager.py   # Database lifecycle management
│   ├── connection_pool.py    # Connection pooling (max 3/DB)
│   ├── admission_control.py  # Backpressure management
│   └── metrics_collector.py  # Performance metrics
│
├── routers/                   # API endpoints
│   ├── databases/
│   │   ├── management.py     # Create/delete databases
│   │   ├── query.py          # Cypher query execution
│   │   ├── ingest.py         # S3 bulk copy operations
│   │   ├── schema.py         # Schema management
│   │   ├── metrics.py        # Database metrics
│   │   ├── backup.py         # Backup operations
│   │   └── restore.py        # Restore operations
│   ├── health.py             # Health checks
│   ├── info.py               # Node information
│   └── tasks.py              # Background task tracking
│
├── middleware/
│   ├── auth.py               # API key authentication
│   └── request_limits.py     # Rate limiting
│
└── models/                    # Pydantic models
    ├── database.py           # Database schemas
    ├── ingestion.py          # Ingestion requests
    ├── streaming.py          # NDJSON streaming
    └── cluster.py            # Cluster configuration
```

### Node Types

- **Writer Nodes** (`writer`): Entity database read/write operations on EC2
- **Shared Master** (`shared_master`): Repository ingestion and writes on EC2
- **Shared Replica** (`shared_replica`): Read-only replicas on EC2 with ALB

## Deployment Infrastructure

### CloudFormation Stack Architecture

The Kuzu API is deployed through a sophisticated multi-stack CloudFormation architecture:

```
1. Infrastructure Stack (kuzu-infra.yaml)
   ├─ DynamoDB Tables (Instance, Graph, Volume Registry)
   ├─ Secrets Manager (API Keys with rotation)
   ├─ SNS Topics (Alerts and notifications)
   └─ Lambda Functions (Instance monitoring)

2. Volume Management Stack (kuzu-volumes.yaml)
   ├─ Volume Manager Lambda (EBS lifecycle)
   ├─ Volume Monitor Lambda (Auto-expansion)
   ├─ Snapshot Management (Backup/restore)
   └─ SNS Topics (Volume alerts)

3. Writer Stacks (kuzu-writers.yaml) - Deployed in parallel
   ├─ Standard Writers (r7g.large, 10 DBs/instance)
   ├─ Enterprise Writers (r7g.large, 1 DB/instance)
   ├─ Premium Writers (r7g.xlarge, 1 DB/instance)
   └─ Shared Master (r7g.large, SEC repository)

4. Replica Stack (kuzu-shared-replicas.yaml)
   ├─ Auto Scaling Group (2-20 instances)
   ├─ Application Load Balancer
   └─ Read-only EC2 instances (r7g.medium)
```

### Infrastructure Tiers

#### Production Environment

| Tier           | Instance Type | DBs/Instance | Memory/DB | Scaling | Use Case            |
| -------------- | ------------- | ------------ | --------- | ------- | ------------------- |
| **Standard**   | r7g.xlarge    | 10           | 2GB       | 1-10    | Most customers      |
| **Enterprise** | r7g.large     | 1            | 14GB      | 0-5     | Isolated workloads  |
| **Premium**    | r7g.xlarge    | 1            | 28GB      | 0-3     | Maximum performance |
| **Shared**     | r7g.large     | N/A          | Shared    | 1       | SEC/Industry data   |

#### Staging Environment

| Tier         | Instance Type | DBs/Instance | Memory/DB | Scaling |
| ------------ | ------------- | ------------ | --------- | ------- |
| **Standard** | r7g.medium    | 10           | 700MB     | 1-5     |
| **Shared**   | r7g.medium    | N/A          | Shared    | 1       |

### DynamoDB Registry Tables

#### Instance Registry

Tracks all Kuzu instances across the infrastructure:

```python
{
    "instance_id": "i-1234567890",      # EC2 instance ID
    "cluster_tier": "standard",         # Tier designation
    "private_ip": "10.0.1.100",
    "status": "healthy",                # initializing|healthy|unhealthy
    "database_count": 5,                # Current databases
    "max_databases": 10,                # Tier limit
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
deploy-kuzu.yml (Orchestrator)
├── deploy-kuzu-infra.yml
│   └── Creates DynamoDB, Secrets, SNS
├── deploy-kuzu-volumes.yml
│   └── Deploys Lambda functions for volume management
├── prepare-writer-matrix
│   └── Parses .github/configs/kuzu.yml for tier specs
├── deploy-kuzu-writers.yml (Matrix strategy, parallel)
│   └── Deploys each tier based on configuration
└── deploy-kuzu-shared-replicas.yml
└── Creates read replica infrastructure
```

### EC2 UserData Initialization

The Kuzu API starts automatically on EC2 instances via userdata script:

```bash
# 1. Register instance in DynamoDB
aws dynamodb put-item --table-name instance-registry ...

# 2. Invoke Volume Manager for EBS attachment
aws lambda invoke --function-name volume-manager ...

# 3. Pull and start Docker container
docker run -d \
  -p 8001:8001 \
  -v /data/kuzu-dbs:/data/kuzu-dbs \
  -e KUZU_NODE_TYPE=writer \
  -e WRITER_TIER=standard \
  -e KUZU_API_KEY=${KUZU_API_KEY} \
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
Authorization: X-Kuzu-API-Key: {api_key}
Content-Type: application/json

{
  "graph_id": "kg1a2b3c4d5",
  "schema_type": "entity"  // entity|shared|custom
}
```

#### Execute Query

```http
POST /databases/{graph_id}/query
Authorization: X-Kuzu-API-Key: {api_key}
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
Authorization: X-Kuzu-API-Key: {api_key}
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
```

This returns a task ID that can be monitored via Server-Sent Events:

```http
GET /tasks/{task_id}/monitor
Authorization: X-Kuzu-API-Key: {api_key}
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
from robosystems.graph_api.client import AsyncKuzuClient

async with AsyncKuzuClient(
    base_url="http://kuzu-writer:8001",
    api_key="kuzu_prod_..."
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
from robosystems.graph_api.client import KuzuClient

client = KuzuClient(
    base_url="http://kuzu-writer:8001",
    api_key="kuzu_prod_..."
)

# Synchronous operations
data = client.query(
    graph_id="kg1a2b3c4d5",
    cypher="MATCH (n) RETURN n LIMIT 10"
)
```

### Client Factory with Intelligent Routing

```python
from robosystems.graph_api.client.factory import get_kuzu_client

# Factory handles routing based on graph type and operation
client = await get_kuzu_client(
    graph_id="sec",              # Routes to shared master
    operation_type="read",        # Could use replica
    environment="prod",
    tier=InstanceTier.STANDARD
)
```

## Configuration

### Environment Variables

```bash
# Node Configuration
KUZU_NODE_TYPE=writer                    # writer|shared_master|shared_replica
WRITER_TIER=standard                     # standard|enterprise|premium|shared
KUZU_DATABASE_PATH=/data/kuzu-dbs       # Storage location
KUZU_PORT=8001                           # API port

# Performance Settings
KUZU_MAX_DATABASES_PER_NODE=10          # Tier-specific limit
KUZU_MAX_MEMORY_MB=14336                # Total memory allocation
KUZU_MEMORY_PER_DB_MB=2048              # Per-database memory
KUZU_CHUNK_SIZE=1000                    # Streaming chunk size
KUZU_QUERY_TIMEOUT=30                   # Query timeout seconds
KUZU_MAX_QUERY_LENGTH=10000             # Max query characters
KUZU_CONNECTION_POOL_SIZE=10            # Connections per database

# Authentication
KUZU_API_KEY=kuzu_prod_...              # Unified API key

# AWS Configuration
AWS_DEFAULT_REGION=us-east-1
DATABASE_URL=postgresql://...           # PostgreSQL for metadata
AWS_S3_BUCKET=robosystems-data         # S3 for ingestion

# Feature Flags
KUZU_CIRCUIT_BREAKERS_ENABLED=true     # Enable circuit breakers
KUZU_REDIS_CACHE_ENABLED=true          # Enable Redis caching
KUZU_RETRY_LOGIC_ENABLED=true          # Enable automatic retries
KUZU_HEALTH_CHECKS_ENABLED=true        # Enable health checking
SHARED_REPLICA_ALB_ENABLED=false       # Enable replica ALB routing
ALLOW_SHARED_MASTER_READS=true         # Allow reads from master
```

### Schema Types

- **Entity**: Multi-tenant databases with accounting extensions
- **Shared**: Repository databases (SEC, industry, economic)
- **Custom**: Custom schemas with custom DDL

## Security

### Authentication

All API requests require authentication via API key header:

```http
X-Kuzu-API-Key: kuzu_prod_64_character_random_string
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

**Namespace**: `RoboSystemsKuzu/{Environment}`

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
curl http://kuzu-writer:8001/status

# Database health
curl http://kuzu-writer:8001/status/databases

# Detailed metrics
curl http://kuzu-writer:8001/metrics
```

### Logging

**CloudWatch Log Groups**:

- `/robosystems/{env}/kuzu-writer-standard`
- `/robosystems/{env}/kuzu-writer-enterprise`
- `/robosystems/{env}/kuzu-writer-premium`
- `/robosystems/{env}/kuzu-shared-master`

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

### OpenTelemetry Integration

```python
# Automatic tracing for all operations
OTEL_SERVICE_NAME=kuzu-writer-standard
OTEL_EXPORTER_OTLP_ENDPOINT=http://172.17.0.1:4318
```

## Development

### Local Development

```bash
# Start full stack with Docker
just start robosystems

# Run API server locally
uv run python -m robosystems.graph_api \
  --base-path ./data/kuzu-dbs \
  --node-type writer \
  --port 8001

# Use direct file access (bypass API)
export KUZU_ACCESS_PATTERN=direct_file
```

### Docker Development

```bash
docker run -d \
  -p 8001:8001 \
  -v kuzu_data:/data/kuzu-dbs \
  -e KUZU_NODE_TYPE=writer \
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
# Requires running Kuzu instance
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
  -H "X-Kuzu-API-Key: test-key" \
  -H "Content-Type: application/json" \
  -d '{"graph_id": "test_db", "schema_type": "entity"}'

# Execute query
curl -X POST http://localhost:8001/databases/test_db/query \
  -H "X-Kuzu-API-Key: test-key" \
  -H "Content-Type: application/json" \
  -d '{"cypher": "RETURN 1 as num"}'
```

## Troubleshooting

### Common Issues

#### 1. Connection Pool Exhaustion

**Symptom**: `503 Service Unavailable` with `Connection pool exhausted`
**Solution**:

- Reduce concurrent requests
- Increase `KUZU_CONNECTION_POOL_SIZE`
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
# Check instance status
aws dynamodb scan \
  --table-name robosystems-kuzu-prod-instance-registry \
  --filter-expression "cluster_tier = :tier" \
  --expression-attribute-values '{":tier":{"S":"standard"}}'

# View recent logs
aws logs tail /robosystems/prod/kuzu-writer-standard \
  --follow --filter-pattern ERROR

# Check volume usage
aws ec2 describe-volumes \
  --filters "Name=tag:Component,Values=KuzuWriter" \
  --query 'Volumes[*].[VolumeId,Size,State]'

# Force instance refresh
aws autoscaling start-instance-refresh \
  --auto-scaling-group-name kuzu-writers-standard-prod
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

- Standard tier provides 2GB per database with 10 databases per instance
- Enterprise/Premium have dedicated memory allocations
- Shared repositories use memory pooling
- Monitor memory usage metrics in CloudWatch

## Known Limitations

1. **Sequential Ingestion**: Files processed one at a time per database (Kuzu constraint)
2. **Connection Limit**: Maximum 3 concurrent connections per database
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

- **Internal Documentation**: See `/docs/kuzu-architecture.md`
- **Runbooks**: Available in `/runbooks/kuzu-operations/`
- **Monitoring Dashboard**: Grafana at `https://grafana.robosystems.ai`
- **Alerts**: Via PagerDuty integration with SNS topics
