# Kuzu API Client & Factory

## Overview

The Kuzu Client and Factory system provides the critical interface between the RoboSystems application (API and workers) and the Kuzu API running on EC2 instances. This layer handles intelligent routing, connection pooling, circuit breaking, and automatic failover to ensure reliable graph database operations at scale.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Application Layer                           │
│                  (FastAPI Routes / Celery Workers)              │
├─────────────────────────────────────────────────────────────────┤
│                    KuzuClientFactory                            │
│              (Intelligent Routing & Discovery)                  │
├─────────────────────────────────────────────────────────────────┤
│                      KuzuClient                                 │
│         (Async/Sync HTTP Client with Retry Logic)               │
├─────────────────────────────────────────────────────────────────┤
│                    EC2 Infrastructure                           │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│   │ Standard │  │Enterprise│  │ Premium  │  │  Shared  │        │
│   │ Writers  │  │ Writers  │  │ Writers  │  │  Master  │        │
│   └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
│                        ┌──────────────┐                         │
│                        │ Replica ALB  │                         │
│                        └──────────────┘                         │
└─────────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. KuzuClientFactory (`factory.py`)

The factory is responsible for intelligent routing decisions based on:

- **Graph Type**: User graphs vs shared repositories (SEC, industry, economic)
- **Operation Type**: Read vs Write operations
- **Environment**: Development, Staging, Production
- **Tier**: Standard, Enterprise, Premium for user graphs

#### Routing Logic

```python
# Shared Repositories (SEC, industry, economic)
├── Write Operations → Shared Master (always)
└── Read Operations
    ├── Production/Staging
    │   ├── Try Replica ALB first (if enabled)
    │   └── Fallback to Shared Master (if allowed)
    └── Development → Local Kuzu Instance

# User Graphs (kg1a2b3c4d5 format)
├── All Operations → Discover from DynamoDB
└── Route to appropriate tier writer instance
```

#### Key Features

- **Dynamic Discovery**: Automatically discovers EC2 instances from DynamoDB
- **Circuit Breakers**: Prevents cascading failures with configurable thresholds
- **Connection Pooling**: Reuses HTTP/2 connections for efficiency
- **Redis Caching**: Caches instance locations to reduce DynamoDB lookups
- **Automatic Failover**: Falls back to alternative endpoints when primary unavailable
- **Retry Logic**: Exponential backoff with jitter for transient errors

### 2. KuzuClient (`client.py`)

Asynchronous HTTP client for interacting with Kuzu API endpoints.

#### Core Operations

```python
# Query execution
result = await client.query(
    cypher="MATCH (n:Entity) RETURN n LIMIT 10",
    graph_id="kg1a2b3c4d5",
    parameters={"param1": "value1"},
    streaming=False  # Enable for large results
)

# Database management
await client.create_database(graph_id="kg1a2b3c4d5", schema_type="entity")
await client.delete_database(graph_id="kg1a2b3c4d5")

# Data ingestion
task = await client.ingest(
    graph_id="kg1a2b3c4d5",
    mode="async",  # or "sync" for immediate processing
    bucket="robosystems-data",
    files=["entities.parquet", "relationships.parquet"],
    priority=7
)

# Health checks
health = await client.health_check()
```

#### Error Handling

The client implements sophisticated error handling:

- **Retriable Errors**: Network issues, timeouts, 503 errors
- **Non-Retriable Errors**: Syntax errors, authentication failures
- **Circuit Breaking**: Opens after threshold failures, auto-closes after timeout

### 3. Configuration (`config.py`)

Centralized configuration management with environment variable support.

```python
# Default configuration values
timeout: 30 seconds
max_retries: 3
retry_delay: 1.0 seconds
retry_backoff: 2.0 (exponential)
max_connections: 100
circuit_breaker_threshold: 5 failures
circuit_breaker_timeout: 60 seconds
```

## Usage Patterns

### Basic Usage

```python
from robosystems.graph_api.client.factory import KuzuClientFactory

# Create client with automatic routing
client = await KuzuClientFactory.create_client(
    graph_id="sec",           # Routes to shared infrastructure
    operation_type="read",    # Determines read vs write routing
    environment="prod",       # Optional, defaults to env.ENVIRONMENT
    tier=None                # Not needed for shared repositories
)

# Use the client
async with client:
    result = await client.query("MATCH (c:Company) RETURN c LIMIT 10")
```

### User Graph Operations

```python
from robosystems.middleware.graph.types import InstanceTier

# Create client for user graph
client = await KuzuClientFactory.create_client(
    graph_id="kg1a2b3c4d5",           # User graph ID
    operation_type="write",            # Write operation
    tier=InstanceTier.ENTERPRISE      # Tier determines routing
)

# Perform operations
await client.create_database(graph_id="kg1a2b3c4d5", schema_type="entity")
await client.ingest(
    graph_id="kg1a2b3c4d5",
    mode="async",
    bucket="robosystems-data",
    files=["data.parquet"]
)
```

### Streaming Large Results

```python
# Enable streaming for large result sets
result = await client.query(
    cypher="MATCH (n) RETURN n",
    graph_id="sec",
    streaming=True  # Returns NDJSON stream
)

# Process streamed results
async for chunk in result:
    process_chunk(chunk)
```

### Error Handling

```python
from robosystems.graph_api.client.exceptions import (
    KuzuTimeoutError,
    KuzuSyntaxError,
    ServiceUnavailableError
)

try:
    client = await KuzuClientFactory.create_client(
        graph_id="kg1a2b3c4d5",
        operation_type="write"
    )
    result = await client.query("INVALID CYPHER")

except KuzuSyntaxError as e:
    # Syntax errors are never retried
    logger.error(f"Invalid Cypher syntax: {e}")

except KuzuTimeoutError as e:
    # Timeouts are automatically retried
    logger.warning(f"Query timeout after retries: {e}")

except ServiceUnavailableError as e:
    # Service unavailable (circuit breaker open, no instances)
    logger.error(f"Service unavailable: {e}")
```

## Environment Variables

### Core Configuration

```bash
# API Endpoints
KUZU_API_URL=http://localhost:8001           # Default API URL (dev/fallback)
KUZU_REPLICA_ALB_URL=http://alb.internal     # Replica ALB endpoint
KUZU_API_KEY=kuzu_prod_64chars...            # Authentication key

# Feature Flags
KUZU_RETRY_LOGIC_ENABLED=true                # Enable automatic retries
KUZU_CIRCUIT_BREAKERS_ENABLED=true           # Enable circuit breakers
KUZU_HEALTH_CHECKS_ENABLED=true              # Enable health checking
KUZU_REDIS_CACHE_ENABLED=true                # Enable Redis caching
SHARED_REPLICA_ALB_ENABLED=true              # Enable replica ALB routing
ALLOW_SHARED_MASTER_READS=true               # Allow reads from master

# Performance Tuning
KUZU_CLIENT_TIMEOUT=30                       # Request timeout (seconds)
KUZU_CLIENT_MAX_RETRIES=3                    # Maximum retry attempts
KUZU_CIRCUIT_BREAKER_THRESHOLD=5             # Failures before opening
KUZU_CIRCUIT_BREAKER_TIMEOUT=60              # Seconds before reset
KUZU_CACHE_TTL=300                           # Cache TTL (seconds)
```

### DynamoDB Configuration

```bash
# For instance discovery
KUZU_INSTANCE_REGISTRY_TABLE=robosystems-kuzu-{env}-instance-registry
KUZU_GRAPH_REGISTRY_TABLE=robosystems-kuzu-{env}-graph-registry
```

## Instance Discovery Flow

1. **Check Cache**: Redis cache with 5-minute TTL
2. **Query DynamoDB**: Find instance hosting the graph
3. **Health Check**: Verify instance is healthy
4. **Create Client**: Initialize with discovered endpoint
5. **Cache Result**: Store for future requests

```python
# Internal discovery flow (handled automatically)
1. KuzuClientFactory.create_client("kg1a2b3c4d5")
2. → Check Redis: kuzu:prod:location:kg1a2b3c4d5
3. → Query DynamoDB: GraphRegistry[graph_id=kg1a2b3c4d5]
4. → Get instance: i-1234567890 at 10.0.1.100
5. → Create client: http://10.0.1.100:8001
6. → Cache location for 300 seconds
```

## Circuit Breaker Pattern

The circuit breaker prevents cascading failures:

```
CLOSED (Normal Operation)
    ↓ [5 failures]
OPEN (Requests blocked for 60s)
    ↓ [Timeout expires]
HALF-OPEN (Test single request)
    ↓ [Success]
CLOSED (Resume normal operation)
```

## Performance Optimization

### Connection Pooling

```python
# Connections are reused across requests
max_connections: 100         # Total connections
max_keepalive_connections: 20 # Persistent connections
keepalive_expiry: 5.0        # Seconds before closing idle
```

### Caching Strategy

```python
# Redis caching layers
1. Instance locations: 5-minute TTL
2. ALB health status: 30-second TTL
3. Shared master URL: 5-minute TTL
```

### Retry Strategy

```python
# Exponential backoff with jitter
Attempt 1: Immediate
Attempt 2: 1.0s + jitter (0-0.1s)
Attempt 3: 2.0s + jitter (0-0.2s)
Attempt 4: 4.0s + jitter (0-0.4s)
```

## Monitoring & Debugging

### Logging

```python
# Enable debug logging
import logging
logging.getLogger("robosystems.graph_api").setLevel(logging.DEBUG)

# Logs include:
- Routing decisions
- Instance discovery
- Circuit breaker state changes
- Retry attempts
- Cache hits/misses
```

### Metrics

The client tracks:

- Request latency by operation
- Circuit breaker trips
- Retry counts
- Cache hit rates
- Connection pool utilization

### Health Checks

```python
# Check specific endpoint health
client = await KuzuClientFactory.create_client("sec", "read")
health = await client.health_check()
print(f"Status: {health['status']}")
print(f"Databases: {health['databases']}")
print(f"Memory: {health['memory_usage_mb']}MB")
```

## Best Practices

### 1. Use Context Managers

```python
# Ensures proper cleanup
async with await KuzuClientFactory.create_client("sec") as client:
    result = await client.query("MATCH (n) RETURN n")
```

### 2. Handle Transient Errors

```python
# The client automatically retries, but set appropriate timeouts
client = await KuzuClientFactory.create_client(
    "kg1a2b3c4d5",
    timeout=60  # Longer timeout for complex queries
)
```

### 3. Stream Large Results

```python
# Prevent memory issues with large datasets
result = await client.query(
    "MATCH (n) RETURN n",
    streaming=True  # Enable NDJSON streaming
)
```

### 4. Use Appropriate Tiers

```python
# Match tier to workload requirements
InstanceTier.STANDARD    # Most workloads (10 DBs/instance)
InstanceTier.ENTERPRISE  # Isolated resources (1 DB/instance)
InstanceTier.PREMIUM     # Maximum performance (1 DB/instance, xlarge)
```

## Troubleshooting

### Common Issues

#### 1. ServiceUnavailableError

**Cause**: No healthy instances available
**Solution**:

- Check DynamoDB registries for instance status
- Verify EC2 instances are running
- Check CloudWatch logs for instance errors

#### 2. Circuit Breaker Open

**Cause**: Too many consecutive failures
**Solution**:

- Wait for timeout (60 seconds)
- Check target service health
- Review CloudWatch metrics

#### 3. Authentication Failures

**Cause**: Invalid or missing API key
**Solution**:

- Verify KUZU_API_KEY is set
- Check Secrets Manager for correct key
- Ensure key matches environment

#### 4. Timeout Errors

**Cause**: Query taking too long
**Solution**:

- Optimize Cypher query
- Increase timeout for complex queries
- Consider pagination or streaming

### Debug Commands

```bash
# Check instance registry
aws dynamodb scan \
  --table-name robosystems-kuzu-prod-instance-registry \
  --filter-expression "status = :healthy" \
  --expression-attribute-values '{":healthy":{"S":"healthy"}}'

# Check graph location
aws dynamodb get-item \
  --table-name robosystems-kuzu-prod-graph-registry \
  --key '{"graph_id":{"S":"kg1a2b3c4d5"}}'

# Test direct connection
curl -X GET http://10.0.1.100:8001/health \
  -H "X-Kuzu-API-Key: $KUZU_API_KEY"
```

## Security Considerations

1. **API Keys**: Stored in AWS Secrets Manager, rotated every 90 days
2. **Network Isolation**: All traffic within VPC, no public endpoints
3. **TLS**: Optional TLS for inter-service communication
4. **Authentication**: Every request requires valid API key
5. **Rate Limiting**: Configurable per-client limits

## Integration Examples

### FastAPI Route

```python
from fastapi import APIRouter, Depends
from robosystems.graph_api.client.factory import KuzuClientFactory

router = APIRouter()

@router.get("/graph/{graph_id}/query")
async def execute_query(
    graph_id: str,
    cypher: str,
    client = Depends(get_kuzu_client)
):
    # Client is automatically created with proper routing
    result = await client.query(cypher, graph_id)
    return result

async def get_kuzu_client(graph_id: str):
    return await KuzuClientFactory.create_client(
        graph_id=graph_id,
        operation_type="read"
    )
```

### Celery Task

```python
from celery import shared_task
from robosystems.graph_api.client.factory import KuzuClientFactory

@shared_task
async def process_graph_data(graph_id: str, data_files: list):
    # Create client for write operations
    client = await KuzuClientFactory.create_client(
        graph_id=graph_id,
        operation_type="write",
        tier=InstanceTier.STANDARD
    )

    # Ingest data
    task = await client.ingest(
        graph_id=graph_id,
        mode="async",
        bucket="robosystems-data",
        files=data_files,
        priority=5
    )

    # Monitor task
    while True:
        status = await client.get_task_status(task.task_id)
        if status["status"] in ["completed", "failed"]:
            break
        await asyncio.sleep(5)

    return status
```

## Performance Benchmarks

Typical latencies in production:

| Operation         | P50   | P95    | P99    |
| ----------------- | ----- | ------ | ------ |
| Simple Query      | 15ms  | 45ms   | 120ms  |
| Complex Query     | 200ms | 800ms  | 2000ms |
| Ingestion (Sync)  | 500ms | 2000ms | 5000ms |
| Ingestion (Async) | 50ms  | 100ms  | 200ms  |
| Health Check      | 5ms   | 15ms   | 30ms   |

## Future Enhancements

1. **GraphQL Support**: Native GraphQL to Cypher translation
2. **Query Caching**: Cache frequent query results in Redis
3. **Load Balancing**: Client-side load balancing for writers
4. **Metrics Export**: Prometheus/OpenTelemetry integration
5. **Connection Multiplexing**: HTTP/3 with QUIC support

## Support

For issues or questions:

- Check CloudWatch logs: `/robosystems/{env}/kuzu-client`
- Review metrics in Grafana dashboards
- Consult runbooks in `/runbooks/kuzu-client/`
