# DuckDB Staging Layer

Production-ready DuckDB staging system for efficient data transformation and ingestion into LadybugDB graph databases.

## Overview

The DuckDB staging layer provides a high-performance data preparation pipeline for loading data into graph databases. It enables efficient bulk data transformation, S3 parquet reading, and incremental updates before final ingestion into LadybugDB.

**Key Concept**: DuckDB acts as a staging area, not a permanent store. External tables are created from S3 parquet files, transformed as needed, then ingested into the graph database. After ingestion, staging tables can be dropped.

## Architecture

```
┌─────────────────────────────────────────────┐
│     S3 Bucket (Parquet Files)              │
│  s3://bucket/graph123/entities/*.parquet    │
└──────────────┬──────────────────────────────┘
               │ read_parquet()
┌──────────────▼──────────────────────────────┐
│     DuckDB Staging Database                 │  ← One per graph_id
│  /data/duckdb-staging/graph123.duckdb       │
│  - Materialized tables from S3              │
│  - Data transformation & validation         │
│  - Deduplication & normalization            │
└──────────────┬──────────────────────────────┘
               │ COPY TO or ATTACH DATABASE
┌──────────────▼──────────────────────────────┐
│     LadybugDB Graph Database                │  ← Final destination
│  /data/lbug-dbs/graph123.lbug               │
│  - Node tables (entities)                   │
│  - Relationship tables (edges)              │
└─────────────────────────────────────────────┘
```

## Components

### 1. DuckDB Connection Pool (`pool.py`)

Thread-safe connection pooling with lifecycle management.

**Classes**:
- `DuckDBConnectionPool` - Main connection pool
- `DuckDBConnectionInfo` - Connection metadata

**Key Features**:
- Per-graph database instances (one DuckDB file per graph_id)
- Thread-safe with proper locking
- Connection TTL and automatic cleanup
- Health checking and recovery
- Configurable connection limits
- Graceful shutdown handling
- S3 credentials configuration
- Extension management (httpfs, parquet)

**Configuration**:
```python
from robosystems.graph_api.core.duckdb import (
    DuckDBConnectionPool,
    initialize_duckdb_pool,
    get_duckdb_pool
)

pool = initialize_duckdb_pool(
    base_path="/data/duckdb-staging",
    max_connections_per_db=3,        # Conservative for DuckDB
    connection_ttl_minutes=30         # 30 min TTL
)

with pool.get_connection("graph123") as conn:
    result = conn.execute("SELECT * FROM entities").fetchall()
```

**Connection Lifecycle**:
1. **Request** - Application requests connection for graph_id
2. **Check Pool** - Pool checks for available connections
3. **Reuse or Create** - Returns existing or creates new connection
4. **Configure** - Installs extensions, configures S3 access
5. **Health Check** - Validates connection with test query
6. **Use** - Application executes queries
7. **Return** - Context manager returns to pool
8. **Cleanup** - Background cleanup removes expired connections

### 2. DuckDB Table Manager (`manager.py`)

High-level table operations for staging data.

**Classes**:
- `DuckDBTableManager` - Main table manager

**Key Features**:
- External table creation from S3 parquet files
- Automatic deduplication (nodes by identifier, edges by from/to)
- Column renaming (from/to → src/dst for LadybugDB)
- Table refresh from PostgreSQL file registry
- Streaming query support with chunking
- SQL injection protection
- Incremental ingestion with file_id tracking

**Table Operations**:
```python
from robosystems.graph_api.core.duckdb import DuckDBTableManager
from robosystems.graph_api.models.tables import TableCreateRequest

manager = DuckDBTableManager()

request = TableCreateRequest(
    graph_id="graph123",
    table_name="entities",
    s3_pattern=["s3://bucket/entities/file1.parquet",
                "s3://bucket/entities/file2.parquet"]
)

response = manager.create_table(request)
print(f"Created table in {response.execution_time_ms}ms")

tables = manager.list_tables("graph123")
for table in tables:
    print(f"{table.table_name}: {table.row_count} rows")

manager.delete_table("graph123", "entities")
```

## Usage

### Initialization

```python
from robosystems.graph_api.core.duckdb import initialize_duckdb_pool

pool = initialize_duckdb_pool(
    base_path="/data/duckdb-staging",
    max_connections_per_db=3,
    connection_ttl_minutes=30
)
```

### Creating External Tables

#### From File List

```python
from robosystems.graph_api.models.tables import TableCreateRequest

request = TableCreateRequest(
    graph_id="graph123",
    table_name="companies",
    s3_pattern=[
        "s3://bucket/entities/companies_2024_01.parquet",
        "s3://bucket/entities/companies_2024_02.parquet"
    ]
)

response = manager.create_table(request)
```

#### From Wildcard Pattern

```python
request = TableCreateRequest(
    graph_id="graph123",
    table_name="relationships",
    s3_pattern="s3://bucket/relationships/*.parquet"
)

response = manager.create_table(request)
```

#### With Incremental Ingestion (v2)

```python
request = TableCreateRequest(
    graph_id="graph123",
    table_name="entities",
    s3_pattern=[
        "s3://bucket/file1.parquet",
        "s3://bucket/file2.parquet"
    ],
    file_id_map={
        "s3://bucket/file1.parquet": "file_123",
        "s3://bucket/file2.parquet": "file_456"
    }
)

response = manager.create_table(request)
```

**Automatic Features**:
- **Node tables** (have `identifier` column): Deduplicated by identifier
- **Relationship tables** (have `from`/`to` columns):
  - Deduplicated by (from, to)
  - Columns renamed: `from` → `src`, `to` → `dst`
  - Column order: src, dst, properties (required by LadybugDB)

### Querying Tables

#### Standard Query

```python
from robosystems.graph_api.models.tables import TableQueryRequest

request = TableQueryRequest(
    graph_id="graph123",
    sql="SELECT * FROM companies WHERE industry = ?",
    parameters=["Technology"]
)

response = manager.query_table(request)

for row in response.rows:
    print(row)
```

#### Streaming Query (Large Results)

```python
request = TableQueryRequest(
    graph_id="graph123",
    sql="SELECT * FROM large_table"
)

for chunk in manager.query_table_streaming(request, chunk_size=1000):
    if "error" in chunk:
        print(f"Error: {chunk['error']}")
        break

    print(f"Chunk {chunk['chunk_index']}: {chunk['row_count']} rows")

    for row in chunk["rows"]:
        process_row(row)

    if chunk["is_last_chunk"]:
        print(f"Total rows: {chunk['total_rows_sent']}")
        break
```

### Incremental Updates

#### Delete File Data

```python
result = manager.delete_file_data(
    graph_id="graph123",
    table_name="entities",
    file_id="file_123"
)

print(f"Deleted {result['rows_deleted']} rows")
```

#### Refresh Table

```python
result = manager.refresh_table("graph123", "entities")

print(f"Refreshed with {result['file_count']} files")
print(f"Total rows: {result['row_count']}")
```

### Listing Tables

```python
tables = manager.list_tables("graph123")

for table in tables:
    print(f"Table: {table.table_name}")
    print(f"  Rows: {table.row_count}")
    print(f"  Size: {table.size_bytes} bytes")
```

### Connection Pool Management

```python
from robosystems.graph_api.core.duckdb import get_duckdb_pool

pool = get_duckdb_pool()

stats = pool.get_stats()
print(f"Total connections: {stats['total_connections']}")
print(f"Databases on disk: {stats['total_databases_on_disk']}")
print(f"Connections created: {stats['stats']['connections_created']}")
print(f"Connections reused: {stats['stats']['connections_reused']}")

if pool.has_active_connections("graph123"):
    pool.close_database_connections("graph123")

pool.force_database_cleanup("graph123")
```

## Configuration

### Environment Variables

```bash
# DuckDB staging directory
DUCKDB_STAGING_DIR=/data/duckdb-staging

# Connection pooling
DUCKDB_MAX_CONNECTIONS_PER_DB=3
DUCKDB_CONNECTION_TTL_MINUTES=30

# Performance tuning
DUCKDB_MAX_THREADS=4
DUCKDB_MEMORY_LIMIT=2GB

# S3 access (for parquet reading)
AWS_S3_ACCESS_KEY_ID=AKIA...
AWS_S3_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1

# LocalStack (development)
AWS_ENDPOINT_URL=http://localstack:4566
```

### S3 Configuration

DuckDB connections are automatically configured with S3 access:

```python
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")
conn.execute("SET s3_access_key_id=?", [access_key])
conn.execute("SET s3_secret_access_key=?", [secret_key])
conn.execute("SET s3_region=?", [region])

if endpoint_url:
    endpoint = endpoint_url.replace("http://", "").replace("https://", "")
    conn.execute("SET s3_endpoint=?", [endpoint])
    conn.execute("SET s3_url_style='path'")
    conn.execute("SET s3_use_ssl=false")
```

### Performance Tuning

```python
conn.execute("SET threads TO 4")
conn.execute("SET memory_limit='2GB'")
```

**Guidelines**:
- **max_connections_per_db**: 3-5 for DuckDB (not as thread-safe as LadybugDB)
- **connection_ttl_minutes**: 30 minutes (balance reuse vs staleness)
- **threads**: Match available CPU cores
- **memory_limit**: Set based on available RAM and workload

## Data Flow Patterns

### Pattern 1: Bulk Import

```python
request = TableCreateRequest(
    graph_id="graph123",
    table_name="entities",
    s3_pattern="s3://bucket/entities/*.parquet"
)
manager.create_table(request)

result = conn.execute("SELECT * FROM entities").fetchall()

manager.delete_table("graph123", "entities")
```

### Pattern 2: Validation Before Import

```python
manager.create_table(request)

validation = conn.execute("""
    SELECT
        COUNT(*) as total,
        COUNT(DISTINCT identifier) as unique_ids,
        COUNT(*) FILTER (WHERE identifier IS NULL) as null_ids
    FROM entities
""").fetchone()

if validation[2] > 0:
    raise Exception("Invalid data: null identifiers found")

manager.delete_table("graph123", "entities")
```

### Pattern 3: Incremental Updates

```python
request = TableCreateRequest(
    graph_id="graph123",
    table_name="entities",
    s3_pattern=["s3://bucket/new_file.parquet"],
    file_id_map={"s3://bucket/new_file.parquet": "file_789"}
)
manager.create_table(request)

result = conn.execute("""
    SELECT * FROM entities
    WHERE file_id = 'file_789'
""").fetchall()

manager.delete_file_data("graph123", "entities", "file_123")
```

## Table Schema Conventions

### Node Tables

**Required**:
- `identifier` column (STRING) - Unique node identifier

**Example**:
```sql
CREATE TABLE entities AS
SELECT * EXCLUDE (rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY identifier ORDER BY identifier) AS rn
  FROM read_parquet(...)
)
WHERE rn = 1
```

### Relationship Tables

**Required**:
- `from` column (STRING) - Source node identifier
- `to` column (STRING) - Target node identifier

**Automatic Transformation**:
```sql
CREATE TABLE relationships AS
SELECT
  "from" as src,      -- Renamed for LadybugDB
  "to" as dst,        -- Renamed for LadybugDB
  * EXCLUDE ("from", "to", rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "from", "to" ORDER BY "from", "to") AS rn
  FROM read_parquet(...)
)
WHERE rn = 1
```

## Security

### SQL Injection Protection

**Table Name Validation**:
```python
def validate_table_name(table_name: str) -> None:
    if not re.match(r"^[a-zA-Z0-9_-]+$", table_name):
        raise HTTPException(400, "Invalid table name")
```

**Parameter Binding**:
```python
conn.execute("SELECT * FROM entities WHERE id = ?", [user_input])
```

**Safe Identifiers**:
```python
quoted_table = f'"{table_name}"'  # After validation
conn.execute(f"SELECT * FROM {quoted_table}")
```

### Path Validation

```python
from robosystems.utils.path_validation import get_duckdb_staging_path

db_path = get_duckdb_staging_path(graph_id, base_path)
```

Prevents path traversal attacks like `../../etc/passwd`.

## Performance Characteristics

### Table Creation

**Factors**:
- S3 network latency
- File size and count
- Deduplication overhead
- Available memory and CPU

**Typical Times**:
- Small files (< 1MB): 100-500ms
- Medium files (1-100MB): 500ms-5s
- Large files (> 100MB): 5s-60s

### Query Performance

**Factors**:
- Table size (row count)
- Query complexity
- Available indexes
- Memory buffer size

**Optimization**:
- Use LIMIT for exploration
- Create indexes for frequent filters
- Use streaming for large results
- Close connections after use

### Connection Pooling

**Metrics**:
```python
stats = pool.get_stats()
reuse_rate = stats['stats']['connections_reused'] / stats['stats']['connections_created']
```

**Target Reuse Rate**: > 80% for efficient pooling

## Error Handling

### Connection Errors

```python
from robosystems.graph_api.core.duckdb import get_duckdb_pool

pool = get_duckdb_pool()

try:
    with pool.get_connection(graph_id) as conn:
        result = conn.execute(query).fetchall()
except Exception as e:
    logger.error(f"DuckDB connection failed: {e}")
    raise
```

### Table Creation Errors

```python
try:
    response = manager.create_table(request)
except HTTPException as e:
    if e.status_code == 400:
        # Invalid request (bad table name, etc.)
        pass
    elif e.status_code == 500:
        # S3 access error, DuckDB error, etc.
        pass
```

### Query Errors

```python
try:
    response = manager.query_table(request)
except HTTPException as e:
    # SQL syntax error, table not found, etc.
    logger.error(f"Query failed: {e.detail}")
```

## Monitoring

### Connection Pool Stats

```python
stats = pool.get_stats()

metrics = {
    "total_connections": stats["total_connections"],
    "total_databases": stats["total_databases_on_disk"],
    "connections_created": stats["stats"]["connections_created"],
    "connections_reused": stats["stats"]["connections_reused"],
    "connections_closed": stats["stats"]["connections_closed"],
    "health_checks": stats["stats"]["health_checks"],
    "health_failures": stats["stats"]["health_failures"]
}
```

### Table Statistics

```python
tables = manager.list_tables(graph_id)

for table in tables:
    print(f"Table: {table.table_name}")
    print(f"  Rows: {table.row_count}")
    print(f"  Size: {table.size_bytes} bytes")
```

### Query Performance

```python
response = manager.query_table(request)

print(f"Execution time: {response.execution_time_ms}ms")
print(f"Rows returned: {response.row_count}")
```

## Testing

### Unit Tests

```python
import pytest
from robosystems.graph_api.core.duckdb import DuckDBTableManager

def test_table_creation():
    manager = DuckDBTableManager()

    request = TableCreateRequest(
        graph_id="test",
        table_name="test_table",
        s3_pattern=["s3://bucket/test.parquet"]
    )

    response = manager.create_table(request)

    assert response.status == "success"
    assert response.table_name == "test_table"
```

### Integration Tests

```python
@pytest.mark.integration
def test_s3_parquet_reading():
    manager = DuckDBTableManager()

    request = TableCreateRequest(
        graph_id="integration_test",
        table_name="entities",
        s3_pattern="s3://test-bucket/entities/*.parquet"
    )

    response = manager.create_table(request)
    tables = manager.list_tables("integration_test")

    assert any(t.table_name == "entities" for t in tables)

    manager.delete_table("integration_test", "entities")
```

## Best Practices

1. **Database Lifecycle**
   - Create staging database per graph_id
   - Clean up after ingestion completes
   - Use `force_database_cleanup()` when graph is deleted

2. **Table Management**
   - Validate table names to prevent injection
   - Use parameter binding for queries
   - Drop tables after ingestion

3. **Connection Pooling**
   - Reuse connections via pool
   - Always use context managers
   - Close connections when done
   - Monitor pool statistics

4. **Query Optimization**
   - Use LIMIT for exploration
   - Stream large result sets
   - Use chunking for bulk operations
   - Close cursors after use

5. **Error Handling**
   - Catch HTTPException for graceful failures
   - Validate data before ingestion
   - Log errors with context
   - Clean up on failure

6. **Security**
   - Always validate table names
   - Use parameter binding
   - Never trust user input
   - Validate S3 paths

## Limitations

### DuckDB Constraints

- **Thread Safety**: Limited concurrent writes per database
- **Connection Limits**: 3-5 connections recommended per database
- **Memory Usage**: Materialized tables consume RAM
- **Transaction Support**: Single-statement transactions only

### Staging Layer Constraints

- **Temporary Storage**: Tables are meant to be dropped after ingestion
- **No Persistence**: Not a permanent data store
- **S3 Dependency**: Requires S3 access for external tables
- **File Format**: Parquet only

## Related Documentation

- **[Core Services README](../README.md)** - Overview of all core services
- **[LadybugDB Service](../ladybug/README.md)** - Final ingestion destination
- **[Graph API README](/robosystems/graph_api/README.md)** - Complete API overview
- **[Ingestion Operations](/robosystems/operations/lbug/README.md)** - Data ingestion workflows

## Support

- **Source Code**: `/robosystems/graph_api/core/duckdb/`
- **Issues**: [robosystems/issues](https://github.com/RoboFinSystems/robosystems/issues)
- **API Docs**: http://localhost:8001/docs
