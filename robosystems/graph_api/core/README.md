# Graph API Core

The service layer under [`../routers/`](../routers). Routers do HTTP; everything
here does the actual work against the three embedded stores that live on the
instance. Nothing in `core/` opens a socket to another host.

| Module | Responsibility |
| --- | --- |
| [`ladybug/`](ladybug/README.md) | The graph itself: engine, connection pool, database lifecycle, schema application, blue-green swap |
| [`duckdb/`](duckdb/README.md) | Parquet staging tables and the copy path into the graph |
| `lance/` | LanceDB vector storage — `memory_store.py` (per-graph semantic memory, row-level CRUD) and `manager.py` (batch IVF-PQ index builder, currently dormant) |
| `admission_control.py` | `LadybugAdmissionController` — refuses requests before the instance runs out of memory |
| `memory_manager.py` | Temporarily raises and restores per-database memory budgets around heavy operations |
| `storage_breakdown.py` | The single source of truth for a graph's disk footprint across all three stores |
| `backup_service.py` | On-instance backup: CHECKPOINT, compress, multipart upload to S3 |
| `migration_service.py` | `EXPORT DATABASE` / `IMPORT DATABASE` across a LadybugDB version upgrade |
| `metrics_collector.py` | `LadybugMetricsCollector` — query, database, system, and ingestion metrics |
| `task_manager.py` | `GenericTaskManager` over Valkey; instances: `backup_task_manager`, `restore_task_manager`, `migration_task_manager` |
| `task_sse.py` | Formats task state into Server-Sent Events for `/tasks/{id}/monitor` |
| `utils.py` | Name and parameter validation shared across routers |

## Startup

`app.py` initializes the two pools and the graph service once, in its lifespan
handler. Request handlers resolve them through the getters — they never
construct an `Engine`, a pool, or a manager themselves.

```python
from robosystems.graph_api.core.duckdb import initialize_duckdb_pool
from robosystems.graph_api.core.ladybug import init_ladybug_service
from robosystems.middleware.graph.types import NodeType, RepositoryType

init_ladybug_service(
    base_path="/data/lbug-dbs",
    max_databases=200,
    read_only=False,
    node_type=NodeType.WRITER,
    repository_type=RepositoryType.ENTITY,
)

initialize_duckdb_pool(
    base_path="./data/staging",
    max_connections_per_db=3,
    connection_ttl_minutes=30,
)
```

`init_ladybug_service` constructs `LadybugDatabaseManager`, which in turn calls
`initialize_connection_pool` — there is no separate step for the graph pool.
Both `init_ladybug_service` and `initialize_connection_pool` are idempotent
singletons; calling them twice logs a warning and returns the existing instance.

After startup, use `get_ladybug_service()`, `get_connection_pool()`,
`get_duckdb_pool()`, and `get_admission_controller()`.

## Request path

A query is admitted before it is executed, and measured after:

1. Router receives the request; the auth middleware validates `X-Graph-API-Key`.
2. If the graph is mid-rebuild, the router returns 503 with `retry_after`.
3. `LadybugAdmissionController.check_admission(graph_id, "query")` returns an
   `AdmissionDecision`. Anything but `ACCEPT` is a 503 with a reason.
4. `LadybugService.execute_query` validates the Cypher, then borrows a
   connection from `LadybugConnectionPool`.
5. `Engine` executes against the embedded database file.
6. `LadybugMetricsCollector.record_query(database, duration_ms, success)`.

## Ingest path

Data becomes graph in two distinct stages, and the seam between them is the
point of the design: staging is where data can be inspected and fixed while it
is still cheap to fix.

1. Parquet is uploaded to S3 by the platform API.
2. `POST /databases/{graph_id}/tables` builds a DuckDB table over those files,
   deduplicating as it goes.
3. `POST /databases/{graph_id}/tables/query` runs read-only SQL against the
   staged rows — count nulls, check cardinality, find bad references.
4. `POST /databases/{graph_id}/tables/{table}/materialize` copies the staged
   rows into LadybugDB.
5. The staging table is kept. Incremental loads anti-join against the graph, and
   a rebuild replays from staging rather than from S3.

The full mechanics — why deduplication happens in DuckDB rather than at COPY
time, and how `from`/`to` become `src`/`dst` — are in
[`duckdb/README.md`](duckdb/README.md).

## Multi-tenancy

Isolation is filesystem-level, not row-level. Each graph is one `.lbug` file
under `LBUG_DATABASE_PATH` and one `.duckdb` file under `DUCKDB_STAGING_PATH`,
with its own pool entry and its own memory budget. Subgraphs are separate
database files on the same instance, named `{parent}_{name}` — the underscore is
load-bearing, since `get_database_memory_config` uses it to hand subgraphs the
smaller `memory_per_subgraph_mb` budget. Path construction goes through
`validate_database_path` and `get_duckdb_staging_path` so a graph ID can never
escape its base directory.

## Conventions

- Acquire connections through a pool context manager, never by constructing an
  `Engine`. Direct engines bypass health checks, TTL, and the pool cap.
- Close every connection for a database before deleting it; the manager does
  this, and callers that skip it leave a file that cannot be unlinked.
- Validate staged data before materializing, not after. Once rows are in the
  graph, fixing them costs a rebuild.
- Catch `ConnectionError` and `QueryError` separately — the first is worth a
  retry or a 503, the second is a 400.
