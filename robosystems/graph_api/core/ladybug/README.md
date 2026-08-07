# LadybugDB Core

LadybugDB is an embedded columnar graph database — a MIT-licensed engine running
in-process against a single `.lbug` file per graph. There is no server to
connect to and no network hop; the Graph API exists to put an HTTP surface in
front of files that only this instance can reach.

Four layers, each wrapping the one below:

| Layer | File | Role |
| --- | --- | --- |
| `LadybugService` | `service.py` | Orchestration: Cypher validation, query execution, streaming, health, cluster info |
| `LadybugDatabaseManager` | `manager.py` | Lifecycle: create, delete, schema application, blue-green swap, capacity |
| `LadybugConnectionPool` | `pool.py` | Per-database connection reuse, TTL, health checks, memory budget |
| `Engine` / `Repository` | `engine.py` | The driver: parameterized Cypher, DDL, transactions |

Supporting modules: `config.py` (memory budget resolution and per-database
overrides) and `materialization_lock.py` (the distributed write lock).

Application code should enter at `get_ladybug_service()`. Constructing an
`Engine` directly in a request handler bypasses pooling, health checks, and the
shared `Database` object — see the concurrency section below for why that
matters.

## Single writer, shared database object

LadybugDB allows one write transaction per database file at a time. The pool
enforces the invariant that makes this workable: **all connections for a given
database share one `lbug.Database` object.** Two `Database` objects over the
same file would not see each other's transactions, so the pool creates one per
database name and hands out `lbug.Connection` instances against it.

Consequences worth internalizing:

- Writes to one database serialize. They do not block reads of a *different*
  database, and they do not block queries on the same instance.
- Ingestion is sequential — one file materialized at a time per database.
- The buffer pool is sized once, when the `Database` object is created. Changing
  a memory budget therefore requires recreating the database object
  (`pool.recreate_database()`), not just setting a new value.

## Connection pool

`initialize_connection_pool(base_path, max_connections_per_db=3,
connection_ttl_minutes=30)` creates the process-global pool;
`get_connection_pool()` retrieves it. `LadybugDatabaseManager` calls
`initialize_connection_pool` from its own constructor, so wiring the service is
enough — there is no separate pool-init step.

```python
from robosystems.graph_api.core.ladybug import get_connection_pool

pool = get_connection_pool()
with pool.get_connection("kg123") as conn:
    result = conn.execute("MATCH (n) RETURN count(n)")
```

Behavior at the cap: requesting a connection beyond `max_connections_per_db`
does **not** block or raise. The pool closes its oldest connection and opens a
fresh one. So a hot database under high concurrency churns connections rather
than queueing, which shows up as connection-creation churn in the pool stats
before it shows up as latency. `pool.get_stats()` reports created / reused /
closed counts; a reuse rate well under 80% means the pool is thrashing and the
answer is more instances, not a larger cap.

Connections are health-checked before being handed out and closed once past
their TTL. Always use the context manager — an un-returned connection stays
counted against the cap until it expires.

## Memory

Each database gets a fixed buffer-pool budget in megabytes, resolved by
`get_database_memory_config(database_name)` in that order of precedence:

1. A per-database override set by `set_ladybug_memory_override` (used during
   materialization).
2. `memory_per_subgraph_mb` if the name contains an underscore — subgraphs are
   identified structurally (`kg123_dev`, `sec_historical`) and get a smaller
   pool, deliberately oversubscribed because they are rarely all hot at once.
3. `memory_per_db_mb` for a parent database.
4. The tier's total `max_memory_mb` for single-database instances.
5. `LBUG_MAX_MEMORY_MB` (default 2048) as the local-dev fallback.

Overrides are scoped to one database on purpose. A global boost leaks to every
subgraph on the instance and OOM-kills the container, taking every database with
it. Boost, do the work, restore, and recreate the database object each way:

```python
from robosystems.graph_api.core.ladybug.config import set_ladybug_memory_override

old = set_ladybug_memory_override(50000, graph_id="sec")
try:
    pool.recreate_database("sec")
    ...
finally:
    set_ladybug_memory_override(old, graph_id="sec")
    pool.recreate_database("sec")
```

Admission control (`../admission_control.py`) is the backstop: it rejects with
503 when free memory falls below 1 GB, measured as the tighter of host-available
and cgroup headroom. It deliberately does not gate on memory *percentage* — a
buffer pool is meant to fill, so percent-of-total conflates a fixed allocation
with the query working set.

Databases open read-only when `LBUG_ROLE=replica`, which avoids WAL recovery and
lock contention on snapshot-restored volumes. Masters always open read-write, so
that an early read request can't lock the file into read-only for the process
lifetime.

## Database lifecycle

```python
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.models.database import DatabaseCreateRequest, QueryRequest

service = get_ladybug_service()
manager = service.db_manager

manager.create_database(DatabaseCreateRequest(
    graph_id="kg123",
    schema_type="entity",       # entity | shared | custom
))

info = manager.get_database_info("kg123")   # size_bytes, is_healthy, timestamps
manager.list_databases()
manager.delete_database("kg123", force=True)
```

`create_database` and the other lifecycle methods take request models, not
keyword arguments. Schema application happens inside `create_database` —
`schema_type="custom"` uses `custom_schema_ddl` from the request. The schema
*declarations* live in [`robosystems/schemas/`](/robosystems/schemas/README.md);
this module is what installs them.

Close every connection for a database before deleting it. `delete_database`
does this, but code that unlinks files directly will find them held open.

## Query execution

```python
response = service.execute_query(QueryRequest(
    database="kg123",
    cypher="MATCH (n:Entity) WHERE n.entity_type = $t RETURN n",
    parameters={"t": "Company"},
))
# response.data, .columns, .row_count, .execution_time_ms

for chunk in service.execute_query_streaming(request, chunk_size=1000):
    ...
```

Always parameterize. `validate_cypher_query` screens statements before they
reach the engine, and string interpolation both defeats that and prevents plan
reuse. Lead the `MATCH` with a selective, indexed anchor — LadybugDB does not
reorder a scan into an index lookup for you.

## Blue-green materialization

Rebuilding a graph in place would take it offline for the duration. Instead, the
new graph is built as `{graph_id}-wip.lbug` while the live database keeps
serving, then promoted in one step.

`POST /databases/{graph_id}/swap` → `manager.swap_database(graph_id)`:

1. `CHECKPOINT` the WIP database to flush its WAL.
2. Close pooled connections for both the active and WIP databases.
3. Remove any leftover `-prev` from an earlier swap.
4. Rename active → `-prev` (with its `.wal`).
5. Rename WIP → active (with its `.wal`).
6. Delete `-prev`.

The `-prev` step is crash safety, not a rollback feature: if the swap fails
between steps 4 and 5, the handler restores active from `-prev`. Once step 6
runs, the old database is gone. The swap is one-way by design — the API exposes
no un-swap.

Swaps refuse to run on read-only nodes, and take the materialization lock.

## The materialization lock

`materialization_lock.py` holds a per-graph distributed lock in Valkey so two
materializations of the same graph cannot interleave.

- **Keyed on the base graph ID.** `kg123-wip` and `kg123` resolve to the same
  lock, so a materialization writing to WIP blocks a swap of the same graph.
  Different graphs never contend.
- **1-hour TTL**, as a safety net for a process that dies holding it.
- **5-second acquire timeout** — a second materialization fails fast rather than
  queueing behind one that may run for an hour.
- **Compare-and-delete release via Lua**, so a slow holder cannot delete a lock
  that has since expired and been re-acquired by someone else.
- **Token passthrough**: a caller that already holds the lock sends it in the
  `X-Materialization-Lock-Token` header, and the swap verifies against that
  token instead of acquiring its own.

## Errors

| Exception | Meaning | Response |
| --- | --- | --- |
| `ConnectionError` | Database unreachable, locked, or file missing | 503, retryable |
| `QueryError` | Cypher failed to parse or execute | 400, not retryable |

Both are exported from `robosystems.graph_api.core.ladybug`. Handle them
separately — retrying a syntax error is wasted work, and 400-ing a transient
lock conflict fails a request that would have succeeded.

## Related

- [`../README.md`](../README.md) — core services map and the ingest path
- [`../duckdb/README.md`](../duckdb/README.md) — where data lives before it is graph
- [`../../README.md`](../../README.md) — endpoints, tiers, deployment
