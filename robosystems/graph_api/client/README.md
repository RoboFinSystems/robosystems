# Graph API Client

The only supported way for application code — FastAPI routes, workers, Dagster
assets — to reach the [Graph API](../README.md). Callers name a `graph_id`; the
client resolves it to an instance, authenticates, retries what is worth
retrying, and gives up quickly on what is not.

| File | Contents |
| --- | --- |
| `factory.py` | `GraphClientFactory` and the `get_graph_client*` helpers; routing, discovery, caching, shared-master circuit breaker |
| `client.py` | `GraphClient` — the async HTTP client and every endpoint method |
| `base.py` | `BaseGraphClient` — URL building, retry classification, per-client circuit breaker |
| `config.py` | `GraphClientConfig` — timeouts, retries, pool sizes, circuit-breaker thresholds |
| `exceptions.py` | The error hierarchy |

`GraphClient` is the single client class. Everything is `async` — there is no
synchronous client.

## Getting a client

```python
from robosystems.graph_api.client import get_graph_client

async with await get_graph_client("kg1a2b3c4d5", operation_type="write") as client:
    await client.query(
        cypher="MATCH (n:Entity) RETURN n LIMIT 10",
        graph_id="kg1a2b3c4d5",
    )
```

`get_graph_client(graph_id, operation_type="read", environment=None, tier=None)`
delegates to `GraphClientFactory.create_client`. Two other entry points exist:

- `get_graph_client_for_instance(instance_ip, api_key=None)` — bypasses routing
  entirely and connects to one instance. Used during allocation, when the graph
  isn't in the registry yet.
- `get_graph_client_sync(...)` — for genuinely synchronous callers. It wraps
  `asyncio.run`, so it **raises `RuntimeError` if an event loop is already
  running**. The client it returns is still async: `async with`, `await`. It is
  not a synchronous context manager.

## Routing

`create_client` branches on what kind of graph it was handed.

**Shared repositories** (`sec`, and subgraphs whose parent is shared):

- Development collapses to the single local instance at `GRAPH_API_URL`.
- Writes always go to the shared master. Replicas are read-only copies synced
  from it, so a write anywhere else would be lost on the next sync.
- Reads prefer the replica ALB. Falling through to the master requires
  `SHARED_MASTER_READS_ENABLED`, so a deployment can keep read load off the
  writer entirely.

**User graphs** (`kg…` and their subgraphs) are looked up per graph — every
operation, read or write, goes to the one instance that holds the files.

## Instance discovery

1. Check Valkey for a cached location — 60 s TTL, skipped entirely when
   `GRAPH_REDIS_CACHE_ENABLED=false`.
2. Query the DynamoDB graph registry for the instance hosting the graph.
3. Confirm the instance is healthy in the instance registry.
4. Build a `GraphClient` against `http://{private_ip}:8001` and cache the
   location.

A short TTL is the point: instances cycle on ASG refresh, and a stale route
costs more than an extra DynamoDB read. If discovery finds nothing healthy, the
factory raises `ServiceUnavailableError`.

Factory state — connection pools, the Valkey client, the shared-master circuit
breaker — is class-level and shared process-wide.

## Retries and circuit breaking

Two independent mechanisms, at two different layers.

**Factory-level** (`with_retry`, used for shared-master URL resolution) retries
only connection-level failures: `httpx.TimeoutException`, `httpx.ConnectError`,
`ServiceUnavailableError`. Anything else propagates on the first raise. Backoff
is exponential with jitter spread over 0.5×–1.5× of the computed delay, so a
fleet of callers doesn't retry in lockstep. `GRAPH_RETRY_LOGIC_ENABLED=false`
disables it.

**Client-level** (`BaseGraphClient._should_retry`) classifies by exception type:

| Class | Retried | Typical cause |
| --- | --- | --- |
| `GraphTransientError` | yes | Network blip, 503 from admission control |
| `GraphTimeoutError` | yes | Subclass of `GraphTransientError` |
| `GraphServerError` | yes | 5xx |
| `GraphSyntaxError` | **never** | Bad Cypher — retrying cannot help |
| `GraphClientError` | no | 4xx other than syntax |

Each client keeps its own circuit breaker: `circuit_breaker_threshold`
consecutive failures (default 5) opens it, subsequent calls raise
`GraphTransientError` immediately, and after `circuit_breaker_timeout` (default
60 s) the next call is allowed through and a success closes it.

`ServiceUnavailableError` and `ConfigurationError` are defined in `factory.py`,
not `exceptions.py` — import them from `robosystems.graph_api.client.factory`.

## Client methods

`GraphClient` covers the whole Graph API surface. The commonly used subset:

```python
await client.query(cypher, graph_id, parameters=None, streaming=False)
await client.execute_query(...)          # query + result unwrapping
await client.execute_single(...)         # first row only
await client.execute_ddl(ddl, graph_id)

await client.create_database(graph_id, schema_type="entity")
await client.delete_database(graph_id)
await client.database_exists(graph_id)
await client.install_schema(...)

await client.create_table(...)           # DuckDB staging
await client.query_table(...)
await client.materialize_table(...)
await client.execute_write(...)
await client.delete_table(graph_id, table_name)

await client.create_backup(...)          # returns a task id
await client.backup_with_sse(...)        # streams progress
await client.restore_backup(...)
await client.swap_database(...)          # blue-green promotion

await client.health_check()
await client.get_storage_breakdown(graph_id)
await client.boost_memory(graph_id)      # and restore_memory / release_memory
```

Note that `client.ingest()` posts to `/databases/{graph_id}/ingest`, which the
service does not expose. Use the staging-table path — `create_table` →
`query_table` → `materialize_table` — instead.

### Streaming

```python
result = await client.query(cypher, graph_id, streaming=True)
async for chunk in result:
    process(chunk)
```

With `streaming=True` the return value is an async generator of NDJSON chunks
rather than a dict — the instance does the chunking instead of materializing the
whole result set in memory. Use it for anything that could return more than a
few thousand rows.

## Configuration

`GraphClientConfig` defaults, overridable per-field or from the environment via
`GraphClientConfig.from_env()` with the `GRAPH_CLIENT_` prefix
(`GRAPH_CLIENT_TIMEOUT`, `GRAPH_CLIENT_MAX_RETRIES`, and so on):

| Setting | Default |
| --- | --- |
| `timeout` | 30 s |
| `max_retries` | 3 |
| `retry_delay` / `retry_backoff` | 1.0 s / 2.0× |
| `max_connections` | 100 |
| `max_keepalive_connections` | 20 |
| `keepalive_expiry` | 5.0 s |
| `circuit_breaker_threshold` | 5 failures |
| `circuit_breaker_timeout` | 60 s |

The factory's own httpx pools use separate connect (5 s) and read (30 s)
timeouts from `robosystems/config/constants.py`.

Feature flags read through `robosystems.config.env`:

```bash
GRAPH_API_URL=http://localhost:8001      # dev / fallback endpoint
GRAPH_API_KEY=                           # cluster API key
GRAPH_RETRY_LOGIC_ENABLED=true
GRAPH_CIRCUIT_BREAKERS_ENABLED=true
GRAPH_HEALTH_CHECKS_ENABLED=true
GRAPH_REDIS_CACHE_ENABLED=true           # route caching; name kept for compatibility
SHARED_MASTER_READS_ENABLED=             # allow shared reads to fall back to the master
```

Registry table names come from `INSTANCE_REGISTRY_TABLE`,
`GRAPH_REGISTRY_TABLE`, and `VOLUME_REGISTRY_TABLE`, defaulting to
`robosystems-graph-{env}-*`.

## Error handling

```python
from robosystems.graph_api.client import GraphSyntaxError, GraphTimeoutError
from robosystems.graph_api.client.factory import ServiceUnavailableError

try:
    async with await get_graph_client("kg1a2b3c4d5", "write") as client:
        result = await client.query(cypher, graph_id="kg1a2b3c4d5")
except GraphSyntaxError:
    ...   # never retried; fix the query
except GraphTimeoutError:
    ...   # already retried to exhaustion
except ServiceUnavailableError:
    ...   # no healthy instance, or the circuit breaker is open
```

The hierarchy: `GraphAPIError` → `GraphTransientError` (→ `GraphTimeoutError`),
`GraphClientError` (→ `GraphSyntaxError`), `GraphServerError`.

## Troubleshooting

**`ServiceUnavailableError`.** No healthy instance was found. Check the graph
registry, then the instance registry, then whether the EC2 instances are
actually running — see [Troubleshooting](../README.md#troubleshooting) in the
service README for the DynamoDB queries.

**Circuit breaker open.** Repeated failures against one endpoint. It self-closes
after 60 s; if it reopens immediately, the target is genuinely unhealthy.

**Authentication failures.** `GRAPH_API_KEY` must match the key the target
instance loaded from Secrets Manager for its environment.

**Timeouts on large queries.** Raise the per-client timeout, or switch to
`streaming=True` so results arrive incrementally rather than after a full
materialization.

```python
import logging
logging.getLogger("robosystems.graph_api").setLevel(logging.DEBUG)
```

Debug logging shows routing decisions, discovery results, cache hits and misses,
retry attempts, and circuit-breaker transitions.
