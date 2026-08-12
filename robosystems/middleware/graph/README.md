# Graph Middleware

The routing and multi-tenancy layer above the graph engine. Application code
asks this package for a repository bound to a `graph_id`; the package resolves
which LadybugDB instance holds that database, applies authorization and
admission control, and hands back a uniform interface. It does not itself talk
to the database — that is the core services layer under
[`../../graph_api/core/`](../../graph_api/core/README.md).

Two layers, cleanly split:

- **Middleware (this package)** — routing, multi-tenancy, allocation, admission
  control, authorization, FastAPI dependencies.
- **Core services** (`graph_api/core/ladybug/`, `graph_api/core/duckdb/`) —
  connection pooling, database lifecycle, query execution, staging.

## Multi-tenancy: `graph_id` is the tenancy boundary

Every operation is scoped to a `graph_id`, and nothing in this package will
give you a repository without one. Graph IDs fall into three categories
(`GraphCategory` in `types.py`):

- **User graphs** — `kg` + at least 16 hex characters, e.g. `kg1a2b3c…`. One
  customer database per instance.
- **Shared repositories** — platform-managed, named (`sec`, …), registered in
  `config/shared_repositories.py`. Read-only to customers.
- **System graphs** — platform internals.

**All tiers are dedicated.** `.github/configs/graph.yml` sets
`databases_per_instance: 1` for `ladybug-standard`, `ladybug-large`, and
`ladybug-xlarge` alike; the tiers differ in instance size, not in whether the
instance is shared. Read that file when the exact instance spec matters — it is
authoritative over any number written here.

### Subgraphs

A subgraph is an isolated database living on the *parent's* instance, addressed
as `{parent_graph_id}_{subgraph_name}`:

```python
from robosystems.middleware.graph.types import (
    is_subgraph_id, parse_graph_id, construct_subgraph_id,
)

is_subgraph_id("kg1234567890abcdef_dev")            # True
parse_graph_id("kg1234567890abcdef_dev")            # ("kg1234567890abcdef", "dev")
construct_subgraph_id("kg1234567890abcdef", "dev")  # "kg1234567890abcdef_dev"
```

Validation (`utils/subgraph.py`):

- Parent ID must match `^kg[a-f0-9]{16,}$`.
- Subgraph name must match `[a-zA-Z0-9]{1,20}` — alphanumeric only, no hyphens
  or underscores. The underscore is the separator, so a name containing one
  would make the ID ambiguous.
- Full ID must match `^(kg[a-f0-9]{16,})_([a-zA-Z0-9]{1,20})$`.

Constraints that bite:

- Subgraphs have **no DynamoDB registry entries of their own**.
  `LadybugAllocationManager.find_database_location()` resolves a subgraph to its
  parent's instance location and returns the subgraph ID with the parent's
  instance details.
- Single level only — a subgraph cannot have subgraphs.
- Subgraphs inherit the parent's tier, instance, credit pool, and permissions.
  Only the data is isolated.
- Shared repositories cannot have subgraphs.

## Getting a repository

```python
from robosystems.middleware.graph import GraphRouter

router = GraphRouter()
repo = await router.get_repository("kg1a2b3c", operation_type="write")
result = await repo.execute_query("MATCH (c:Entity) RETURN c")
```

`GraphRouter.get_repository()` is async. It delegates to
`graph_api.client.factory.GraphClientFactory`, or returns a direct-file
`Repository` when `LBUG_ACCESS_PATTERN=direct_file`. Writes always route to a
writer/master endpoint; reads may route to a replica when one exists for that
repository.

What comes back is a `UniversalRepository` (`repository.py`) — a wrapper over
either a direct `Repository` or an async Graph API `GraphClient`, so callers
never branch on which one they got. It offers `execute_query`,
`execute_query_streaming`, `execute_transaction`, `get_schema`, `health_check`,
`close`, sync variants (`execute_query_sync`, …), and both `async with` and
`with`. Build one through `create_universal_repository()` /
`get_universal_repository()` rather than instantiating the class.

In FastAPI routes, prefer the dependencies in `dependencies/` — they resolve the
graph and check access in one step:

```python
from robosystems.middleware.graph.dependencies import get_graph_repository_with_auth

@router.post("/v1/graphs/{graph_id}/query")
async def execute_query(repo=Depends(get_graph_repository_with_auth)):
    return await repo.execute_query(cypher)
```

Available dependencies include `get_graph_database`,
`get_graph_repository_with_auth`, `get_universal_repository_with_auth`,
`get_user_graph_repository`, `get_shared_repository`, `get_main_repository`,
`get_graph_repository_dependency`, and the path-param helpers `require_entity`,
`require_user_graph`, `require_graph_category` (each with an `optional_*`
variant).

## Authorization: one gauntlet, all transports

`statement_kernel.py` is the single policy path for graph statements. REST
(`/query/cypher`, `/query/sql`), MCP, and Operators all run through it rather
than re-implementing checks inline. It owns write detection (via
`security/cypher_analyzer.py`), the three-tier write policy — main graph
read-only, subgraph read+write, shared repository read-only — per-engine
validation, and the role-based write check.

It deliberately does **not** own circuit breaking, rate limiting, repository
acquisition, execution-strategy selection, or streaming: those are
transport-specific and position-sensitive in the hot path, and stay in the
callers. Adding a new query surface means calling the kernel, not copying it.

## Queueing and admission control

`query_queue.py` (`QueryQueueManager`, via `get_query_queue()`) accepts a query,
executes it immediately if there is capacity, and otherwise queues it by
priority with long polling for the result:

```python
from robosystems.middleware.graph.query_queue import get_query_queue

queue = get_query_queue()
query_id = await queue.submit_query(
    cypher="MATCH (n) RETURN count(n)",
    parameters=None,
    graph_id="kg1a2b3c",
    user_id="user_123",
    priority=8,
)
status = await queue.get_query_status(query_id)
result = await queue.get_query_result(query_id)
```

`admission_control.py` decides whether to accept at all. This one is
**percent-gated on every axis**: `AdmissionController.check_admission()` issues
`REJECT_MEMORY` as soon as `memory_percent` exceeds
`ADMISSION_MEMORY_THRESHOLD` (85%) and `REJECT_CPU` above
`ADMISSION_CPU_THRESHOLD` (90%). Queue capacity (80%) is softer — crossing
`ADMISSION_QUEUE_THRESHOLD` starts *probabilistic* load shedding (50/70/90%
rejection as the queue fills, scaled down for high-priority queries), not a hard
reject, and a combined pressure score over 0.7 sheds on top of that.
`LOAD_SHEDDING_ENABLED=false` short-circuits the whole gate to accept.

**Two admission controllers exist and they do not behave alike — don't
generalize from one to the other.**

|                | `middleware/graph/admission_control.py` (this one) | `graph_api/core/admission_control.py` |
| -------------- | ------------------------------------------------- | ------------------------------------- |
| Guards | the query queue, in the API process | every Graph API request, on the engine host |
| Memory gate | percent used > 85% → `REJECT_MEMORY` | absolute headroom < `MIN_AVAILABLE_MB` (1 GB); percent is *reported only* |
| CPU | > 90%, uniform | > 90% as wired, tightened 10 points for ingestion (the class's own 95 default is overridden by `get_admission_controller`) |
| Third axis | queue depth (probabilistic shedding) | per-database connection count |
| Config | `ADMISSION_*` / `admission/` SSM | `LBUG_ADMISSION_*` / `lbug_admission/` SSM |

The absolute-headroom design belongs to the *Graph API* controller and is
argued in `config/defaults.py`: a LadybugDB buffer pool is a fixed
pre-commitment that is *supposed* to fill, so percent-of-total conflates that
constant with the query working set that actually predicts exhaustion, and an
absolute figure does not move when the pool or the instance size changes. That
reasoning applies where the buffer pool lives; the queue gate here runs in the
API process, which has no buffer pool, so percent-used is a fair proxy there.
Both read `AdmissionDefaults.MEMORY_THRESHOLD = 85.0` — same number, opposite
role.

`execution_strategies.py` picks the execution path (direct query vs MCP,
load-aware). `streaming_wrapper.py` adapts streaming results for Graph API
clients. `instance_busy.py` maintains a DynamoDB busy counter so destructive
operations don't run against an instance mid-work.

## Allocation

`allocation_manager.py` (`LadybugAllocationManager`) assigns databases to
instances through a DynamoDB registry, atomically, and enables ASG scale-in
protection on instances that hold allocated databases.

```python
from robosystems.middleware.graph.allocation_manager import LadybugAllocationManager
from robosystems.config.graph_tier import GraphTier

manager = LadybugAllocationManager(environment="prod")
location = await manager.allocate_database(
    entity_id="kg1a2b3c",
    instance_tier=GraphTier.LADYBUG_STANDARD,
)
```

The registry is authoritative for *where* a database lives, and it can drift
from reality when instances cycle — a healing pass exists for exactly that
reason. Treat a "database not found" that contradicts a healthy instance as a
registry question first.

## Write-path limits

`ingestion_limits.py` blocks materialization on two independent categories:

1. **Aggregate storage GB** — the tier-scoped product cap, bounding instance
   disk cost. Enforced at the write path so a customer cannot accumulate data
   they are then unable to promote.
2. **Per-operation row caps** (`max_rows_per_copy`, `max_single_table_rows`) —
   OOM guardrails set per instance class, not marketing limits.

Row counts come from `GraphFile.duckdb_row_count` (recorded at upload), storage
from the Graph API's `get_database_info()` `size_bytes`, and the caps from
`GraphTierConfig.get_graph_limits(tier)`.

## Telemetry

`query_telemetry.py` classifies bad outcomes on the **shared** query surface
only — timeouts, capacity rejections, rate limits, policy denials, engine
disruptions — and records them through the security audit log, which publishes
the CloudWatch metrics the detective-control alarms watch. It also emits
structured start/end cost lines so operators can rank users by execution time
and reconstruct in-flight work during an incident. Every entry point gates on
the shared-repository check and no-ops on user graphs, which run on dedicated
instances. All of it is best-effort and never raises into the request path.

## Credits

Only AI operations (Anthropic Claude via AWS Bedrock, token-based) consume
credits. **Every graph query, import, ingestion, and backup is free**, included
in the subscription tier. Storage is limit-enforced, not metered into credits.
See [`../billing/README.md`](../billing/README.md).

## Types

`types.py` is the canonical source for routing enums and graph-ID helpers;
there is no separate `clusters.py`.

| Name                | Meaning                                              |
| ------------------- | ---------------------------------------------------- |
| `NodeType`          | Node role — writer, shared master, shared replica     |
| `RepositoryType`    | Entity graph vs shared repository                     |
| `GraphCategory`     | `USER`, `SHARED`, `SYSTEM`                            |
| `AccessPattern`     | `READ_ONLY`, `READ_WRITE`, `RESTRICTED`               |
| `ConnectionPattern` | Connection routing strategy                           |

It also exposes `is_subgraph_id`, `parse_graph_id`, `construct_subgraph_id`,
`GraphIdentity`, and `GraphTypeRegistry`. `GraphTier` (`LADYBUG_STANDARD`,
`LADYBUG_LARGE`, `LADYBUG_XLARGE`) comes from `config/graph_tier.py` and is
re-exported here.

`utils/` holds `MultiTenantUtils`, which aggregates static helpers from
`validation.py`, `database.py`, `identity.py`, and `subgraph.py`:

```python
from robosystems.middleware.graph.utils import MultiTenantUtils

graph_id = MultiTenantUtils.validate_graph_id("kg1a2b3c")
db_name = MultiTenantUtils.get_database_name(graph_id)
is_shared = MultiTenantUtils.is_shared_repository("sec")
```

## Configuration

Read every one of these through `robosystems.config.env`, never `os.getenv()`.

```bash
LBUG_DATABASE_PATH=./data/lbug-dbs      # database directory
LBUG_ACCESS_PATTERN=api_auto            # api_auto | api_writer | direct_file
LBUG_NODE_TYPE=writer
LBUG_CONNECTION_POOL_SIZE=10            # inert — surfaced but read by nothing; pool is capped at 3
LBUG_DATABASES_PER_INSTANCE=10
GRAPH_API_URL=                          # resolved dynamically in prod
```

Queue and admission settings are **SSM-tunable at runtime** (no redeploy) —
they resolve through `TuningConfig` with an env-var override:

| Setting                      | SSM key                     | Default |
| ---------------------------- | --------------------------- | ------- |
| `QUERY_QUEUE_MAX_SIZE`       | `queues/MAX_SIZE`           | 1000    |
| `QUERY_QUEUE_MAX_CONCURRENT` | `queues/MAX_CONCURRENT`     | 50      |
| `QUERY_QUEUE_MAX_PER_USER`   | `queues/MAX_PER_USER`       | 10      |
| `QUERY_QUEUE_TIMEOUT`        | `queues/TIMEOUT`            | 300 s   |
| `ADMISSION_MEMORY_THRESHOLD` | `admission/MEMORY_THRESHOLD`| 85.0 %  |
| `ADMISSION_CPU_THRESHOLD`    | `admission/CPU_THRESHOLD`   | 90.0 %  |
| `ADMISSION_QUEUE_THRESHOLD`  | `admission/QUEUE_THRESHOLD` | 80.0 %  |
| `LOAD_SHEDDING_ENABLED`      | `LOAD_SHEDDING_ENABLED`     | true    |

```bash
just ssm-get prod tuning/queues/MAX_CONCURRENT
just ssm-set prod tuning/queues/MAX_CONCURRENT 64
```

## Local debugging

```bash
just graph-health                          # Graph API health
just graph-info GRAPH_ID                   # database info
just graph-query GRAPH_ID "MATCH (n) RETURN count(n)"
just lbug-query GRAPH_ID "MATCH (n) RETURN count(n)"   # bypass the API
```

## Related

- [`../../graph_api/core/README.md`](../../graph_api/core/README.md) — core services
- [`../../graph_api/core/ladybug/README.md`](../../graph_api/core/ladybug/README.md) — engine, pool, manager, service
- [`../../graph_api/core/duckdb/README.md`](../../graph_api/core/duckdb/README.md) — staging layer
- [`../../graph_api/README.md`](../../graph_api/README.md) — Graph API service
- [`../auth/README.md`](../auth/README.md) — authentication and graph access
- [`../billing/README.md`](../billing/README.md) — credits and enforcement
- `.github/configs/graph.yml` — authoritative tier and instance specifications
