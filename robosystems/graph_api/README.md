# Graph API

A FastAPI service (port 8001) that owns the graph data plane. It runs on the same
host as the databases it serves: LadybugDB holds the graph, DuckDB holds the
staging tables data lands in before it becomes graph, and LanceDB holds vector
indexes. Everything is embedded and local — the HTTP layer exists so the rest of
the platform (API, workers, Dagster) can reach a specific instance's files
without sharing a filesystem with it.

The application never talks to this service directly by URL. It goes through
[`client/`](client/README.md), which resolves a `graph_id` to an instance and
handles retries, circuit breaking, and failover.

| Directory | Contents |
| --- | --- |
| [`core/`](core/README.md) | Engines, connection pools, staging, admission control, tasks |
| [`core/ladybug/`](core/ladybug/README.md) | Graph engine, pooling, database lifecycle, blue-green swap |
| [`core/duckdb/`](core/duckdb/README.md) | Parquet staging tables and the path into the graph |
| [`client/`](client/README.md) | Async client, routing factory, circuit breaker |
| `routers/` | HTTP surface (see endpoints below) |
| `models/` | Pydantic request/response models |
| `middleware/` | API-key auth, request size limits |
| `interfaces/` | Engine protocol shared with `middleware/graph` |

## Authentication

Every request outside `/health` carries the cluster API key:

```http
X-Graph-API-Key: <key>
```

`Authorization: Bearer <key>` is accepted as an alternative. Keys live in AWS
Secrets Manager, are rotated on a schedule, and are compared in constant time.
The service listens only inside the VPC — there is no public route to port 8001.

## Endpoints

### Cluster

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/health` | Load-balancer probe. Returns 503 while a replica is warming or a version migration is running |
| GET | `/info` | Node identity, type, database list, capacity |
| GET | `/metrics` | System, database, query, and ingestion metrics |

### Databases

| Method | Path | Body / returns |
| --- | --- | --- |
| GET | `/databases` | `DatabaseListResponse` |
| POST | `/databases` | `DatabaseCreateRequest` → `DatabaseCreateResponse` |
| GET | `/databases/{graph_id}` | `DatabaseInfo` (size, health, timestamps) |
| DELETE | `/databases/{graph_id}` | Closes pooled connections, then removes files |
| POST | `/databases/{graph_id}/query` | `QueryRequest` → `QueryResponse`, NDJSON, or SSE |
| POST | `/databases/{graph_id}/schema` | `SchemaInstallRequest` → `SchemaInstallResponse` |
| GET | `/databases/{graph_id}/schema` | Installed node/relationship tables |
| GET | `/databases/{graph_id}/metrics` | Per-database query and size metrics |
| GET | `/databases/{graph_id}/storage` | Itemized disk use across LadybugDB, LanceDB, and DuckDB staging |
| POST | `/databases/{graph_id}/swap` | Promote `{graph_id}-wip` to active |
| POST | `/databases/{graph_id}/backup` | Starts a task, returns `task_id` |
| POST | `/databases/{graph_id}/restore` | Starts a task, returns `task_id` |
| POST | `/databases/{graph_id}/backup-download` | Stream a stored backup |

### Staging tables

All under `/databases/{graph_id}/tables`. The whole group returns **501** on
read-only replicas — staging is a writer-only concern.

| Method | Path | Notes |
| --- | --- | --- |
| POST | `` | Create a staging table from Parquet in S3 (`TableCreateRequest`) |
| GET | `` | `list[TableInfo]` |
| POST | `/{table_name}/insert` | Append more Parquet into an existing table |
| POST | `/query` | Read-only SQL. Table name goes in the SQL, not the path |
| POST | `/execute` | Write/DDL SQL — internal materialization paths only |
| POST | `/{table_name}/materialize` | Copy staged rows into the graph |
| POST | `/{subgraph_id}/fork-from/{parent_graph_id}` | Seed a subgraph from the parent's staging |
| DELETE | `/{table_name}` | Drop the staging table |
| DELETE | `/{table_name}/files/{file_id}` | Drop one file's rows |

### Vector and semantic memory

| Method | Path |
| --- | --- |
| GET / DELETE | `/databases/{graph_id}/tables/{table_name}/vector` |
| POST | `/databases/{graph_id}/tables/{table_name}/vector/{build,search,export}` |
| POST | `/databases/{graph_id}/semantic-memory/{rows,search,list}` |
| GET / PATCH / DELETE | `/databases/{graph_id}/semantic-memory/rows/{memory_id}` |

### Memory, tasks, migration

| Method | Path | Notes |
| --- | --- | --- |
| POST | `/databases/{graph_id}/memory/{boost,restore,release}` | Temporarily raise or drop a database's memory budget |
| GET | `/databases/{graph_id}/memory/status` | Current budget and whether a boost is active |
| GET | `/tasks` · `/tasks/stats` | List and aggregate background tasks |
| GET | `/tasks/{task_id}/status` | Poll a task |
| GET | `/tasks/{task_id}/monitor` | SSE progress stream |
| POST | `/migration/{export,import,cleanup}` · GET `/migration/status` | Engine-version migration of on-disk databases |

## Examples

```bash
# Create a database
curl -X POST http://localhost:8001/databases \
  -H "X-Graph-API-Key: $GRAPH_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"graph_id": "kg1a2b3c4d5", "schema_type": "entity"}'

# Query it. `database` in the body must match the path segment.
curl -X POST http://localhost:8001/databases/kg1a2b3c4d5/query \
  -H "X-Graph-API-Key: $GRAPH_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"database": "kg1a2b3c4d5", "cypher": "MATCH (n:Entity) RETURN n LIMIT 10"}'

# Stage Parquet from S3
curl -X POST http://localhost:8001/databases/kg1a2b3c4d5/tables \
  -H "X-Graph-API-Key: $GRAPH_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"graph_id": "kg1a2b3c4d5", "table_name": "Entity",
       "s3_pattern": "s3://bucket/entities/*.parquet"}'

# Validate it before it becomes graph
curl -X POST http://localhost:8001/databases/kg1a2b3c4d5/tables/query \
  -H "X-Graph-API-Key: $GRAPH_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"graph_id": "kg1a2b3c4d5",
       "sql": "SELECT count(*) FROM Entity WHERE identifier IS NULL"}'

# Materialize into the graph
curl -X POST http://localhost:8001/databases/kg1a2b3c4d5/tables/Entity/materialize \
  -H "X-Graph-API-Key: $GRAPH_API_KEY" \
  -H "Content-Type: application/json" -d '{}'
```

`TableMaterializationRequest` sets `extra = "forbid"` — an unrecognized key in
that body is a 422, not a silently ignored field. The accepted keys are
`file_ids`, `source_graph_id`, `batch_num`, `num_batches`,
`materialize_embeddings`, and `incremental`.

## Operational limits

These are properties of the engine and the instance, not tunables to be
optimized away.

- **One writer per database.** LadybugDB permits a single write transaction per
  database file. Materializations additionally take a per-graph distributed lock
  so two of them can't interleave — see [`core/ladybug/`](core/ladybug/README.md).
- **Sequential ingestion.** Files are materialized one at a time per database.
- **Connection pool: 3 per database.** Hardcoded, not tier-resolved: the DuckDB
  pool is initialized at 3 in `app.py`, and the LadybugDB pool takes the
  `max_connections_per_db=3` default on `LadybugDatabaseManager`, which
  `LadybugService` never overrides. Asking for a fourth does not queue — the
  pool closes the oldest connection to make room. Concurrency comes from more
  instances, not a bigger pool.
- **Admission control rejects before it crashes.** Requests are refused with 503
  when free memory drops below 1 GB (measured as the tighter of host-available
  and cgroup headroom) or CPU exceeds 90% — 80% for ingestion, which gets a
  stricter limit. A percentage-of-total memory gate would misfire, because a
  buffer pool is *supposed* to fill.
- **No cross-database queries.** Each query is scoped to one database file.
- **Memory is a fixed per-database budget** derived from the tier config, not a
  shared heap. Exceeding it is an OOM kill of the container, which is why
  materialization boosts and then restores the budget rather than running high
  permanently.

## Configuration

Read through `robosystems/config/env.py`; never call `os.getenv` directly.

```bash
LBUG_NODE_TYPE=writer                 # writer | shared_master | shared_replica
LBUG_ROLE=master                      # replica disables staging + forces read-only opens
CLUSTER_TIER=ladybug-standard         # selects the tier block in .github/configs/graph.yml
LBUG_DATABASE_PATH=/data/lbug-dbs     # LadybugDB files
DUCKDB_STAGING_PATH=./data/staging    # DuckDB staging files
LBUG_ACCESS_PATTERN=api_auto          # api_auto | api_writer | direct_file
GRAPH_API_KEY=                        # cluster API key
GRAPH_QUERY_TIMEOUT=30
```

Memory, thread counts, chunk sizes and subgraph caps are resolved per tier from
[`.github/configs/graph.yml`](/.github/configs/graph.yml) using `CLUSTER_TIER`;
the `LBUG_MAX_MEMORY_MB` / `LBUG_CHUNK_SIZE` env vars are only the local-dev
fallback when no tier block matches.

Connection-pool size is **not** among them. `graph.yml` carries a
`connection_pool_size` per tier and `env.get_lbug_tier_config()` surfaces it
(as does `LBUG_CONNECTION_POOL_SIZE`), but nothing reads either — both pools are
capped at a hardcoded 3 per database (see Operational limits above).

Client-side behavior (retries, circuit breaker, timeouts) is configured
separately under the `GRAPH_CLIENT_` prefix — see [`client/`](client/README.md).

### Tiers

Every tier is dedicated: one customer database per instance, with subgraphs
sharing that instance. `.github/configs/graph.yml` is authoritative.

| Tier | Instance | RAM | Parent buffer pool | Subgraphs |
| --- | --- | --- | --- | --- |
| `ladybug-standard` | m7g.medium | 4 GB | 1 GB | 3 |
| `ladybug-large` | m7g.large | 8 GB | 2 GB | 10 |
| `ladybug-xlarge` | r7g.xlarge | 32 GB | 8 GB | 25 |
| `ladybug-shared` | r7g.2xlarge | 64 GB | 10 GB | 10 (platform-managed) |

Shared replicas are a separate read-only fleet: they pull `.lbug` and `.duckdb`
files from S3 on boot, serve queries locally, and return 503 from `/health`
until warm. They refresh through a rolling ASG instance refresh after new
database files are published.

## Deployment

CloudFormation, deployed through GitHub Actions — never by hand.

| Template | Workflow | Creates |
| --- | --- | --- |
| `cloudformation/graph-infra.yaml` | `deploy-graph-infra.yml` | DynamoDB registries, Secrets Manager entries, SNS topics |
| `cloudformation/graph-volumes.yaml` | `deploy-graph-volumes.yml` | EBS lifecycle and auto-expansion Lambdas |
| `cloudformation/graph-ladybug.yaml` | `deploy-graph-ladybug.yml` | Writer ASGs, one stack per tier (matrix) |
| `cloudformation/graph-ladybug-replicas.yaml` | `deploy-graph-replicas.yml` | Shared read-replica fleet + ALB |

`deploy-graph.yml` orchestrates all four. `graph-asg-refresh.yml` cycles
instances when only the S3 userdata changed — CloudFormation does not track
external S3 content, so a template-free userdata change needs an explicit
refresh.

Three DynamoDB registries back instance discovery:

| Table | Keyed by | Answers |
| --- | --- | --- |
| `robosystems-graph-{env}-instance-registry` | `instance_id` | Which instances exist, their tier, IP, health, capacity |
| `robosystems-graph-{env}-graph-registry` | `graph_id` | Which instance hosts this graph |
| `robosystems-graph-{env}-volume-registry` | `volume_id` | Which EBS volume holds which database |

On boot, an instance registers itself, invokes the volume-manager Lambda to
attach its EBS volume, starts the container, and signals CloudFormation.

## Local development

```bash
just start                 # full stack on the robosystems profile
just graph-health          # check the service is up
just graph-info GRAPH_ID   # database info
just graph-query GRAPH_ID "MATCH (n) RETURN count(n)"
just lbug-query GRAPH_ID "MATCH (n) RETURN count(n)"   # bypass the API
```

To run only the service against a local directory:

```bash
uv run python -m robosystems.graph_api \
  --base-path ./data/lbug-dbs \
  --node-type writer \
  --port 8001
```

`--help` lists the rest: `--max-databases`, `--repository-type`, `--read-only`,
`--log-level`, `--workers`. There is no client CLI subcommand — use `just
graph-*` or `curl`.

```bash
uv run pytest tests/graph_api/ -v
uv run pytest tests/graph_api/ -m integration   # needs a live instance
```

## Troubleshooting

**503 from admission control.** The instance is near its memory or CPU limit.
Check `GET /metrics` and `GET /databases/{graph_id}/storage`. Slow the caller;
raising thresholds trades a rejected request for an OOM kill that takes every
database on the instance with it.

**503 with `"status": "rebuilding"`.** A blue-green materialization is in flight
for that graph. Queries resume when the swap completes.

**501 on a `/tables` route.** You reached a replica. Staging is writer-only.

**Query returns an empty result with no error.** Usually a LadybugDB aggregation
quirk rather than an empty database — check that a `WHERE` isn't filtering on a
column produced by an aggregating `WITH`.

**No healthy instance found.** Check the graph registry, then the instance
registry:

```bash
aws dynamodb get-item \
  --table-name robosystems-graph-prod-graph-registry \
  --key '{"graph_id":{"S":"kg1a2b3c4d5"}}'

aws dynamodb scan \
  --table-name robosystems-graph-prod-instance-registry \
  --filter-expression "cluster_tier = :tier" \
  --expression-attribute-values '{":tier":{"S":"ladybug-standard"}}'
```

Instances log to the unified CloudWatch group `/robosystems/{env}/graph-api`,
and publish EC2 alarms under the `RoboSystems/Graph/{env}` namespace.
