# DuckDB Staging

DuckDB is the waiting room between S3 and the graph. Parquet files land in S3,
are read into a per-graph DuckDB file as materialized tables, get inspected and
deduplicated there, and are then copied into LadybugDB. Staging exists because
data is cheap to fix while it is still a table and expensive to fix once it is a
graph.

```
s3://bucket/{graph}/entities/*.parquet
        │  read_parquet(), deduplicate, rename
        ▼
{DUCKDB_STAGING_PATH}/{graph_id}.duckdb      ← one file per graph, persists
        │  COPY
        ▼
{LBUG_DATABASE_PATH}/{graph_id}.lbug
```

Staging tables are kept after materialization, not dropped. Incremental loads
anti-join against them, and a graph rebuild replays from staging rather than
re-reading thousands of S3 objects.

Two modules: `pool.py` (`DuckDBConnectionPool`, connection lifecycle and
security posture) and `manager.py` (`DuckDBTableManager`, table operations).

## Creating tables

```python
from robosystems.graph_api.core.duckdb import DuckDBTableManager
from robosystems.graph_api.models.tables import TableCreateRequest

manager = DuckDBTableManager()          # takes no arguments

manager.create_table(TableCreateRequest(
    graph_id="kg123",
    table_name="Entity",
    s3_pattern="s3://bucket/entities/*.parquet",   # glob or list[str]
))
```

`TableCreateRequest` sets `extra = "forbid"`; unknown keys are a validation
error, not a no-op. The fields:

| Field | Default | Purpose |
| --- | --- | --- |
| `s3_pattern` | required | Glob string or explicit list. Every entry must start with `s3://` |
| `file_id_map` | `None` | `{s3_key: file_id}` — stamps provenance so single files can later be removed |
| `null_columns` | `None` | Columns kept in the schema but written as NULL, e.g. `["embedding"]` to skip vectors |
| `deduplicate` | `True` | Dedupe on insert via `NOT EXISTS` on the dedup key |
| `timeout_seconds` | `1800` | 60 s to 4 h |

`insert_into_table(request)` appends more Parquet into an existing table using
the same request model.

## Table conventions

The shape of the Parquet decides how the table is built. `create_table` probes
the columns and picks one of three paths.

**Node tables** — have an `identifier` column. Deduplicated on `identifier`:

```sql
CREATE OR REPLACE TABLE "Entity" AS
SELECT "identifier", FIRST("name") AS "name", FIRST("cik") AS "cik"
FROM read_parquet(?, union_by_name=true, hive_partitioning=false)
GROUP BY "identifier"
```

**Relationship tables** — have `from` and `to` columns. Deduplicated on the
pair, and renamed on the way through, because LadybugDB requires the source and
target columns to be named `src` and `dst` and to come first:

```sql
CREATE OR REPLACE TABLE "ENTITY_HAS_EVENT" AS
SELECT "from" AS src, "to" AS dst, FIRST("weight") AS "weight"
FROM read_parquet(?, union_by_name=true, hive_partitioning=false)
GROUP BY "from", "to"
```

**Anything else** — read straight through with no deduplication.

Two details are load-bearing:

- **Deduplication uses `GROUP BY` + `FIRST()`, not `ROW_NUMBER()`.** Hash
  aggregation spills to disk under DuckDB's external aggregation; a window
  function over a large table does not, and OOMs.
- **`union_by_name=true`** tolerates files whose column order or column set
  drifted between writes.

## Why deduplication has to happen here

Materialization into LadybugDB is a plain `COPY` with no `ignore_errors`.
LadybugDB 0.18's `ignore_errors` path silently drops *valid* rows in proportion
to batch size, so it cannot be used. But LadybugDB does enforce unique node
primary keys and unique relationship `(src, dst)` pairs — so a single duplicate
reaching it hard-fails the whole COPY.

Deduplicating in staging is what makes a clean COPY both safe and complete. This
is not an optimization; removing it breaks ingestion.

## Querying staged data

```python
from robosystems.graph_api.models.tables import TableQueryRequest

response = manager.query_table(TableQueryRequest(
    graph_id="kg123",
    sql="SELECT count(*) FROM Entity WHERE identifier IS NULL",
))
# response.columns, .rows, .row_count, .execution_time_ms

for chunk in manager.query_table_streaming(request, chunk_size=1000):
    if "error" in chunk:
        break
    process(chunk["rows"])
    if chunk["is_last_chunk"]:
        break
```

`query_table` runs on a hardened read-only connection (see below). Internal
write and DDL paths use `execute_write(request)` on the read-write connection
instead — that is what backs `POST /tables/execute`, and it is not exposed to
tenant SQL.

## Two connections, two threat models

`pool.get_connection(graph_id)` returns the pooled read-write connection, with
httpfs and `postgres_scanner` loaded. Staging and materialization need it: they
read S3 and run `CREATE TABLE AS SELECT ... postgres_scan(...)`.

`pool.get_readonly_connection(graph_id)` is a separate, unpooled connection for
untrusted tenant SQL. It opens the file `read_only` with
`enable_external_access=false` and `lock_configuration=true`, and loads no
extensions. That blocks external `ATTACH`, httpfs egress, `read_text` /
`read_blob`, `postgres_scan`, and `COPY ... TO` — the connection can read only
this graph's own local staging tables.

Two consequences fall out of DuckDB refusing a second connection with a
different configuration to the same file:

- Opening the read-only connection **closes the read-write connections for that
  graph first**, under the per-graph lock. Because the staging write methods run
  outside that lock, a tenant read can abort a concurrent staging run mid-
  statement. That is a retryable failure, not data loss.
- Spill-to-disk is unavailable on the read-only connection, since DuckDB counts
  `temp_directory` as external file access. Read queries are therefore
  memory-bound: an over-limit sort or aggregation errors out rather than
  spilling. A bounded failure is the right trade for a tenant-facing surface.

Table names are validated against `^[a-zA-Z0-9_-]+$` before being quoted into
SQL, values are bound as parameters, and paths are resolved through
`get_duckdb_staging_path` with a defense-in-depth check that the result stays
under the base directory.

Column names are tenant-controlled too — they are probed from the uploaded
file and become identifiers in the staging DDL. `create_table` and
`insert_into_table` refuse any probed name outside
`^[A-Za-z_][A-Za-z0-9_]*$` (max 128 chars) with a 400 before building SQL
(`validate_column_names`), and every identifier interpolation goes through
`quote_identifier`, which doubles embedded quotes. The validator is the
barrier; the quoting is defense in depth for the next interpolation site
someone adds. `tests/graph_api/core/duckdb/test_staging_identifiers.py` drives
both against real DuckDB.

## Incremental loads

Pass `file_id_map` at create time and each row carries its source file's ID:

```python
manager.create_table(TableCreateRequest(
    graph_id="kg123",
    table_name="Entity",
    s3_pattern=["s3://bucket/2026-02.parquet"],
    file_id_map={"s3://bucket/2026-02.parquet": "3f2a…"},   # must be a UUID
))

manager.delete_file_data("kg123", "Entity", "3f2a…")   # retract one file's rows
manager.refresh_table("kg123", "Entity")               # rebuild from the registry
```

`refresh_table` rebuilds the table from the *current* set of completed files in
the platform PostgreSQL registry (`GraphTable` / `GraphFile`) — the authority on
which files belong to a table, not the S3 listing. It 404s if the table is not
registered and 400s if the registry has no completed files for it.

On the materialize side, `TableMaterializationRequest(incremental=True)` COPYs
only rows not already present, by anti-joining against a keyset snapshot of
existing node identifiers or relationship `(src, dst)` pairs. The default,
`incremental=False`, assumes a freshly rebuilt empty target.

## Pool and configuration

```python
from robosystems.graph_api.core.duckdb import get_duckdb_pool

pool = get_duckdb_pool()          # initialized once in app.py
stats = pool.get_stats()          # connections created/reused/closed, health checks
pool.close_database_connections("kg123")
pool.force_database_cleanup("kg123")   # call when the graph is deleted
```

The pool is created with `max_connections_per_db=3` and
`connection_ttl_minutes=30`. Asking for a fourth connection closes the oldest
rather than queueing. Three is a deliberate ceiling: DuckDB is less tolerant of
many concurrent connections per file than LadybugDB, and the staging workload is
a small number of long operations rather than many short ones.

| Setting | Source |
| --- | --- |
| Staging directory | `DUCKDB_STAGING_PATH` (default `./data/staging`) |
| Memory limit | Per-graph override → tier `duckdb_memory_limit` from `.github/configs/graph.yml` via `CLUSTER_TIER` |
| Threads | Tier `duckdb_max_threads`, aligned to instance vCPUs |
| S3 credentials / endpoint | Standard AWS env vars; `AWS_ENDPOINT_URL` switches to path-style URLs for LocalStack |

Memory can be boosted for the duration of a staging run and then restored —
`set_duckdb_memory_override` / `reconfigure_memory_limit` — for the same reason
LadybugDB does it: running permanently high on an instance where both engines
are resident is how the container gets OOM-killed.

Replicas set `duckdb_memory_limit: "0"`. They do not run staging at all, and the
`/tables` routes return 501 there. A DuckDB file that has not finished
downloading from S3 raises `FileNotFoundError` rather than being created empty.

## Related

- [`../ladybug/README.md`](../ladybug/README.md) — the materialization target
- [`../README.md`](../README.md) — core services map and the ingest path
- [`../../README.md`](../../README.md) — the `/tables` endpoints
