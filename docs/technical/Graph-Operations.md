# Graph Operations

This guide shows you how to perform graph-lifecycle writes — creating subgraphs and backups, changing tier, and materializing — through the RoboSystems CQRS command surface and how to monitor long-running operations to completion.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [The CQRS Command Surface](#the-cqrs-command-surface)
- [The OperationEnvelope](#the-operationenvelope)
- [Idempotency](#idempotency)
- [Monitoring Progress with SSE](#monitoring-progress-with-sse)
- [Operations Reference](#operations-reference)
- [Worked Example: Create a Backup and Watch It Complete](#worked-example-create-a-backup-and-watch-it-complete)
- [Downloading and Unpacking a Backup](#downloading-and-unpacking-a-backup)
- [Common Pitfalls](#common-pitfalls)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

Graph operations are the **write half** of a CQRS (Command Query Responsibility Segregation) split. Anything that mutates the lifecycle of a graph — its subgraphs, backups, tier, or materialized state — goes through one uniform door:

```text
POST /v1/graphs/{graph_id}/operations/{op_name}
```

Every one of these commands returns the same JSON shape, an **`OperationEnvelope`**, and every one accepts an optional **`Idempotency-Key`** header for safe retries. Operations that finish immediately complete inside the response body; operations that hand off to a background worker return an `operation_id` you can stream over Server-Sent Events (SSE) until they finish.

Reads stay on the other side of the split as ordinary REST `GET`s — listing subgraphs, listing backups, checking operation status, and health checks are never tunneled through the command surface.

Fifteen operations are mounted on this surface. The lifecycle group is the one most callers reach for:

| Operation | Mutates | Sync or async |
| --- | --- | --- |
| **create-subgraph** | Adds a child graph under a parent | Sync (empty) or async (forked) |
| **delete-subgraph** | Removes a child graph | Sync |
| **create-backup** | Produces a stored, downloadable dump | Async |
| **change-tier** | Migrates a graph to a new instance tier | Async |
| **materialize** | Rebuilds the analytical graph from OLTP/staged data | Async (sync on dry run) |
| **update-graph-metadata** | Renames, re-describes, or re-tags a graph | Sync |
| **delete-graph** | Destroys the graph itself | Async |

The rest move content and memory in and out of the graph, using the identical envelope and `Idempotency-Key` contract:

| Operation | Mutates | Sync or async |
| --- | --- | --- |
| **create-file-upload** | Presigns an upload target for a staging file | Sync |
| **ingest-file** | Stages an uploaded file into a columnar table | Sync |
| **delete-file** | Removes a staged file | Sync |
| **index-document** | Adds a document to the search index | Sync |
| **delete-document** | Removes a document from the index | Sync |
| **remember** | Stores a semantic memory | Sync |
| **update-memory** | Revises a stored memory | Sync |
| **forget** | Deletes a stored memory | Sync |

`delete-graph` and the content/memory operations are covered briefly in the [Operations Reference](#operations-reference).

**Not on this surface:** `change-reporting-style` is a RoboLedger operation, not a graph-lifecycle one — it lives at `POST /extensions/roboledger/{graph_id}/operations/change-reporting-style`. See [RoboLedger Operations](RoboLedger-Operations).

## Prerequisites

Before starting, ensure you have:

- A RoboSystems account and an API key — see [Quick Start](Quick-Start)
- A writable graph to act on — one you create in the app with **Create Graph** (see [Quick Start](Quick-Start)). Substitute your own `graph_id` everywhere `kg1a2b3c4d5` appears below.
- `jq` and `curl` on your path

The shared `sec` repository is **read-only** — these operations are blocked on it. Use a graph you own.

## Quick Start

Send your API key as `X-API-Key`. The base URL is `https://api.robosystems.ai`.

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
```

```bash
# Create an empty subgraph (synchronous — completes in the response)
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5/operations/create-subgraph" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "dev", "display_name": "Development Environment"}'
```

```bash
# Kick off a backup (asynchronous — returns 202 and an operation_id)
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5/operations/create-backup" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"backup_format": "full_dump", "retention_days": 30}'
```

The full surface — every field, every status code — is in the API reference at [https://robosystems.ai/docs/api](https://robosystems.ai/docs/api). This page carries the concepts and the worked tasks; the spec is the exhaustive reference.

## The CQRS Command Surface

All lifecycle writes share one URL shape. The `graph_id` is a path parameter — authentication and per-graph access are validated by FastAPI dependencies before the handler runs, so the graph in the URL is always the scope of the command.

```text
POST /v1/graphs/{graph_id}/operations/{op_name}
```

Funneling every write through one envelope means the same idempotency, auditing, and progress-monitoring machinery applies uniformly. A client that can call one operation can call them all.

**Sync vs async.** Each operation declares whether it runs to completion in the request or hands off to a background worker:

- **Synchronous** operations finish in the response body and return **HTTP 200** with `status: "completed"`. The `result` field carries the command's output.
- **Asynchronous** operations enqueue work and return **HTTP 202** with `status: "pending"`. The `result` carries an `operation_id` and a `monitoring` block pointing at the SSE endpoint. You watch the tail of the operation over [SSE](#monitoring-progress-with-sse).

Either way, the response is an `OperationEnvelope` and the `operation_id` is the through-line that ties the response to its audit log, its SSE stream, and its status snapshot.

## The OperationEnvelope

Every operation — sync or async, success or failure — returns the same envelope. On the wire the field names are **camelCase**:

| JSON field | Type | Meaning |
| --- | --- | --- |
| **`operation`** | string | The kebab-case operation name (e.g. `create-backup`) |
| **`operationId`** | string | An `op_`-prefixed identifier. Always present. The correlation key for SSE, `/status`, and audit logs |
| **`status`** | string | `completed` (sync done), `pending` (async accepted), or `failed` |
| **`result`** | object or null | The command payload. For async ops this is `{status, message, monitoring}`; may be `null` while pending |
| **`at`** | string | ISO-8601 UTC timestamp with a `Z` suffix |
| **`createdBy`** | string or null | The initiating user id |
| **`idempotentReplay`** | boolean | `true` when the envelope was served from the idempotency cache (the command did **not** re-run) |

A representative async response:

```json
{
  "operation": "create-backup",
  "operationId": "op_01J9ZK7M3QABCDEF...",
  "status": "pending",
  "result": {
    "status": "accepted",
    "message": "Backup creation started",
    "monitoring": { "sse_endpoint": "/v1/operations/op_01J9ZK7M3QABCDEF.../stream" }
  },
  "at": "2026-06-11T18:22:05Z",
  "createdBy": "user_abc123",
  "idempotentReplay": false
}
```

**Note:** Python field names are snake_case internally, but clients should read the camelCase keys (`operationId`, `createdBy`, `idempotentReplay`) off the wire.

## Idempotency

Every operation accepts an optional `Idempotency-Key` header. It is opt-in protection for retries on a flaky network — without it, a retried POST runs the command twice.

The semantics:

- **Same key + identical body, within the TTL** → the cached envelope is replayed with `idempotentReplay: true`. The command does **not** run again.
- **Same key + a different body** → **HTTP 409 Conflict**. The key is bound to the first request body it saw.
- **TTL** → 24 hours.
- **Scope** → keys are user-scoped. You cannot replay another user's operation, even with the same key string.

```bash
# Retrying this exact call with the same key replays the cached result
# rather than creating a second backup.
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5/operations/create-backup" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: backup-2026-06-11-001" \
  -H "Content-Type: application/json" \
  -d '{"backup_format": "full_dump", "retention_days": 30}'
```

**Important:** A natural mistake is to reuse a key while changing the body (for example, bumping `retention_days`). That returns a 409, not a fresh operation. Pick a new key when the request changes.

## Monitoring Progress with SSE

Async operations return an `operation_id`. Two endpoints let you follow it, both mounted under `/v1`:

```text
GET /v1/operations/{operation_id}/stream    # live SSE event stream (text/event-stream)
GET /v1/operations/{operation_id}/status    # point-in-time JSON snapshot
```

**Live stream.** Connect to `/stream` to receive events as the operation progresses. The event types are:

| Event type | Meaning |
| --- | --- |
| **`operation_started`** | The worker picked up the operation |
| **`operation_progress`** | Incremental progress update (fractional, 0 to 1) |
| **`operation_completed`** | Finished successfully; carries the result data |
| **`operation_error`** | Failed; carries the error |
| **`operation_cancelled`** | The operation was cancelled |

```bash
# Stream events live (-N disables curl buffering so events arrive as emitted)
curl -N "https://api.robosystems.ai/v1/operations/op_01J9ZK7M3QABCDEF.../stream" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

On reconnect, pass `from_sequence` to replay events you missed rather than starting blind.

**Snapshot.** If you do not want a long-lived connection, poll `/status` for a point-in-time JSON view — it returns the `operation_id`, `operation_type`, `status`, timestamps, `graph_id`, an optional `result` or `error`, and a `_links.stream` pointer back to the live endpoint.

```bash
# Point-in-time status (no streaming)
curl -s "https://api.robosystems.ai/v1/operations/op_01J9ZK7M3QABCDEF.../status" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

**Note:** SSE streaming consumes no credits, but it is authenticated, enforces ownership (you can only stream your own operations), and is rate-limited to 5 concurrent SSE connections per user.

## Operations Reference

Each subsection lists the body fields that matter and the relevant guards. The OpenAPI spec at [https://robosystems.ai/docs/api](https://robosystems.ai/docs/api) is authoritative for the full request and response models.

### create-subgraph

Adds a child graph under a parent. The resulting subgraph id is `{parent_graph_id}_{name}` (for example, `kg1a2b3c4d5_dev`).

- **Body:** `name` (alphanumeric, 1–20 chars, lowercased), `display_name` (required), optional `description`, `schema_extensions`, `metadata`, `subgraph_type` (`static` — the parent's base schema plus extensions, and the default — or `knowledge` for a knowledge-only schema, or `empty` for a bare database), and `fork_parent`.
- **Sync/async:** Creating an empty subgraph is **synchronous** (200). Setting `fork_parent: true` copies the parent's data and runs **asynchronously** (202 with an `operation_id`).
- Subgraph count is capped by tier — see [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy).

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5/operations/create-subgraph" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "dev", "display_name": "Development Environment"}'
```

### delete-subgraph

Removes a child graph. **Synchronous.**

- **Body:** `subgraph_name`, `force` (default `false`), `backup_first` (default `true`).
- Backs up before deleting unless you opt out.

### create-backup

Produces a stored dump. **Asynchronous** (202).

- **Body:** `backup_format` (must be `full_dump`), `backup_type` (default `full`), `retention_days` (1–90, default 30), `compression` (forced `true`), optional `schedule`.
- **Requires graph `admin`.** A member or viewer gets 403.
- Blocked on shared repositories, and killable per deployment: with `BACKUP_CREATION_ENABLED=false` the operation returns 403 for everyone.

**You do not have to ask for a backup to have one.** Every active customer graph and subgraph is backed up nightly, on a schedule that fans out one job per graph so a single failure does not take the fleet's backups with it. `create-backup` is the *on-demand* path — the one you reach for before a risky change, or when you want a dump to download right now. Backups on the read side carry an `initiated_by` field distinguishing the two: `scheduled` backups are taken on your behalf and do not count against the tier's daily backup limit; `user` ones do. Shared repositories such as `sec` are excluded from the nightly sweep — they are re-ingestible from their public source.

### Backups Are for Downloading, Not Restoring

There is no customer-facing restore operation, and this is deliberate rather than a gap. Every graph type that has an upstream is recovered by rebuilding from that upstream, not from a snapshot: entity graphs re-materialize from the extensions OLTP database, generic graphs from their staged source files, shared repositories by re-ingesting. Backups exist so you have a retrievable record of what a graph held — see [Downloading and Unpacking a Backup](#downloading-and-unpacking-a-backup).

The two classes with no upstream — entity subgraphs and the semantic memory store — are recovered by downloading the payload and rebuilding, or by an operator-run restore that has no public endpoint.

### change-tier

Migrates a graph to a new instance tier (an EBS volume migration). **Asynchronous** (202).

- **Body:** `new_tier`, one of `ladybug-standard`, `ladybug-large`, `ladybug-xlarge`.

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5/operations/change-tier" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"new_tier": "ladybug-large"}'
```

### materialize

Rebuilds the analytical graph (LadybugDB) from OLTP or staged source data. **Asynchronous** (202), except a dry run which runs **synchronously** — it returns a `completed` envelope reporting what it would do, without executing.

- **Body:** `source` (`staged` or `extensions`; omit it to materialize from the DuckDB staging tables), `rebuild`, `force`, `dry_run`, `materialize_embeddings` — all four booleans default `false`.

```bash
# Materialize an entity graph from the extensions OLTP database
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5/operations/materialize" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{"source": "extensions", "rebuild": false}'
```

```bash
# Dry run — synchronous, returns a completed envelope with the plan, materializes nothing
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5/operations/materialize" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"dry_run": true}'
```

After a materialize completes, the rebuilt graph is queryable — see [Querying the Analytical Graph](Querying-the-Analytical-Graph).

### delete-graph

Deletes the graph itself. **Asynchronous** (202).

- **Body:** `confirm` (required — must equal the `graph_id` in the URL) and `at_period_end` (default `false`). With `at_period_end: true`, cancellation and teardown are deferred to the end of the current billing period and the graph stays usable until then.
- The caller must be both an org **owner** and a graph **admin**.

### Content and Memory Operations

The remaining eight operations share the surface and the contract, and are all synchronous:

- **`create-file-upload` / `ingest-file` / `delete-file`** — the staging-file path: presign an upload target, stage the uploaded file into a columnar table, then `materialize` to move it into the graph.
- **`index-document` / `delete-document`** — add and remove documents in the search index. See [Search and AI Retrieval](Search-and-AI-Retrieval).
- **`remember` / `update-memory` / `forget`** — the per-graph semantic memory store an Operator writes to. See [AI Operators and MCP](AI-Operators-and-MCP).

## Worked Example: Create a Backup and Watch It Complete

This walks the full async path end to end: kick off a backup, monitor it over SSE, and confirm it landed via the read side.

### Step 1: Kick Off the Backup

Backups are asynchronous, so this returns HTTP 202 with a `pending` envelope and an `operation_id`.

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5/operations/create-backup" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: backup-2026-06-11-001" \
  -H "Content-Type: application/json" \
  -d '{"backup_format": "full_dump", "retention_days": 30}'
```

**Output:**

```json
{
  "operation": "create-backup",
  "operationId": "op_01J9ZK7M3QABCDEF...",
  "status": "pending",
  "result": {
    "status": "accepted",
    "message": "Backup creation started",
    "monitoring": { "sse_endpoint": "/v1/operations/op_01J9ZK7M3QABCDEF.../stream" }
  },
  "at": "2026-06-11T18:22:05Z",
  "createdBy": "user_abc123",
  "idempotentReplay": false
}
```

Copy the `operationId` — every following step keys off it.

### Step 2: Stream Progress

Connect to the SSE endpoint and watch the operation move through its event types.

```bash
curl -N "https://api.robosystems.ai/v1/operations/op_01J9ZK7M3QABCDEF.../stream" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

The stream emits `operation_started`, then one or more `operation_progress` events, then a terminal `operation_completed` (or `operation_error` if something failed). The completion event carries the result data.

### Step 3: Or Take a Snapshot Instead

If you would rather poll than hold a connection open, hit `/status` for a point-in-time view:

```bash
curl -s "https://api.robosystems.ai/v1/operations/op_01J9ZK7M3QABCDEF.../status" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

### Step 4: Confirm the Backup Landed

The read side is a plain `GET` — no envelope, no operation. List the graph's backups to see the new one:

```bash
curl -s "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5/backups" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

Because you sent an `Idempotency-Key` in Step 1, re-running that exact request within 24 hours replays the original envelope (`idempotentReplay: true`) instead of creating a second backup.

## Downloading and Unpacking a Backup

Downloading is a read, not an operation. Ask for a time-limited presigned URL, then fetch the file straight from object storage:

```bash
# Returns { "download_url": "...", "expires_at": "...", "backup_id": "..." }
curl -s "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5/backups/bk1a2b3c4d5/download?expires_in=3600" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"

# Fetch the file — the presigned URL carries its own auth, so no API key here
curl -L -o backup.zip "PASTE_THE_DOWNLOAD_URL"
```

The URL expires after `expires_in` seconds (300–86400, default 3600), and each download counts against your graph tier's or repository plan's monthly download allowance.

### Which File You Get

You don't have to guess: every entry from `GET /v1/graphs/{graph_id}/backups` carries a **`download_extension`** telling you what the download will be and therefore how to unpack it. It is `null` only when the backup has no stored object yet.

| `download_extension` | Filename | Contents |
| --- | --- | --- |
| `.lbug.zip` | `{graph_id}_{timestamp}.lbug.zip` | ZIP archive containing the LadybugDB database file `{graph_id}.lbug` |
| `.lbug.zst` | `{graph_id}_{timestamp}.lbug.zst` | A single zstd-compressed LadybugDB database file — how shared repository snapshots (e.g. `sec`) ship |

### Unpacking a `.lbug.zip`

Nothing special — `unzip` on macOS/Linux, or double-click in Finder / Explorer:

```bash
unzip kg1a2b3c4d5_20260805_031500.lbug.zip
# -> kg1a2b3c4d5.lbug
```

### Unpacking a `.lbug.zst`

Shared repository snapshots are compressed with [zstd](https://github.com/facebook/zstd), which is **not** installed by default on macOS or most Linux distributions. Install it first:

```bash
# macOS (Homebrew)
brew install zstd

# Debian / Ubuntu
sudo apt-get install zstd

# Amazon Linux / Fedora / RHEL
sudo dnf install zstd

# Windows — via winget, or use 7-Zip which reads .zst natively
winget install Facebook.Zstandard
```

Then decompress from the directory holding the download. On macOS you can jump straight there by right-clicking the enclosing folder → **Services** → **New Terminal at Folder**:

```bash
# Writes sec_20260805_031500.lbug and keeps the .zst
zstd -d sec_20260805_031500.lbug.zst

# Same, but removes the compressed file afterwards
zstd -d --rm sec_20260805_031500.lbug.zst
```

No `--long` flag is needed on your side: the snapshots are compressed with a 128 MB window that plain `zstd -d` handles. Budget disk for roughly **2×** the download size — these files compress at about 2.2×.

### Using the Decompressed `.lbug`

The result is an ordinary LadybugDB database file. Open it from Python with the `ladybug` package (pinned to `0.18.1` in this repo — match the version the snapshot was written with):

```python
import ladybug as lbug

db = lbug.Database("sec_20260805_031500.lbug")
conn = lbug.Connection(db)

result = conn.execute("MATCH (e:Entity) RETURN e.name LIMIT 10")
while result.has_next():
    print(result.get_next())

conn.close()
```

If you run your own stack, you can also drop the file into its database directory — see [Self-hosted deployments](#self-hosted-deployments).

## Common Pitfalls

### Shared Repositories Reject These Operations

`create-backup`, `change-tier`, `delete-subgraph`, and `delete-graph` all return **403** on shared repositories such as `sec`. Run lifecycle operations against a graph you own.

### There Is No restore-backup Operation

Looking for one is the most common wrong turn on this surface. Backups are a download capability; recovery is a rebuild from the upstream — `materialize` for entity and generic graphs, re-ingestion for shared repositories. See [Backups Are for Downloading, Not Restoring](#backups-are-for-downloading-not-restoring).

### create-backup Needs Graph Admin

`create-backup` calls `verify_admin_access` before anything else — a member or viewer gets **403**, as does everyone when the deployment sets `BACKUP_CREATION_ENABLED=false`.

### create-backup Only Supports full_dump

Any `backup_format` other than `full_dump` returns **400**.

### Retention Is Capped, Not Rejected

If `retention_days` exceeds your tier's maximum (7, 30, or 90 depending on tier), it is silently **clamped** to the cap rather than rejected. Check the stored value if it matters. 90 days is the hard ceiling on any tier: the storage lifecycle expires backup objects then regardless of what was requested, so an uncapped value would leave a completed record pointing at a deleted file.

### Reusing an Idempotency-Key With a Changed Body Is a 409

The key is bound to the first body it saw. Change the body and you get a **409 Conflict**, not a new operation. Use a fresh key when the request changes.

### delete-graph Requires Explicit Confirmation and Elevated Roles

`delete-graph` returns **400** unless the `confirm` field equals the `graph_id`, and the caller must be both an org **owner** and a graph **admin**. This is deliberate friction on an irreversible action.

### compression Must Be true

The backup request model rejects `compression: false`. Omit it (it defaults to and is forced to `true`) rather than trying to turn it off.

## Self-hosted deployments

**Querying a downloaded `.lbug` on your own stack.** Drop the decompressed file into the local database directory and query it directly:

```bash
mv sec_20260805_031500.lbug ./data/lbug-dbs/sec.lbug
just lbug-query sec "MATCH (e:Entity) RETURN e.name LIMIT 10"
```

**Nightly backups.** The nightly backup schedule ships stopped on a local `dev` stack, so the only backups you will see there are ones you asked for with `create-backup`.

## Related Documentation

**Wiki Guides:**

- **[Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy)** - What `graph_id`, subgraphs, tiers, and shared repositories are — the things these operations act on
- **[Querying the Analytical Graph](Querying-the-Analytical-Graph)** - Query the LadybugDB graph after a `materialize` rebuilds it
- **[RoboLedger Operations](RoboLedger-Operations)** - The extensions analogue of this surface (`POST /extensions/roboledger/{g}/operations/{op}`), using the same `OperationEnvelope` and `Idempotency-Key` contract

**Codebase Documentation:**

- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration in codebase
- **[Graph Routing Middleware](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/graph/README.md)** - Multi-tenant graph routing in codebase

**API Reference:**

- **[API reference](https://robosystems.ai/docs/api)** - Full request/response models and status codes (machine-readable OpenAPI spec)

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
