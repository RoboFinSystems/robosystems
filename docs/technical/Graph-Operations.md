# Graph Operations

Graph operations change a graph's lifecycle — subgraphs, backups, tier, materialization, metadata and deletion — through one command endpoint. This page covers each operation, the guards that refuse it, and how to follow a long-running one to completion.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development.md).

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

The lifecycle group is the one most callers reach for:

| Operation | Mutates | Sync or async |
| --- | --- | --- |
| **create-subgraph** | Adds a child graph under a parent | Sync (empty) or async (forked) |
| **delete-subgraph** | Removes a child graph | Sync |
| **create-backup** | Produces a stored, downloadable dump | Async |
| **change-tier** | Migrates a graph to a new instance tier | Async |
| **materialize** | Rebuilds the analytical graph from OLTP/staged data | Async (sync on dry run) |
| **update-graph-metadata** | Renames, re-describes, or re-tags a graph | Sync |
| **delete-graph** | Destroys the graph itself | Sync (teardown is queued) |

The rest move content and memory in and out of the graph, using the identical envelope and `Idempotency-Key` contract:

| Operation | Mutates | Sync or async |
| --- | --- | --- |
| **create-file-upload** | Presigns an upload target for a staging file | Sync |
| **ingest-file** | Stages an uploaded file into a columnar table | Sync, or async for a large file or with `ingest_to_graph` |
| **delete-file** | Removes a staged file | Sync |
| **index-document** | Adds a document to the search index | Sync |
| **delete-document** | Removes a document from the index | Sync |
| **remember** | Stores a semantic memory | Sync |
| **update-memory** | Revises a stored memory | Sync |
| **forget** | Deletes a stored memory | Sync |

`update-graph-metadata`, `delete-graph` and the content/memory operations are covered briefly in the [Operations Reference](#operations-reference).

**Not on this surface:** `change-reporting-style` is a RoboLedger operation, not a graph-lifecycle one — it lives at `POST /extensions/roboledger/{graph_id}/operations/change-reporting-style`. See [RoboLedger Operations](RoboLedger-Operations.md).

## Prerequisites

Before starting, ensure you have:

- A RoboSystems account and an API key — see [Quick Start](Quick-Start.md)
- A writable graph to act on — one you create in the app with **Create Graph** (see [Quick Start](Quick-Start.md)). Substitute your own `graph_id` everywhere `kg1a2b3c4d5e6f7a8b` appears below.
- `jq` and `curl` on your path

The shared `sec` repository is **read-only** — these operations are blocked on it. Use a graph you own.

## Quick Start

Send your API key as `X-API-Key`. The base URL is `https://api.robosystems.ai`.

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
```

```bash
# Create an empty subgraph (synchronous — completes in the response)
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/operations/create-subgraph" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "dev", "display_name": "Development Environment"}'
```

```bash
# Kick off a backup (asynchronous — returns 202 and an operation_id)
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/operations/create-backup" \
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

- **Synchronous** operations finish in the response body with `status: "completed"`. The `result` field carries the command's output.
- **Asynchronous** operations enqueue work and return `status: "pending"`. You follow the operation's `operationId` over [SSE](#monitoring-progress-with-sse).

The HTTP code is fixed per operation, not chosen by how the call ran: `create-backup`, `change-tier`, `materialize` and `delete-graph` always answer **202** — including a materialize dry run and a `delete-graph`, which come back `completed` — and the rest answer **200**, including a forked `create-subgraph`, which comes back `pending`. Either way, the response is an `OperationEnvelope`, and its `status` — not the HTTP code — says which you got. A request the operation refuses (validation, access, a conflict) is an HTTP error with a `detail` and `request_id` rather than an envelope; see [Operations Contract](Operations-Contract.md#when-an-operation-fails) and [Errors and Rate Limits](Errors-and-Rate-Limits.md).

## The OperationEnvelope

Every operation on this surface returns an `OperationEnvelope` — `operation`, `operationId`, `status` (`completed`, `pending`, or `failed`), `result`, `at`, `createdBy`, `idempotentReplay`, camelCase on the wire. The field reference, success and pending examples, and what a failure looks like (an HTTP error body for a refused request, an `operation_error` event for async work that fails later) are in [Operations Contract](Operations-Contract.md#the-operationenvelope).

## Idempotency

Every operation accepts an optional `Idempotency-Key` header: a retry with the same key and body within 24 hours replays the original envelope with `idempotentReplay: true`, and the same key with a different body is a **409**. The full rules — scope, in-flight retries, and what happens after a failure — are in [Operations Contract](Operations-Contract.md#idempotency-key).

## Monitoring Progress with SSE

Async operations are followed by `operationId` at `GET /v1/operations/{operation_id}/stream` (Server-Sent Events) or `GET /v1/operations/{operation_id}/status` (a snapshot). The event names, payloads, and reconnecting with `from_sequence` are in [Operations Contract](Operations-Contract.md#following-an-async-operation).

## Operations Reference

Each subsection lists the body fields that matter and the relevant guards. The OpenAPI spec at [https://robosystems.ai/docs/api](https://robosystems.ai/docs/api) is authoritative for the full request and response models.

### create-subgraph

Adds a child graph under a parent. The resulting subgraph id is `{parent_graph_id}_{name}` (for example, `kg1a2b3c4d5e6f7a8b_dev`).

- **Body:** `name` (alphanumeric, 1–20 chars, lowercased), `display_name` (required), optional `description`, `metadata`, `subgraph_type`, and `fork_parent`.
- **The schema follows `subgraph_type`:** `static` (the default) gets the parent's base schema plus the parent's extensions, `knowledge` a knowledge-only schema, and `empty` a bare database. The request model also carries a `schema_extensions` field, but the operation does not read it — choose the schema with `subgraph_type`.
- **Sync/async:** Creating an empty subgraph is **synchronous**. Setting `fork_parent: true` copies the parent's data and runs **asynchronously** — the envelope comes back `pending` (over HTTP 200) with an `operationId` to follow.
- **Requires admin on the parent graph.** A name already in use is a **409**; a parent at its tier's subgraph cap, a tier with no subgraph capacity, or a shared repository is a **403**. See [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy.md#subgraphs).

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/operations/create-subgraph" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "dev", "display_name": "Development Environment"}'
```

### delete-subgraph

Removes a child graph. **Synchronous.**

Sent to the **parent** graph's URL, naming the subgraph in the body.

- **Body:** `subgraph_name`, `force` (default `false`), `backup_first` (default `true`).
- **Requires admin on the parent graph** (403 otherwise); an unknown `subgraph_name` is a 404, and a shared repository refuses with 403.
- **A subgraph that holds data is refused unless `force: true`.**
- **`backup_first` backs up before deleting**, and registers that backup on the **parent's** backup list, where it can still be listed and downloaded after the subgraph is gone. If the backup fails, the subgraph is not deleted.

### create-backup

Produces a stored dump. **Asynchronous** (202).

- **Body:** every field is optional. `backup_format` (`full_dump`, the default and only value), `backup_type` (`full`, likewise), `retention_days` (1–90, default 30), `compression` (must stay `true`). Any other `backup_format` or `backup_type`, `compression: false`, or a `retention_days` outside 1–90 fails validation with **422**.
- **Requires graph `admin`.** A member or viewer gets 403.
- **Counts against the tier's daily backup limit.** Once a graph has used its allowance for the day, `create-backup` returns **429**; the cap per tier is in `GET /v1/graphs/tiers`.
- Blocked on shared repositories, and killable per deployment: with `BACKUP_CREATION_ENABLED=false` the operation returns 403 for everyone.

**You do not have to ask for a backup to have one.** Every active customer graph and subgraph is backed up nightly, on a schedule that fans out one job per graph so a single failure does not take the fleet's backups with it. `create-backup` is the *on-demand* path — the one you reach for before a risky change, or when you want a dump to download right now. Backups on the read side carry an `initiated_by` field: `scheduled` backups are taken on your behalf and do not count against the tier's daily backup limit; `user` ones do; `final` is the backup taken when a graph is deleted (see [delete-graph](#delete-graph)). Shared repositories such as `sec` are excluded from the nightly sweep — they are re-ingestible from their public source.

### Backups Are for Downloading, Not Restoring

There is no customer-facing restore operation, and this is deliberate rather than a gap. Every graph type that has an upstream is recovered by rebuilding from that upstream, not from a snapshot: entity graphs re-materialize from the extensions OLTP database, generic graphs from their staged source files, shared repositories by re-ingesting. Backups exist so you have a retrievable record of what a graph held — see [Downloading and Unpacking a Backup](#downloading-and-unpacking-a-backup).

The two classes with no upstream — entity subgraphs and the semantic memory store — are recovered by downloading the payload and rebuilding, or by an operator-run restore that has no public endpoint.

### change-tier

Migrates a graph to a new instance tier (an EBS volume migration). **Asynchronous** (202).

- **Body:** `new_tier`, one of `ladybug-standard`, `ladybug-large`, `ladybug-xlarge` (anything else is a 422).
- **Org owner only.** A tier change changes the price, so only the owner of the organization that pays for the graph can make it (403 otherwise).
- **Needs an active subscription.** A subscription in any other state — including one already mid-change — is a 400, as is asking for the tier the graph is already on.
- **The target tier must have capacity.** When it has none available, the call is a **409** naming the tier; request access from the tier picker in the app, or contact support.
- **A downgrade must fit.** Moving to a smaller tier is refused with 400 when the graph has more subgraphs than the new tier allows, or more stored data than its storage limit; when stored data cannot be measured right now it is a 503 — retry shortly.
- **Billing moves with it.** The new tier's price applies to the subscription, and the graph's monthly credit allocation switches to the new tier's at once. If the payment provider or the migration cannot be started, every change is rolled back.

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/operations/change-tier" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"new_tier": "ladybug-large"}'
```

### materialize

Rebuilds the analytical graph (LadybugDB) from OLTP or staged source data. **Asynchronous** (202), except a dry run which runs **synchronously** — it returns a `completed` envelope reporting what it would do, without executing.

- **Body:** `source` (`staged` or `extensions`), `rebuild`, `force`, `dry_run`, `materialize_embeddings` — all four booleans default `false`.
- **Omit `source` and it follows the graph type:** an entity graph materializes from the extensions database, a custom graph from its staged uploads. Naming the other one is a **400** — an entity graph cannot take `staged`, nor a custom graph `extensions`. An `extensions` run always rebuilds a graph that already exists, building the new copy alongside and swapping it in.
- **Needs the `member` or `admin` role.** A viewer gets 403, as does a subgraph (subgraphs are written to directly, and a rebuild would discard those writes) and a shared repository.
- **One at a time per graph.** While another materialization of the graph is running the call is a **409**; so is a staged run that would re-copy rows the graph already holds — pass `rebuild: true` (see [File Uploads](File-Uploads.md#auto-ingest-vs-batch)).
- **Tier limits apply.** A rebuild that would exceed the tier's limits is a **413**. When the platform cannot check them, or cannot take the per-graph lock, the call is a **503** — retry shortly.

```bash
# Materialize an entity graph from the extensions OLTP database
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/operations/materialize" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{"source": "extensions", "rebuild": false}'
```

```bash
# Dry run — synchronous, returns a completed envelope with the plan, materializes nothing
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/operations/materialize" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"dry_run": true}'
```

After a materialize completes, the rebuilt graph is queryable — see [Querying the Analytical Graph](Querying-the-Analytical-Graph.md).

### update-graph-metadata

Edits the graph's platform-level label — its display name, description and tags. **Synchronous** (200).

- **Body:** `graph_name` (1–255 characters), `description` (up to 1,000), `tags` (up to 20, each trimmed, de-duplicated and capped at 50 characters). All three are optional, but at least one must be present (400 otherwise).
- **Partial update.** Only the fields you send change. `graph_name` cannot be cleared; pass `""` to clear the description and `[]` to clear the tags. `tags` replaces the whole list rather than merging into it.
- **Requires admin on the graph** (403 otherwise). A shared repository's metadata is platform-managed and refuses with 403.
- The `result` echoes the stored `graph_name`, `description` and `tags`, plus `updated_fields` — the fields this call actually changed, empty when the values already matched.

This is not the entity name printed on financial statements; change that with the RoboLedger `update-entity` operation (see [RoboLedger Operations](RoboLedger-Operations.md)).

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/operations/update-graph-metadata" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"graph_name": "Acme Consulting LLC", "tags": ["consulting", "production"]}'
```

### delete-graph

Deletes the graph itself. **Synchronous** in the envelope — it comes back `completed` (over HTTP 202) once the subscription is cancelled — while the teardown itself is queued and runs in the background.

- **Body:** `confirm` (required — must equal the `graph_id` in the URL) and `at_period_end` (default `false`). With `at_period_end: true`, cancellation and teardown are deferred to the end of the current billing period and the graph stays usable until then; that needs an active subscription (400 otherwise).
- The caller must be both an org **owner** and a graph **admin**.
- **An immediate delete credits the unused period; it is never refunded.** A period buys time and an AI-credit allocation, and the credit is the smaller unused share of the two: credits consumed ahead of the time elapsed reduce it, so a graph whose allocation was burned on day one gets nothing back for the days it did not use. On an active card-billed subscription, Stripe's time-based proration and the credits offset appear as two lines on the final invoice, which is finalized in the same call rather than left as a draft, so the net credit lands on the organization's account balance at once and is applied to its next invoice; a graph created right after is charged the difference. On an invoiced plan the single credit line carries the reduced amount, with the time-only figure and the consumed share in its metadata. On an invoiced plan, the credit is a negative line on that period's invoice while it is open, so the invoice charges only for the time used; once that invoice is paid, the credit is issued as a separate invoice with a negative total. A past-due card subscription gets no credit, and a deferred delete needs none — the graph runs to the end of the period it paid for.
- **A final backup is taken before teardown.** It is recorded with `initiated_by: "final"`, and a graph admin can still download it for a grace period after the graph is torn down — the export copy of a departing graph.

### Content and Memory Operations

The content and memory operations share the surface and the contract, and all but `ingest-file` are synchronous:

- **`create-file-upload` / `ingest-file` / `delete-file`** — the staging-file path: presign an upload target, stage the uploaded file into a columnar table, then `materialize` to move it into the graph.
- **`index-document` / `delete-document`** — add and remove documents in the search index. See [Search and AI Retrieval](Search-and-AI-Retrieval.md).
- **`remember` / `update-memory` / `forget`** — the per-graph semantic memory store an Operator writes to. See [AI Operators and MCP](AI-Operators-and-MCP.md).

Two of these families are refused on a subgraph with 403: the staging-file path (`create-file-upload`, `ingest-file`), because a staging rebuild would discard the subgraph's direct writes, and `remember` / `update-memory`, because memory lives on the parent graph. `delete-file` and `forget` stay open on a subgraph, so anything stored there earlier can still be removed.

## Worked Example: Create a Backup and Watch It Complete

This walks the full async path end to end: kick off a backup, monitor it over SSE, and confirm it landed via the read side.

### Step 1: Kick Off the Backup

Backups are asynchronous, so this returns HTTP 202 with a `pending` envelope and an `operation_id`.

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/operations/create-backup" \
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
curl -s "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/backups" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

Because you sent an `Idempotency-Key` in Step 1, re-running that exact request within 24 hours replays the original envelope (`idempotentReplay: true`) instead of creating a second backup.

## Downloading and Unpacking a Backup

Downloading is a read, not an operation. Ask for a time-limited presigned URL, then fetch the file straight from object storage:

```bash
# Returns { "download_url": "...", "expires_at": "...", "backup_id": "..." }
curl -s "https://api.robosystems.ai/v1/graphs/kg1a2b3c4d5e6f7a8b/backups/backup_01J9ZK8N4R5T6V7W8X9YABCDEF/download?expires_in=3600" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"

# Fetch the file — the presigned URL carries its own auth, so no API key here
curl -L -o backup.zip "PASTE_THE_DOWNLOAD_URL"
```

The URL expires after `expires_in` seconds (300–86400, default 3600), and each download counts against your graph tier's or repository plan's monthly download allowance.

### Which File You Get

You don't have to guess: every entry from `GET /v1/graphs/{graph_id}/backups` carries a **`download_extension`** telling you what the download will be and therefore how to unpack it. It is `null` only when the backup has no stored object yet.

| `download_extension` | Filename | Contents |
| --- | --- | --- |
| `.lbdb.zip` | `{graph_id}_{timestamp}.lbdb.zip` | ZIP archive containing the LadybugDB database file `{graph_id}.lbdb` |
| `.lbdb.zst` | `{graph_id}_{timestamp}.lbdb.zst` | A single zstd-compressed LadybugDB database file — how shared repository snapshots (e.g. `sec`) ship |

Archives made before the `.lbdb` rename carry `.lbug` in these positions (`.lbug.zip`, `.lbug.zst`, a `{graph_id}.lbug` member); they unpack and restore the same way.

### Unpacking a `.lbdb.zip`

Nothing special — `unzip` on macOS/Linux, or double-click in Finder / Explorer:

```bash
unzip kg1a2b3c4d5e6f7a8b_20260805_031500.lbdb.zip
# -> kg1a2b3c4d5e6f7a8b.lbdb
```

### Unpacking a `.lbdb.zst`

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
# Writes sec_20260805_031500.lbdb and keeps the .zst
zstd -d sec_20260805_031500.lbdb.zst

# Same, but removes the compressed file afterwards
zstd -d --rm sec_20260805_031500.lbdb.zst
```

No `--long` flag is needed on your side: the snapshots are compressed with a 128 MB window that plain `zstd -d` handles. Budget disk for roughly **2×** the download size — these files compress at about 2.2×.

### Using the Decompressed `.lbdb`

The result is an ordinary LadybugDB database file. Open it from Python with the `ladybug` package (pinned to `0.22.1` in this repo — match the release that wrote the snapshot, which its dataset card and commit title state):

```python
import ladybug as lbug

db = lbug.Database("sec_20260805_031500.lbdb")
conn = lbug.Connection(db)

result = conn.execute("MATCH (e:Entity) RETURN e.name LIMIT 10")
while result.has_next():
    print(result.get_next())

conn.close()
```

If you run your own stack, you can also drop the file into its database directory — see [Self-hosted deployments](#self-hosted-deployments).

## Common Pitfalls

### Shared Repositories Reject These Operations

`create-subgraph`, `delete-subgraph`, `create-backup`, `change-tier`, `materialize`, `update-graph-metadata` and `delete-graph` all return **403** on shared repositories such as `sec`. Run lifecycle operations against a graph you own.

### There Is No restore-backup Operation

Looking for one is the most common wrong turn on this surface. Backups are a download capability; recovery is a rebuild from the upstream — `materialize` for entity and generic graphs, re-ingestion for shared repositories. See [Backups Are for Downloading, Not Restoring](#backups-are-for-downloading-not-restoring).

### create-backup Needs Graph Admin

`create-backup` calls `verify_admin_access` before anything else — a member or viewer gets **403**, as does everyone when the deployment sets `BACKUP_CREATION_ENABLED=false`.

### create-backup Only Supports full_dump

Any `backup_format` other than `full_dump` fails request validation with **422**. Omit the field; `full_dump` is the default.

### Retention Is Capped, Not Rejected

If `retention_days` exceeds your tier's maximum (7, 30, or 90 depending on tier), it is **clamped** to the cap rather than rejected; the envelope's `result.retention_days` carries the value actually applied. A value above 90 is a validation error (**422**). 90 days is the hard ceiling on any tier: the storage lifecycle expires backup objects then regardless of what was requested, so an uncapped value would leave a completed record pointing at a deleted file.

### Reusing an Idempotency-Key With a Changed Body Is a 409

The key is bound to the first body it saw. Change the body and you get a **409 Conflict**, not a new operation. Use a fresh key when the request changes.

### delete-graph Requires Explicit Confirmation and Elevated Roles

`delete-graph` returns **400** unless the `confirm` field equals the `graph_id`, and the caller must be both an org **owner** and a graph **admin**. This is deliberate friction on an irreversible action.

### A Subgraph Cannot Be Materialized

`materialize` returns **403** on a subgraph. A subgraph is written to directly, and a rebuild would discard those writes; load data into a subgraph with Cypher or the MCP schema tools instead (see [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy.md#where-writes-go)).

### compression Must Be true

The backup request model rejects `compression: false` with **422**. Omit it (it defaults to and is forced to `true`) rather than trying to turn it off.

## Self-hosted deployments

**Querying a downloaded `.lbdb` on your own stack.** Drop the decompressed file into the local database directory and query it directly:

```bash
mv sec_20260805_031500.lbdb ./data/lbug-dbs/sec.lbdb
just lbug-query sec "MATCH (e:Entity) RETURN e.name LIMIT 10"
```

**Nightly backups.** The nightly backup schedule ships stopped on a local `dev` stack, so the only backups you will see there are ones you asked for with `create-backup`.

## Related Documentation

**Guides:**

- **[Operations Contract](Operations-Contract.md)** - The envelope, `Idempotency-Key`, and SSE progress contract every operation here follows
- **[Errors and Rate Limits](Errors-and-Rate-Limits.md)** - Error bodies, status codes, and rate-limit headers
- **[Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy.md)** - What `graph_id`, subgraphs, tiers, and shared repositories are — the things these operations act on
- **[Querying the Analytical Graph](Querying-the-Analytical-Graph.md)** - Query the LadybugDB graph after a `materialize` rebuilds it
- **[RoboLedger Operations](RoboLedger-Operations.md)** - The extensions analogue of this surface (`POST /extensions/roboledger/{g}/operations/{op}`), using the same `OperationEnvelope` and `Idempotency-Key` contract

**Codebase Documentation:**

- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration in codebase
- **[Graph Routing Middleware](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/graph/README.md)** - Multi-tenant graph routing in codebase

**API Reference:**

- **[API reference](https://robosystems.ai/docs/api)** - Full request/response models and status codes (machine-readable OpenAPI spec)

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
