# Graph Operations

This guide shows you how to perform graph-lifecycle writes — creating subgraphs and backups, restoring, changing tier, and materializing — through the RoboSystems CQRS command surface and how to monitor long-running operations to completion.

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
- [Common Pitfalls](#common-pitfalls)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

Graph operations are the **write half** of a CQRS (Command Query Responsibility Segregation) split. Anything that mutates the lifecycle of a graph — its subgraphs, backups, tier, or materialized state — goes through one uniform door:

```text
POST /v1/graphs/{graph_id}/operations/{op_name}
```

Every one of these commands returns the same JSON shape, an **`OperationEnvelope`**, and every one accepts an optional **`Idempotency-Key`** header for safe retries. Operations that finish immediately complete inside the response body; operations that hand off to a background worker return an `operation_id` you can stream over Server-Sent Events (SSE) until they finish.

Reads stay on the other side of the split as ordinary REST `GET`s — listing subgraphs, listing backups, checking operation status, and health checks are never tunneled through the command surface.

The six core lifecycle operations:

| Operation | Mutates | Sync or async |
| --- | --- | --- |
| **create-subgraph** | Adds a child graph under a parent | Sync (empty) or async (forked) |
| **delete-subgraph** | Removes a child graph | Sync |
| **create-backup** | Produces a stored dump | Async |
| **restore-backup** | Rebuilds a graph from a dump | Async |
| **change-tier** | Migrates a graph to a new instance tier | Async |
| **materialize** | Rebuilds the analytical graph from OLTP/staged data | Async (sync on dry run) |

Two further operations live on the same surface and follow the same contract: `delete-graph` and `change-reporting-style`. They are covered briefly in the [Operations Reference](#operations-reference).

## Prerequisites

Before starting, ensure you have:

- A running RoboSystems development stack — `just start`
- Demo credentials — `just demo-user` writes your API key to `.local/config.json`
- A writable graph to act on. `just demo-roboledger` provisions an entity graph and gives you a real `graph_id`. Substitute your own `graph_id` everywhere `kg1a2b3c4d5` appears below.
- `jq` and `curl` on your path

The shared `sec` repository is **read-only** — these operations are blocked on it. Use a graph you own.

## Quick Start

Read the API key inline from `.local/config.json` and send it as `X-API-Key`. The base URL for local testing is always `http://localhost:8000`.

```bash
# Create an empty subgraph (synchronous — completes in the response)
curl -s -X POST "http://localhost:8000/v1/graphs/kg1a2b3c4d5/operations/create-subgraph" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"name": "dev", "display_name": "Development Environment"}'
```

```bash
# Kick off a backup (asynchronous — returns 202 and an operation_id)
curl -s -X POST "http://localhost:8000/v1/graphs/kg1a2b3c4d5/operations/create-backup" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"backup_format": "full_dump", "retention_days": 30, "encryption": true}'
```

The full surface — every field, every status code — is in the live OpenAPI spec at [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs) (or `http://localhost:8000/docs` when running locally). This page carries the concepts and the worked tasks; the spec is the exhaustive reference.

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
curl -s -X POST "http://localhost:8000/v1/graphs/kg1a2b3c4d5/operations/create-backup" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Idempotency-Key: backup-2026-06-11-001" \
  -H "Content-Type: application/json" \
  -d '{"backup_format": "full_dump", "retention_days": 30, "encryption": true}'
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
curl -N "http://localhost:8000/v1/operations/op_01J9ZK7M3QABCDEF.../stream" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

On reconnect, pass `from_sequence` to replay events you missed rather than starting blind.

**Snapshot.** If you do not want a long-lived connection, poll `/status` for a point-in-time JSON view — it returns the `operation_id`, `operation_type`, `status`, timestamps, `graph_id`, an optional `result` or `error`, and a `_links.stream` pointer back to the live endpoint.

```bash
# Point-in-time status (no streaming)
curl -s "http://localhost:8000/v1/operations/op_01J9ZK7M3QABCDEF.../status" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

**Note:** SSE streaming consumes no credits, but it is authenticated, enforces ownership (you can only stream your own operations), and is rate-limited to 5 concurrent SSE connections per user.

## Operations Reference

Each subsection lists the body fields that matter and the relevant guards. The OpenAPI spec at [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs) is authoritative for the full request and response models.

### create-subgraph

Adds a child graph under a parent. The resulting subgraph id is `{parent_graph_id}_{name}` (for example, `kg1a2b3c4d5_dev`).

- **Body:** `name` (alphanumeric, 1–20 chars, lowercased), `display_name` (required), optional `description`, `schema_extensions`, `subgraph_type` (only `static`), and `fork_parent`.
- **Sync/async:** Creating an empty subgraph is **synchronous** (200). Setting `fork_parent: true` copies the parent's data and runs **asynchronously** (202 with an `operation_id`).
- Subgraph count is capped by tier — see [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy).

```bash
curl -s -X POST "http://localhost:8000/v1/graphs/kg1a2b3c4d5/operations/create-subgraph" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"name": "dev", "display_name": "Development Environment"}'
```

### delete-subgraph

Removes a child graph. **Synchronous.**

- **Body:** `subgraph_name`, `force` (default `false`), `backup_first` (default `true`).
- Backs up before deleting unless you opt out.

### create-backup

Produces a stored dump. **Asynchronous** (202).

- **Body:** `backup_format` (must be `full_dump`), `backup_type` (default `full`), `retention_days` (1–2555, default 30), `compression` (forced `true`), `encryption` (default `false`), optional `schedule`.
- Encrypted backups can be **restored** but **not downloaded**; unencrypted backups can be downloaded but not restored.

### restore-backup

Rebuilds a graph from a dump. **Asynchronous** (202).

- **Body:** `backup_id`, `create_system_backup` (default `true`), `verify_after_restore` (default `true`).
- Only **encrypted** backups are restorable. Restore is **forbidden on entity graphs** — rebuild those with `materialize` instead (see [Common Pitfalls](#common-pitfalls)).

### change-tier

Migrates a graph to a new instance tier (an EBS volume migration). **Asynchronous** (202).

- **Body:** `new_tier`, one of `ladybug-standard`, `ladybug-large`, `ladybug-xlarge`.

```bash
curl -s -X POST "http://localhost:8000/v1/graphs/kg1a2b3c4d5/operations/change-tier" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"new_tier": "ladybug-large"}'
```

### materialize

Rebuilds the analytical graph (LadybugDB) from OLTP or staged source data. **Asynchronous** (202), except a dry run which runs **synchronously** — it returns a `completed` envelope reporting what it would do, without executing.

- **Body:** `source` (`staged` or `extensions`), `rebuild`, `force`, `ignore_errors` (default `true`), `dry_run`, `materialize_embeddings`.

```bash
# Materialize an entity graph from the extensions OLTP database
curl -s -X POST "http://localhost:8000/v1/graphs/kg1a2b3c4d5/operations/materialize" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{"source": "extensions", "rebuild": false}'
```

```bash
# Dry run — synchronous, returns a completed envelope with the plan, materializes nothing
curl -s -X POST "http://localhost:8000/v1/graphs/kg1a2b3c4d5/operations/materialize" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"dry_run": true}'
```

After a materialize completes, the rebuilt graph is queryable — see [Querying the Analytical Graph](Querying-the-Analytical-Graph).

### delete-graph and change-reporting-style

Two further operations share the surface:

- **`delete-graph`** (async, 202) — deletes the graph itself. Requires `confirm` in the body to equal the `graph_id`, and the caller must be both an org **owner** and a graph **admin**.
- **`change-reporting-style`** (sync, 200) — sets the active reporting style for the graph by `reporting_style_id`.

## Worked Example: Create a Backup and Watch It Complete

This walks the full async path end to end: kick off a backup, monitor it over SSE, and confirm it landed via the read side.

### Step 1: Kick Off the Backup

Backups are asynchronous, so this returns HTTP 202 with a `pending` envelope and an `operation_id`.

```bash
curl -s -X POST "http://localhost:8000/v1/graphs/kg1a2b3c4d5/operations/create-backup" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Idempotency-Key: backup-2026-06-11-001" \
  -H "Content-Type: application/json" \
  -d '{"backup_format": "full_dump", "retention_days": 30, "encryption": true}'
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
curl -N "http://localhost:8000/v1/operations/op_01J9ZK7M3QABCDEF.../stream" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

The stream emits `operation_started`, then one or more `operation_progress` events, then a terminal `operation_completed` (or `operation_error` if something failed). The completion event carries the result data.

### Step 3: Or Take a Snapshot Instead

If you would rather poll than hold a connection open, hit `/status` for a point-in-time view:

```bash
curl -s "http://localhost:8000/v1/operations/op_01J9ZK7M3QABCDEF.../status" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

### Step 4: Confirm the Backup Landed

The read side is a plain `GET` — no envelope, no operation. List the graph's backups to see the new one:

```bash
curl -s "http://localhost:8000/v1/graphs/kg1a2b3c4d5/backups" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

Because you sent an `Idempotency-Key` in Step 1, re-running that exact request within 24 hours replays the original envelope (`idempotentReplay: true`) instead of creating a second backup.

## Common Pitfalls

### Shared Repositories Reject These Operations

`create-backup`, `restore-backup`, `change-tier`, `delete-subgraph`, and `delete-graph` all return **403** on shared repositories such as `sec`. Run lifecycle operations against a graph you own.

### restore-backup Is Forbidden on Entity Graphs

Calling `restore-backup` on an entity graph returns **400** telling you to use `materialize` instead. Entity graphs are rebuilt from the extensions OLTP database, not restored from a dump.

### A Backup Is Either Downloadable or Restorable — Not Both

Only **encrypted** backups can be restored, and **encrypted** backups cannot be downloaded. Decide up front: set `encryption: true` for disaster recovery, or leave it `false` if you need to pull the dump down.

### create-backup Only Supports full_dump

Any `backup_format` other than `full_dump` returns **400**.

### Retention Is Capped, Not Rejected

If `retention_days` exceeds your tier's maximum, it is silently **clamped** to the cap rather than rejected. Check the stored value if it matters.

### Reusing an Idempotency-Key With a Changed Body Is a 409

The key is bound to the first body it saw. Change the body and you get a **409 Conflict**, not a new operation. Use a fresh key when the request changes.

### delete-graph Requires Explicit Confirmation and Elevated Roles

`delete-graph` returns **400** unless the `confirm` field equals the `graph_id`, and the caller must be both an org **owner** and a graph **admin**. This is deliberate friction on an irreversible action.

### compression Must Be true

The backup request model rejects `compression: false`. Omit it (it defaults to and is forced to `true`) rather than trying to turn it off.

## Related Documentation

**Wiki Guides:**

- **[Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy)** - What `graph_id`, subgraphs, tiers, and shared repositories are — the things these operations act on
- **[Querying the Analytical Graph](Querying-the-Analytical-Graph)** - Query the LadybugDB graph after a `materialize` rebuilds it
- **[RoboLedger Operations](RoboLedger-Operations)** - The extensions analogue of this surface (`POST /extensions/roboledger/{g}/operations/{op}`), using the same `OperationEnvelope` and `Idempotency-Key` contract

**Codebase Documentation:**

- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration in codebase
- **[Graph Routing Middleware](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/graph/README.md)** - Multi-tenant graph routing in codebase

**API Reference:**

- **[API Documentation](https://api.robosystems.ai/docs)** - Full request/response models and status codes (machine-readable OpenAPI spec)
- **RoboSystems API (local)**: http://localhost:8000/docs (when running locally)

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
