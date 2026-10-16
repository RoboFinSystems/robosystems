# Operations Contract

Graph and extension writes in RoboSystems are named operations sharing one contract: one URL shape, response envelope, retry key, and progress stream. This page is the one home for that contract. The pages for each surface — [Graph Operations](Graph-Operations.md), [RoboLedger Operations](RoboLedger-Operations.md), [RoboInvestor Operations](RoboInvestor-Operations.md) — cover what each operation does and link back here for how they all behave. A few writes sit outside it as plain REST calls: account-level ones such as API keys, organization members, and billing, and graph-level ones such as membership grants and connection management.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development.md).

## Table of Contents

- [Where Operations Live](#where-operations-live)
- [The OperationEnvelope](#the-operationenvelope)
- [Sync and Async](#sync-and-async)
- [When an Operation Fails](#when-an-operation-fails)
- [Idempotency-Key](#idempotency-key)
- [Following an Async Operation](#following-an-async-operation)
- [Client Checklist](#client-checklist)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Where Operations Live

Two families of operations share the contract:

```text
POST /v1/graphs/{graph_id}/operations/{operation_name}                  # graph lifecycle and content
POST /extensions/{roboledger|roboinvestor}/{graph_id}/operations/{operation_name}   # extension domains
```

The first carries graph lifecycle and content writes (`create-subgraph`, `create-backup`, `materialize`, `create-file-upload`, `ingest-file`, and the rest). The second carries the RoboLedger and RoboInvestor command writes (`create-event-block`, `close-period`, `create-security`) and the analytical view operations (`build-fact-grid`, `financial-statement-analysis`), which are read-only but ride the same envelope.

In both, `graph_id` is a URL path parameter, authentication is the `X-API-Key` header, and access to the graph is checked before the operation runs. The body is JSON. Reads are not operations: they are REST `GET`s under `/v1` or GraphQL at `/extensions/{graph_id}/graphql`.

The per-operation request and response models are in the [API reference](https://robosystems.ai/docs/api).

## The OperationEnvelope

Every operation that succeeds returns an `OperationEnvelope`. On the wire its field names are **camelCase**:

| Field | Type | Meaning |
| --- | --- | --- |
| **`operation`** | string | The kebab-case operation name, e.g. `create-event-block` |
| **`operationId`** | string | `op_` followed by a 26-character ULID. The correlation key for the progress stream, the status snapshot, and the audit log |
| **`status`** | string | `completed`, `pending`, or `failed` |
| **`result`** | object, list, or null | The operation's payload. Its shape is per operation and is typed in the API reference |
| **`at`** | string | ISO-8601 UTC timestamp with a `Z` suffix, to the second |
| **`createdBy`** | string or null | The id of the user whose credentials made the call |
| **`idempotentReplay`** | boolean | `true` when this envelope came from the idempotency cache and the operation did **not** run again |

The payload inside `result` keeps the operation's own field names, which are snake_case (`external_id`, `posting_date`). Only the envelope's own keys are camelCase.

A synchronous success:

```json
{
  "operation": "create-event-block",
  "operationId": "op_01K5Q3Z8W4N2B7C1D9E6F0G3H5",
  "status": "completed",
  "result": {
    "id": "evt_01K5Q3Z8X2M4P6R8T0V2W4Y6Z8",
    "event_type": "invoice_issued",
    "status": "captured",
    "source": "driftline",
    "external_id": "drf_inv_10042",
    "amount": 480000,
    "currency": "USD"
  },
  "at": "2026-09-22T14:05:11Z",
  "createdBy": "user_abc123",
  "idempotentReplay": false
}
```

(The `result` above is trimmed; an event block carries more fields.)

## Sync and Async

Each operation either finishes inside the request or hands its work to a background worker.

- **Synchronous** operations return `status: "completed"` with the output in `result`, normally over **HTTP 200**.
- **Asynchronous** operations return `status: "pending"`, normally over **HTTP 202**. `result` carries what the operation knows at hand-off — an acknowledgement message and, for some operations, a `monitoring.sse_endpoint` — and `operationId` is the handle you follow.

A pending envelope:

```json
{
  "operation": "create-backup",
  "operationId": "op_01K5Q41B7C9D2E4F6G8H0J2K4M",
  "status": "pending",
  "result": {
    "status": "accepted",
    "message": "Backup creation started",
    "retention_days": 30,
    "monitoring": { "sse_endpoint": "/v1/operations/op_01K5Q41B7C9D2E4F6G8H0J2K4M/stream" }
  },
  "at": "2026-09-22T14:07:40Z",
  "createdBy": "user_abc123",
  "idempotentReplay": false
}
```

**Branch on the envelope's `status`, not on the HTTP code.** The two usually agree, but not always: a `materialize` dry run returns a `completed` envelope under the route's 202, and `create-subgraph` with `fork_parent: true` returns a `pending` envelope under 200. Some operations are sync or async depending on the request; the envelope always tells you which one you got.

## When an Operation Fails

`failed` is a legal envelope status, but over REST you meet failure in two other shapes, depending on when it happens.

**Refused before or during a synchronous run** — validation, access, a domain rule, a conflict. The response is an HTTP error with the platform's standard error body, not an envelope:

```json
{
  "detail": "Cannot write to closed period '2026-08' (posting_date=2026-08-14). Reopen the period first if an adjustment is needed.",
  "request_id": "7f3c9a1e-2b4d-4c6e-8f0a-1b2c3d4e5f60"
}
```

Nothing was written: a synchronous operation that raises rolls back its whole unit of work. The status codes, the `detail` shapes, and what to do about each are in [Errors and Rate Limits](Errors-and-Rate-Limits.md).

**Failed after an async hand-off.** The request already returned a `pending` envelope, so the failure arrives on the operation's progress stream as an `operation_error` event, and the status snapshot reports `"status": "failed"` with an `error` message — see [Following an Async Operation](#following-an-async-operation).

## Idempotency-Key

Every operation accepts an optional `Idempotency-Key` header. Without one, a retried POST runs the operation again. With one, the platform recognises the retry:

| Situation | Response |
| --- | --- |
| First request with this key | The operation runs; its envelope is stored under the key |
| Same key, identical body, after the first finished | The stored envelope, with `idempotentReplay: true`. The operation does not run again |
| Same key, identical body, while the first is still running | **409 Conflict** — the first request is in progress; retry after it completes to receive its result. The key is held this way for at most **5 minutes**: if the first request has neither finished nor failed by then, a retry with the key runs the operation again |
| Same key, **different** body | **409 Conflict** — the key is bound to the first body it saw. Use a fresh key for a different request |
| First request failed (an HTTP error) | Nothing is stored; a retry with the same key runs the operation again |
| First request returned `pending` and the background work later failed or was cancelled | The stored envelope is evicted, so a retry with the same key dispatches again rather than replaying `pending` |

The details that matter when you design retries:

- **Window.** A stored envelope is kept for **24 hours**. Before there is an envelope to store, the key is reserved for the first request for up to 5 minutes, a window sized to outlast the slowest synchronous operation (async operations answer at hand-off).
- **Scope.** A key is scoped to the calling user, the graph, and the operation name. The same key string used by two users, on two graphs, or for two operations never collides.
- **Body identity.** The comparison is over the parsed JSON body, so key order and whitespace do not matter; any change of value does.
- **What a replay returns.** The original envelope, including its original `operationId`, `at`, and `result`, with `idempotentReplay` set to `true`. For an async operation the replay is the original `pending` envelope — follow its `operationId` for the outcome.

```bash
# Safe to re-run: the second call replays the first envelope instead of creating a second backup
curl -s -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/operations/create-backup" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: nightly-export-2026-09-22" \
  -H "Content-Type: application/json" \
  -d '{"backup_format": "full_dump", "retention_days": 30}'
```

A good key names the intent of the request — a source record id plus a version, or a job id plus a date — so the retry after a network error carries the same key and a genuinely new request carries a new one.

`Idempotency-Key` is separate from any idempotency an operation has in its own data. `create-event-block`, for example, also refuses a second event with the same `source` and `external_id`, with or without the header — see [Build a Ledger Integration](Build-a-Ledger-Integration.md#step-5-capture-the-event).

## Following an Async Operation

A pending envelope's `operationId` works with these endpoints, all under `/v1`:

```text
GET    /v1/operations/{operation_id}/stream    # Server-Sent Events (text/event-stream)
GET    /v1/operations/{operation_id}/status    # point-in-time JSON snapshot
DELETE /v1/operations/{operation_id}           # cancel a pending, running, or paused operation
POST   /v1/operations/{operation_id}/resume    # answer an operation paused for input
```

All are authenticated with `X-API-Key`, answer only for operations you started, and consume no credits. They carry no graph id, so they take an account-wide key: a graph-scoped key is refused here with 401 (see [Choosing a Key Scope](Authentication-and-API-Keys.md#choosing-a-key-scope)). The stream endpoint allows a small number of concurrent connections per user (the limit is in its description in the [API reference](https://robosystems.ai/docs/api)); close streams you are done with.

### The stream

```bash
curl -N "https://api.robosystems.ai/v1/operations/op_01K5Q41B7C9D2E4F6G8H0J2K4M/stream" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

(`-N` turns off curl's buffering so events print as they arrive.)

Each event is a standard SSE frame: an `event:` name and a single-line JSON `data:` payload. Operation events carry `operation_id`, `timestamp`, and a `sequence_number` alongside their own fields:

```text
event: connected
data: {"operation_id": "op_01K5Q41B7C9D2E4F6G8H0J2K4M", "connection_id": "0b6e…", "from_sequence": 0}

event: operation_progress
data: {"operation_id": "op_01K5Q41B7C9D2E4F6G8H0J2K4M", "timestamp": "2026-09-22T14:07:52Z", "sequence_number": 3, "message": "Exporting database", "progress_percent": 40}

event: operation_completed
data: {"operation_id": "op_01K5Q41B7C9D2E4F6G8H0J2K4M", "timestamp": "2026-09-22T14:09:03Z", "sequence_number": 6, "message": "Operation completed successfully", "result": {"backup_id": "bk_…"}}

event: stream_end
data: {"status": "completed", "operation_id": "op_01K5Q41B7C9D2E4F6G8H0J2K4M", "message": "Stream ended normally"}
```

The operation events:

| Event | Meaning | Payload fields beyond the common three |
| --- | --- | --- |
| `operation_started` | A worker picked the operation up | `message`, `progress_percent` (0) |
| `operation_progress` | An incremental update | `message`, `progress_percent` (0–100, may be null), plus operation-specific detail |
| `operation_completed` | Finished; terminal | `message`, `result` |
| `operation_error` | Failed; terminal | `error`, `error_details` |
| `operation_cancelled` | Cancelled; terminal | — |
| `operation_awaiting_input` | Paused at a checkpoint for a decision; ends the stream until resumed | — |
| `operation_resumed` | A paused operation was resumed | — |

The stream also emits framing events: `connected` when it opens, `keepalive` during quiet periods, `stream_end` after a terminal event, and `error` if it cannot carry on once open (for example, too many concurrent streams). An unknown or expired operation id answers HTTP **404**, and someone else's operation **403**, before any stream opens. A stream opened on an operation that has already finished replays its whole history and then ends.

**Reconnecting.** Events are stored with their sequence numbers for a limited time, so a dropped connection loses nothing. Reconnect with `from_sequence` set to one more than the last `sequence_number` you saw; the stream replays everything from that number (inclusive) before going live:

```bash
curl -N "https://api.robosystems.ai/v1/operations/op_01K5Q41B7C9D2E4F6G8H0J2K4M/stream?from_sequence=4" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

With the default `from_sequence=0`, a stream on a running operation shows only what happens from now on; pass `from_sequence=1` to replay from the start.

### The status snapshot

If you would rather poll:

```bash
curl -s "https://api.robosystems.ai/v1/operations/op_01K5Q52D4F6H8J0K2M4N6P8R0S/status" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

```json
{
  "operation_id": "op_01K5Q52D4F6H8J0K2M4N6P8R0S",
  "operation_type": "extensions_materialize",
  "status": "failed",
  "created_at": "2026-09-22T14:07:40Z",
  "updated_at": "2026-09-22T14:08:15Z",
  "graph_id": "kg1a2b3c4d5e6f7890",
  "error": "…",
  "message": "Operation execution failed",
  "_links": { "stream": "/v1/operations/op_01K5Q52D4F6H8J0K2M4N6P8R0S/stream" }
}
```

`operation_type` names the background task that ran the work, which is not always the operation's own name.

`status` is one of `pending`, `running`, `awaiting_input`, `completed`, `failed`, or `cancelled`. A completed operation carries `result`; a failed one carries `error`. While the operation is `pending` the snapshot also reports `queue_position` and `queue_depth`, and while it can still be stopped `_links` includes `cancel` — `DELETE /v1/operations/{operation_id}` cancels a pending, running, or paused operation.

**Resuming a paused operation.** An operation in `awaiting_input` has stopped at a checkpoint for a decision. Its snapshot carries `input_request` (`prompt`, `details`, `requested_at`) and a `resume` link. Answer with the decision in the shape the prompt describes:

```bash
curl -s -X POST "https://api.robosystems.ai/v1/operations/op_01K5Q52D4F6H8J0K2M4N6P8R0S/resume" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"input": {"approved": true}}'
```

The response is **202**: the operation keeps its id and goes back on the worker queue, so reconnect to its stream to follow it. Resuming an operation that is not waiting for input answers **409**.

Once an operation's records expire, both endpoints answer **404**. Read the outcome while it is fresh, and treat the resource the operation produced (the backup, the materialized graph) as the durable record.

## Client Checklist

- Send an `Idempotency-Key` on every write you might retry, and reuse it only for the same request.
- Branch on `status` in the envelope: `completed` → read `result`; `pending` → follow `operationId`.
- Treat a 409 whose `detail` says the key is in progress as "wait and retry the same request", and one that says the key was reused with a different body as a bug in your key choice.
- On a non-2xx response, log the `request_id` from the body (also sent as the `X-Request-ID` header) — see [Errors and Rate Limits](Errors-and-Rate-Limits.md#quoting-a-request-id).
- For async work, prefer the stream; fall back to polling `/status` with a delay; stop at a terminal state. Follow operations with an account-wide key.

## Self-hosted deployments

The contract is identical on a local stack at `http://localhost:8000`. Idempotency records and operation events live in the deployment's Valkey, so flushing its caches (for example with `just admin dev cache flush`) forgets stored envelopes and progress history. The `Idempotency-Key` behaviour is best-effort by design: if the cache is unreachable, operations still run rather than fail.

## Related Documentation

**Guides:**

- **[Errors and Rate Limits](Errors-and-Rate-Limits.md)** - The error body, status codes, rate-limit headers, and insufficient-credits responses
- **[Graph Operations](Graph-Operations.md)** - The graph lifecycle and content operations
- **[Extensions Surface Overview](Extensions-Surface-Overview.md)** - The `/extensions` surface these operations share with GraphQL reads
- **[Build a Ledger Integration](Build-a-Ledger-Integration.md)** - The contract in use, end to end
- **[Versioning and Compatibility](Versioning-and-Compatibility.md)** - What stays stable as the API evolves

**API Reference:**

- **[API reference](https://robosystems.ai/docs/api)** - Every operation's request and typed `result` model

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
