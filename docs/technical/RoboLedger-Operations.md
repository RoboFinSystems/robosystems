# RoboLedger Operations

This guide shows you how to drive RoboLedger's command-write and analytical-view surface — the graph-scoped operations at `POST /extensions/roboledger/{graph_id}/operations/{op_name}`. Command writes mutate the ledger (update the entity, close a period, generate a report); analytical views query the XBRL hypercube to build pivot tables. Both share one response contract and live under the same URL prefix.

**Quick Start:** Run `just demo-roboledger` to provision a fully-populated tenant, then `curl` any operation below using the API key from `.local/config.json`.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [The Operation Envelope Contract](#the-operation-envelope-contract)
- [Command Writes](#command-writes)
- [Worked Example: close-period](#worked-example-close-period)
- [Analytical View Operations](#analytical-view-operations)
- [Worked Example: build-fact-grid](#worked-example-build-fact-grid)
- [Financial Statement Analysis](#financial-statement-analysis)
- [Report Bundle Downloads](#report-bundle-downloads)
- [Gotchas and Pitfalls](#gotchas-and-pitfalls)
- [Related Documentation](#related-documentation)

## Overview

RoboLedger's extensions surface follows a CQRS split with three sub-surfaces, all graph-scoped at the URL level:

| Sub-surface | Endpoint | Purpose |
| --- | --- | --- |
| **Typed reads** | `POST /extensions/{graph_id}/graphql` | Strawberry GraphQL — entity, fiscal calendar, mappings, reports. See [GraphQL Reads](GraphQL-Reads). |
| **Command writes** | `POST /extensions/roboledger/{graph_id}/operations/{op_name}` | Mutations — close a period, update the entity, generate a report. |
| **Analytical views** | `POST /extensions/roboledger/{graph_id}/operations/{view_name}` | Read-only pivots over the XBRL hypercube (`build-fact-grid`). |

This page covers the latter two. The `graph_id` always comes from the URL path — FastAPI dependencies validate authentication and per-graph access before the handler runs. The reads sibling is documented in [GraphQL Reads](GraphQL-Reads), and the surface as a whole in [Extensions Surface Overview](Extensions-Surface-Overview).

Command writes and analytical views share one response shape — the `OperationEnvelope` — and accept an `Idempotency-Key` header. Most operations complete synchronously; a few dispatch to a background worker and stream progress over Server-Sent Events.

## Prerequisites

- A running local stack (`just start`).
- A graph with an initialized RoboLedger ledger. The fastest path is the [RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough), which provisions a tenant with 16 months of synthetic books in ~60 seconds.
- An API key and graph id in `.local/config.json` (written by `just demo-user` or `just demo-roboledger`).
- `ROBOLEDGER_ENABLED=true` gates the command-write operations. The analytical view router is gated independently by `FACT_GRID_ENABLED`, so deployments serving only the SEC shared repository can mount `build-fact-grid` without enabling RoboLedger tenants.

## Quick Start

Read the credentials from `.local/config.json` once, then reuse them across calls:

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=$(jq -r '.graphs[].graph_id' .local/config.json | head -1)
```

Every example below uses `http://localhost:8000` and the `X-API-Key` header. The full endpoint and schema reference — request bodies, response models, every field — lives in the live OpenAPI spec at [http://localhost:8000/docs](http://localhost:8000/docs) (or [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs)). This page carries the concepts and the worked examples; it does not re-tabulate the endpoint surface.

## The Operation Envelope Contract

Every command write and analytical view returns the same envelope. This uniformity is what lets a client treat all writes identically — parse one shape, branch on `status`, and follow up via `operation_id` if needed.

| Field | Wire alias | Meaning |
| --- | --- | --- |
| `operation` | `operation` | The kebab-case operation name (e.g. `close-period`). |
| `operation_id` | `operationId` | An `op_`-prefixed ULID identifying this execution. |
| `status` | `status` | `completed`, `pending`, or `failed`. |
| `result` | `result` | The typed payload (operation-specific) or `null`. |
| `at` | `at` | ISO-8601 UTC timestamp. |
| `created_by` | `createdBy` | The acting user id. |
| `idempotent_replay` | `idempotentReplay` | `true` when this is a cached replay, not a fresh execution. |

The envelope is defined in [`middleware/operations.py`](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/operations.py). Wire aliases are camelCase (`operationId`, `createdBy`, `idempotentReplay`), and the model is `populate_by_name=True`, so both spellings are accepted on input.

**Idempotency-Key.** Send an `Idempotency-Key` header to make a write safe to retry. A replay with the same key returns the cached envelope (with `idempotentReplay: true`) without re-executing. Reusing a key with a *different* request body returns `409` — the body fingerprint must match, so the cache never silently overwrites a different operation.

**Synchronous vs async.** Most operations run synchronously and return `200` with `status: completed`. A few — notably `auto-map-elements` — dispatch to the background worker, return `202` with `status: pending`, and stream progress. The `operation_id` bridges to the monitoring surface: subscribe to `GET /v1/operations/{operation_id}/stream` (SSE) to follow a long-running operation to completion.

## Command Writes

Command writes are the mutation half of the surface. The pattern is uniform: `POST` to `/extensions/roboledger/{graph_id}/operations/{op_name}` with a JSON body, get back an `OperationEnvelope`. The handlers are thin — they validate, delegate to the operations kernel, and wrap the result in the envelope.

The registered command-write operations, grouped by workflow stage (from [`routers/extensions/roboledger/operations.py`](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/routers/extensions/roboledger/operations.py)):

| Stage | Operations |
| --- | --- |
| **Setup** | `initialize`, `update-entity` |
| **Taxonomy Blocks** | `create-taxonomy-block`, `update-taxonomy-block`, `delete-taxonomy-block`, `link-entity-taxonomy` |
| **Mapping** | `create-mapping-association`, `delete-mapping-association`, `auto-map-elements` |
| **Information Blocks** | `create-information-block`, `update-information-block`, `delete-information-block`, `evaluate-rules` |
| **Agents** | `create-agent`, `update-agent` |
| **Event Blocks** | `create-event-block`, `update-event-block`, `execute-event-block`, `preview-event-block` |
| **Event Handlers** | `create-event-handler`, `update-event-handler` |
| **Journal Entries** | `update-journal-entry`, `delete-journal-entry` |
| **Schedules** | `promote-obligations` |
| **Close Workflow** | `set-close-target`, `close-period`, `reopen-period` |
| **Reports** | `create-report`, `regenerate-report`, `delete-report`, `share-report`, `file-report`, `transition-filing-status` |
| **Publish Lists** | `create-publish-list`, `update-publish-list`, `delete-publish-list`, `add-publish-list-members`, `remove-publish-list-member` |

`auto-map-elements` is the async one (returns `202 pending`); the rest complete synchronously. The exact request body for each is in the OpenAPI spec — the worked examples below show the two most common shapes.

A second example, `update-entity`, illustrates the partial-update convention. Only the non-null fields in the body are applied; an empty body returns `400 "No fields provided for update."`

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/update-entity" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "legal_name": "Cascade Advisory Group, LLC",
    "tax_id": "12-3456789",
    "state_of_incorporation": "WA",
    "fiscal_year_end": "12-31",
    "entity_type": "llc"
  }'
```

## Worked Example: close-period

The close workflow is the most product-distinctive command path. The fiscal calendar tracks two cursors:

- `closed_through` — the **system** boundary, the last period actually locked.
- `close_target` — the **user-set** intent, the period you are working toward.

Setting a target closes nothing. `close-period` does the locking, and it works **sequentially**: the `period` you pass must equal `closed_through + 1`. The `period` value matches the regex `^\d{4}-(0[1-9]|1[0-2])$` (e.g. `2026-03`).

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/close-period" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: close-2026-03-$(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{"period": "2026-03", "allow_stale_sync": false}'
```

On success, the envelope's `result` is a close summary:

```json
{
  "operation": "close-period",
  "operationId": "op_01HVF8T0M2YTAY3BBNRH0V0",
  "status": "completed",
  "result": {
    "fiscal_calendar": {
      "graph_id": "...",
      "closed_through": "2026-03",
      "close_target": "2026-03",
      "...": "..."
    },
    "period": "2026-03",
    "entries_posted": 3,
    "target_auto_advanced": true,
    "rule_summary": {"pass": 38, "fail": 0, "error": 0, "skipped": 0},
    "evaluated_structure_ids": ["..."]
  },
  "at": "2026-06-11T00:00:00Z",
  "createdBy": "user_...",
  "idempotentReplay": false
}
```

**Blockers are structured.** When a period cannot close, `close-period` returns `422` with a structured `blockers` array — not a flat message. Clients must parse `detail.blockers`:

```json
{
  "detail": {
    "message": "Cannot close period '2026-03'.",
    "blockers": ["sync_stale"],
    "sync_stale_days": 5
  }
}
```

The verified blocker codes are `sequence_violation`, `period_incomplete`, `sync_stale`, `calendar_not_initialized`, `period_already_closed`, and `pending_obligations`. Some carry extra detail fields: `pending_obligation_count`, `pending_obligation_sample[]`, `earliest_pending_period`, and `sync_stale_days`. The `sync_stale` blocker is overridable with `allow_stale_sync: true` *after you have manually verified the source data is current*.

The same `closed_through` / `close_target` cursors surface as a read on the GraphQL `fiscalCalendar` query, which also exposes `gap_periods`, `catch_up_sequence`, and `closeable_now`. See [GraphQL Reads](GraphQL-Reads) for the read side of the close workflow.

## Analytical View Operations

Analytical view operations are read-only queries over the **XBRL hypercube** — the `Fact` nodes materialized into the LadybugDB graph. They share the `OperationEnvelope` contract with command writes and live under the same `/operations/` prefix, but they mutate nothing. They are the bridge from accounting facts to a tabular, pivot-ready shape.

`build-fact-grid` is the primary view operation. It takes a set of concept filters and period filters, walks the matching `Fact` nodes, deduplicates them, and projects the result into a pivot table according to a `view_config`. The same logic is exposed as the MCP tool `build-fact-grid` for agent-driven use.

Two things make this router distinct from the command writes:

- **It is gated independently.** The view router checks `FACT_GRID_ENABLED`, not `ROBOLEDGER_ENABLED`. A deployment that serves only the SEC shared repository can mount fact-grid without provisioning any RoboLedger tenant.
- **It reads the OLAP graph, not the OLTP database.** Tenant data only appears in the hypercube *after materialization* — a freshly-written ledger will not surface in a fact grid until the blue/green materialization runs. The SEC shared repository is always materialized.

The request body is a `CreateViewRequest` (defined in [`models/api/views/view_config.py`](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/api/views/view_config.py)). The two filter axes you must satisfy:

- **Concept filter** — `elements[]` (XBRL qnames like `us-gaap:Assets`) or `canonical_concepts[]` (semantic names like `revenue`, `net_income`, which match every mapped qname for that concept).
- **Period filter** — `periods[]` (`YYYY-MM-DD`), or `period_type` (`annual` / `quarterly` / `instant`), or `fiscal_year`.

You must provide at least one concept filter **and** at least one period filter, or the request returns `400`. Other optional fields scope the query further: `entity`, `entities[]`, `form`, `fiscal_period` (`FY`, `Q1`, …), `include_summary`, and `view_config`.

## Worked Example: build-fact-grid

A balance-sheet rollup against a tenant graph, scoped to three concepts at one instant:

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/build-fact-grid" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "elements": ["us-gaap:Assets", "us-gaap:Liabilities", "us-gaap:StockholdersEquity"],
    "periods": ["2026-03-31"],
    "period_type": "instant",
    "include_summary": true
  }'
```

The envelope's `result` is a `ViewResponse`:

```json
{
  "metadata": {
    "view_id": "...",
    "facts_processed": 3,
    "construction_time_ms": 42,
    "source": "..."
  },
  "presentations": {
    "pivot_table": {
      "index": ["..."],
      "columns": ["..."],
      "data": [["..."]],
      "metadata": {"...": "..."}
    },
    "summary": {"...": "..."}
  }
}
```

`presentations` always contains `pivot_table`; it also contains `summary` when `include_summary: true`.

The `view_config` block gives you control over the pivot layout — which axis is rows, which is columns, how members are labeled, and the aggregation function. This example pivots revenue across NVIDIA and AMD by fiscal year against the SEC shared repository (replace `$GRAPH_ID` with the SEC shared-repository graph id for your deployment):

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/build-fact-grid" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "canonical_concepts": ["revenue"],
    "entities": ["NVDA", "AMD"],
    "form": "10-K",
    "fiscal_period": "FY",
    "view_config": {
      "rows": [{"type": "entity"}],
      "columns": [{"type": "period",
                   "selected_members": ["2022-12-31", "2023-12-31", "2024-12-31"],
                   "member_labels": {"2022-12-31": "FY22", "2023-12-31": "FY23", "2024-12-31": "FY24"}}],
      "values": "numeric_value",
      "aggregation_function": "sum"
    }
  }'
```

A `ViewConfig` has `rows[]` and `columns[]` of axis configs (`type` is one of `element`, `period`, `dimension`, `entity`), a `values` source (default `numeric_value`), an `aggregation_function` (`sum`, `average`, `count`), and an optional `fill_value`. The full schema is in [`models/api/views/`](https://github.com/RoboFinSystems/robosystems/tree/main/robosystems/models/api/views).

## Financial Statement Analysis

`financial-statement-analysis` is the second view operation. It renders a complete financial statement from the hypercube rather than returning a free-form pivot. The body takes a `statement_type` — one of `income_statement`, `balance_sheet`, `cash_flow_statement`, or `equity_statement` — plus either a `report_id` (for tenant graphs) or a `ticker` (for shared-repository graphs).

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/financial-statement-analysis" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"statement_type": "balance_sheet", "report_id": "..."}'
```

How statements are rendered from the hypercube — the structures, the calc rollup, the footing — is the subject of [Reporting and Rendering](Reporting-and-Rendering).

## Report Bundle Downloads

Downloading a generated report bundle is a plain REST `GET`, **not** an operation. There is no envelope and no idempotency key — you are fetching an artifact, not running a command.

```bash
# JSON-LD: returns a JSON envelope wrapping a presigned download URL
curl "http://localhost:8000/extensions/roboledger/$GRAPH_ID/reports/$REPORT_ID/download?format=jsonld" \
  -H "X-API-Key: $API_KEY"

# XBRL 2.1: streams a zip directly
curl "http://localhost:8000/extensions/roboledger/$GRAPH_ID/reports/$REPORT_ID/download?format=xbrl-2.1" \
  -H "X-API-Key: $API_KEY" -o report.zip
```

`format=jsonld` returns a presigned-URL JSON envelope; `format=xbrl-2.1` streams a zip. A report generated before the bundle-serialization feature has no stamped artifact and returns `404`.

Generating, regenerating, and filing reports are command writes (`create-report`, `regenerate-report`, `file-report`, `transition-filing-status`) — the download endpoint only fetches an already-produced bundle.

## Gotchas and Pitfalls

### Sequential Close Only

`close-period` requires `period == closed_through + 1`. Closing out of order returns `422` with `blockers: ["sequence_violation"]`. To advance multiple periods, close them one at a time in order.

### "Ledger not initialized" 404

Any operation against a graph that has no extensions schema returns `404 "Ledger not initialized. Connect a data source first."` Call `initialize` (or connect a data source) before any other RoboLedger operation.

### update-entity With an Empty Body

`update-entity` applies only the non-null fields in the body. An empty body returns `400 "No fields provided for update."`

### close-period Blockers Are Structured

Do not parse a flat error string — read `detail.blockers`. `sync_stale` is overridable with `allow_stale_sync: true` only after you have manually verified the source data is current. A `WRITE_BACK_FAILED` failure carries a `failed_events[]` array for diagnosis.

### Fact Grid Needs Both a Concept and a Period Filter

`build-fact-grid` enforces two guards: it `400`s if you omit the concept filter (`elements` or `canonical_concepts`) and `400`s if you omit the period filter (`periods`, `period_type`, or `fiscal_year`). Always send at least one of each.

### Fact Grid Only Sees Materialized Data

A tenant ledger must be materialized into LadybugDB before its facts appear in a grid. A ledger you just wrote will not surface until the blue/green materialization runs. The SEC shared repository is always materialized.

### Posted Entries Are Immutable

`update-journal-entry` and `delete-journal-entry` operate on **drafts** only. To correct a posted entry, record a reversal via `create-event-block` with `event_type='journal_entry_reversed'`. Library-seeded mapping associations are likewise immutable — deleting one returns `403`.

### Idempotency-Key Reuse With a Changed Body

Reusing an `Idempotency-Key` with a different request body returns `409`, not a silent overwrite. Use a fresh key per distinct operation.

## Related Documentation

**Wiki Guides:**

- **[Extensions Surface Overview](Extensions-Surface-Overview)** - The full RoboLedger / RoboInvestor extensions surface and CQRS split
- **[GraphQL Reads](GraphQL-Reads)** - The typed-read sibling surface (`POST /extensions/{graph_id}/graphql`)
- **[Reporting and Rendering](Reporting-and-Rendering)** - How statements are rendered and footed from the hypercube
- **[RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough)** - See the operations surface end-to-end against a populated tenant
- **[Connecting QuickBooks Locally](Connecting-QuickBooks-Locally)** - Layer real synced data under the same workflow

**API Reference:**

- **[API Documentation](https://api.robosystems.ai/docs)** - Live OpenAPI spec with every request body and response model
- **RoboSystems API (local)**: http://localhost:8000/docs

**Codebase Documentation:**

- **[RoboLedger operations router](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/routers/extensions/roboledger/operations.py)** - The registered command-write operations
- **[Analytical view router](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/routers/extensions/roboledger/views.py)** - `build-fact-grid` and `financial-statement-analysis`
- **[Operation envelope](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/operations.py)** - `OperationEnvelope`, idempotency, audit
- **[Operations kernel](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business logic patterns behind every operation

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
