# RoboLedger Operations

This guide shows you how to drive RoboLedger's command-write and analytical-view surface — the graph-scoped operations at `POST /extensions/roboledger/{graph_id}/operations/{op_name}`. Command writes mutate the ledger (update the entity, close a period, generate a report); analytical views read facts back out, either from the XBRL hypercube or from the live OLTP ledger. Both share one response contract and live under the same URL prefix.

**Quick Start:** With an API key and a RoboLedger graph, `curl` any operation below against `https://api.robosystems.ai`, sending the key in the `X-API-Key` header.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [The Operation Envelope Contract](#the-operation-envelope-contract)
- [Command Writes](#command-writes)
- [Worked Example: close-period](#worked-example-close-period)
- [Analytical View Operations](#analytical-view-operations)
- [Worked Example: build-fact-grid](#worked-example-build-fact-grid)
- [Financial Statements](#financial-statements)
- [Gotchas and Pitfalls](#gotchas-and-pitfalls)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)

## Overview

RoboLedger's extensions surface follows a CQRS split with three sub-surfaces, all graph-scoped at the URL level:

| Sub-surface | Endpoint | Purpose |
| --- | --- | --- |
| **Typed reads** | `POST /extensions/{graph_id}/graphql` | Strawberry GraphQL — entity, fiscal calendar, mappings, reports. See [GraphQL Reads](GraphQL-Reads). |
| **Command writes** | `POST /extensions/roboledger/{graph_id}/operations/{op_name}` | Mutations — close a period, update the entity, generate a report. |
| **Analytical views** | `POST /extensions/roboledger/{graph_id}/operations/{view_name}` | Read-only fact queries over the XBRL hypercube (`build-fact-grid`, `financial-statement-analysis`) and over the live OLTP ledger (`live-financial-statement`). |

This page covers the latter two. The `graph_id` always comes from the URL path — FastAPI dependencies validate authentication and per-graph access before the handler runs. The reads sibling is documented in [GraphQL Reads](GraphQL-Reads), and the surface as a whole in [Extensions Surface Overview](Extensions-Surface-Overview).

Command writes and analytical views share one response shape — the `OperationEnvelope` — and accept an `Idempotency-Key` header. Most operations complete synchronously; a few dispatch to a background worker and stream progress over Server-Sent Events.

## Prerequisites

- A RoboSystems account and an API key, created in the app under **Settings → API keys**. See [Quick Start](Quick-Start).
- A graph with an initialized RoboLedger ledger. On the hosted platform a RoboLedger graph comes from roboledger.ai when you connect QuickBooks, or from **Create Graph** in the app.
- The SEC examples (`/extensions/roboledger/sec/...`) need a subscription to the SEC repository, from **Repositories** in the app.
- `ROBOLEDGER_ENABLED` gates the command-write operations. The analytical view router is gated independently by `FACT_GRID_ENABLED`, so deployments serving only the SEC shared repository can mount `build-fact-grid` without enabling RoboLedger tenants. On robosystems.ai both are on; a self-hosted deployment sets them in its environment.

## Quick Start

Set your credentials once, then reuse them across calls:

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector
```

Every example below uses `https://api.robosystems.ai` and the `X-API-Key` header. The full endpoint and schema reference — request bodies, response models, every field — lives in the live OpenAPI spec at [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs). This page carries the concepts and the worked examples; it does not re-tabulate the endpoint surface.

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
| **Setup** | `initialize`, `update-entity`, `change-reporting-style` |
| **Taxonomy Blocks** | `create-taxonomy-block`, `update-taxonomy-block`, `delete-taxonomy-block`, `link-entity-taxonomy` |
| **Mapping** | `create-mapping-association`, `delete-mapping-association`, `auto-map-elements` |
| **Information Blocks** | `create-information-block`, `update-information-block`, `delete-information-block`, `bind-text-block`, `evaluate-rules` |
| **Forecasting and Metrics** | `compute-forecast`, `backfill-plan-history`, `compute-metrics`, `assert-metrics` |
| **Agents** | `create-agent`, `update-agent` |
| **Event Blocks** | `create-event-block`, `update-event-block`, `execute-event-block`, `preview-event-block` |
| **Event Handlers** | `create-event-handler`, `update-event-handler` |
| **Journal Entries** | `update-journal-entry`, `delete-journal-entry` |
| **Schedules** | `promote-obligations`, `rebuild-schedule` |
| **Close Workflow** | `set-close-target`, `close-period`, `reopen-period` |
| **Reports** | `create-report`, `regenerate-report`, `delete-report`, `share-report`, `revoke-report-share`, `file-report`, `transition-filing-status` |
| **Publish Lists and Distribution** | `create-publish-list`, `update-publish-list`, `delete-publish-list`, `add-publish-list-members`, `remove-publish-list-member`, `block-source-graph`, `unblock-source-graph` |

`auto-map-elements` is the async one (returns `202 pending`); the rest complete synchronously. The exact request body for each is in the OpenAPI spec — the worked examples below show the two most common shapes.

**Forecasting and metrics** is the arc least self-evident from its operation names. A *forecast block* carries a driver cascade: `compute-forecast` walks it month-by-month forward from the block's base period — lever-driven `rs-driver` rules in dependency order, carry-forward for unmodeled income-statement lines, then calc-DAG subtotals — upserting one scenario income-statement FactSet (plus a working-capital balance-sheet set) per forward month, keyed by the block's `scenario_id` (`NULL` means actuals). `backfill-plan-history` fills the plan's *historical* columns behind the close boundary by running a real reopen → reclose cycle per month; it is chunked, so loop until `remaining_periods` comes back empty. `compute-metrics` derives a period's standing metric FactSet from the entity's most recent persisted report facts, and `assert-metrics` is its observation sibling — externally-observed values (usage counts, marketing numbers, hand-carried figures) written into the same standing-FactSet shape with asserted provenance. Structures carrying Derive rules are compute-owned and reject assertions, so derived and asserted series never share a structure. All four are deterministic and non-AI: they consume no credits.

A second example, `update-entity`, illustrates the partial-update convention. Only the non-null fields in the body are applied; an empty body returns `400 "No fields provided for update."`

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/update-entity" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "legal_name": "Cascade Advisory Group, LLC",
    "tax_id": "12-3456789",
    "state_of_incorporation": "WA",
    "fiscal_year_end": "12-31",
    "entity_type": "llc"
  }'
```

**Downloading a report bundle is a read, not an operation.** Generating, regenerating, and filing reports are command writes (`create-report`, `regenerate-report`, `file-report`, `transition-filing-status`), but fetching the produced artifact is not: the former `GET .../reports/{id}/download` REST resource has been retired. Use the GraphQL field `reportDownloadUrl(reportId:, format:, expiresIn:)` on `POST /extensions/{graph_id}/graphql`. Every flavor — `JSONLD` (the default), `HOLON_JSONLD`, `XBRL_2_1` — resolves to a short-lived presigned S3 URL the client follows directly; the API never streams the bytes. See [GraphQL Reads](GraphQL-Reads).

## Worked Example: close-period

The close workflow is the most product-distinctive command path. The fiscal calendar tracks two cursors:

- `closed_through` — the **system** boundary, the last period actually locked.
- `close_target` — the **user-set** intent, the period you are working toward.

Setting a target closes nothing. `close-period` does the locking, and it works **sequentially**: the `period` you pass must equal `closed_through + 1`. The `period` value matches the regex `^\d{4}-(0[1-9]|1[0-2])$` (e.g. `2026-03`).

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/close-period" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
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
    "entries_published_to_qb": 2,
    "entries_posted_locally": 1,
    "target_auto_advanced": true,
    "rule_summary": {"pass": 38, "fail": 0, "error": 0, "skipped": 0},
    "evaluated_structure_ids": ["..."],
    "statements_stamped": true,
    "statement_stamp_note": null,
    "stamped_statement_sets": {"struct_...": "fs_..."},
    "statement_rule_summary": {"pass": 12, "fail": 0, "error": 0, "skipped": 0}
  },
  "at": "2026-06-11T00:00:00Z",
  "createdBy": "user_...",
  "idempotentReplay": false
}
```

`entries_posted` is the total across both post paths, and `entries_published_to_qb` / `entries_posted_locally` are the split. On a graph whose QuickBooks connection is `qb_authoritative`, the close first publishes eligible RoboLedger-originated drafts back to QuickBooks (each is promoted to posted at publish time); everything else goes through the local bulk transition.

The close also stamps the period's canonical statement FactSets. `stamped_statement_sets` maps `structure_id` → `fact_set_id` for each set minted, and `statement_rule_summary` reports statement-rule verification (distinct from `rule_summary`, which is the schedule-rule pass). When the tenant hasn't set up reporting yet, `statements_stamped` is `false` and `statement_stamp_note` carries the soft-skip reason (`no_coa_mapping`, `no_entity`, `no_statement_structures`, or `no_taxonomy`) — the close still succeeds.

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

Analytical view operations are read-only queries over the **XBRL hypercube** — the `Fact` nodes materialized into the LadybugDB graph. They share the `OperationEnvelope` contract with command writes and live under the same `/operations/` prefix, but they mutate nothing.

`build-fact-grid` is the primary view operation. It takes a set of concept filters and period filters, walks the matching `Fact` nodes, deduplicates them, and returns them as a **flat list** alongside the aspects those facts span.

**It does not pivot.** Arranging the facts into a table is the consumer's job, because collapsing cells safely requires the full aspect signature — two facts can share an element and a period end and still differ on `period_start`, `duration_type`, `unit`, or entity. Summing across those is a presentation decision the endpoint deliberately refuses to make on the caller's behalf. The same logic is exposed as the MCP tool `build-fact-grid`, with the same flat contract.

Two things make this router distinct from the command writes:

- **It is gated independently.** The view router checks `FACT_GRID_ENABLED`, not `ROBOLEDGER_ENABLED`. A deployment that serves only the SEC shared repository can mount fact-grid without provisioning any RoboLedger tenant.
- **It reads the OLAP graph, not the OLTP database.** Tenant data only appears in the hypercube *after materialization* — a freshly-written ledger will not surface in a fact grid until the blue/green materialization runs. The SEC shared repository is always materialized.

The request body is a `CreateViewRequest` (defined in [`models/api/views/view_config.py`](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/api/views/view_config.py)). The two filter axes you must satisfy:

- **Concept filter** — `elements[]` (XBRL qnames like `us-gaap:Assets`) or `canonical_concepts[]` (semantic names like `revenue`, `net_income`, which match every mapped qname for that concept).
- **Period filter** — `periods[]` (`YYYY-MM-DD`), or `period_type` (`annual` / `quarterly` / `instant`), or `fiscal_year`.

You must provide at least one concept filter **and** at least one period filter, or the request returns `400`. Note that `fiscal_period` is *not* a period filter — it narrows the fiscal context of an already-scoped query.

On a **shared-repository** graph (SEC), `entity` or `entities` is required as well: those graphs host thousands of filers, so an unscoped query returns an arbitrary slice of arbitrary companies. A tenant graph is already scoped to its entity by the URL — and that entity is often a private company with no ticker or CIK to filter on — so the requirement applies only to shared repos.

Other optional fields scope the query further: `entity`, `entities[]`, `form`, `fiscal_period` (`FY`, `Q1`, …), `include_summary`, `limit` (default `250`, max `5000`), and `view_config`.

## Worked Example: build-fact-grid

A balance-sheet rollup against a tenant graph, scoped to three concepts at one instant:

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/build-fact-grid" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "elements": ["us-gaap:Assets", "us-gaap:Liabilities", "us-gaap:StockholdersEquity"],
    "periods": ["2026-03-31"],
    "period_type": "instant",
    "include_summary": true
  }'
```

The envelope's `result` is a `ViewResponse` — `metadata`, `dimensions`, `facts`, `summary`:

```json
{
  "metadata": {
    "view_id": "3e0b2208-ab92-4629-9e74-e69a7a4b60e0",
    "facts_processed": 3,
    "construction_time_ms": 42.1,
    "source": "fact_grid",
    "period_start": null,
    "period_end": null,
    "truncated": false
  },
  "dimensions": [
    {"name": "Element", "type": "element",
     "members": ["us-gaap:Assets", "us-gaap:Liabilities", "us-gaap:StockholdersEquity"]},
    {"name": "Period", "type": "period", "members": ["2026-03-31"]}
  ],
  "facts": [
    {"element_id": "us-gaap:Assets", "element_name": "Assets",
     "period_start": null, "period_end": "2026-03-31", "duration_type": null,
     "value": 1842300.0, "unit": "USD", "entity_ticker": null, "entity_name": null},
    {"element_id": "us-gaap:Liabilities", "element_name": "Liabilities",
     "period_start": null, "period_end": "2026-03-31", "duration_type": null,
     "value": 612450.0, "unit": "USD", "entity_ticker": null, "entity_name": null},
    {"element_id": "us-gaap:StockholdersEquity", "element_name": "StockholdersEquity",
     "period_start": null, "period_end": "2026-03-31", "duration_type": null,
     "value": 1229850.0, "unit": "USD", "entity_ticker": null, "entity_name": null}
  ],
  "summary": {
    "us-gaap:Assets": {"count": 1, "total": 1842300.0, "average": 1842300.0,
                       "min": 1842300.0, "max": 1842300.0},
    "us-gaap:Liabilities": {"count": 1, "total": 612450.0, "average": 612450.0,
                            "min": 612450.0, "max": 612450.0},
    "us-gaap:StockholdersEquity": {"count": 1, "total": 1229850.0, "average": 1229850.0,
                                   "min": 1229850.0, "max": 1229850.0}
  }
}
```

`facts[]` is the payload: one deduplicated record per fact, carrying its full aspect signature (`element_id`, `element_name`, `period_start`, `period_end`, `duration_type`, `value`, `unit`, `entity_ticker`, `entity_name`). `dimensions[]` reports the aspects those facts span — element / period / entity with their observed members — which is what a client needs to lay out rows and columns itself. `summary` is present only when `include_summary: true`, and its `total` sums across every returned period, which is meaningful for duration facts and not for instants.

`metadata.truncated` is `true` when more facts matched than `limit` allowed. The returned facts are the most recent by period, so a truncated response is a valid answer to a narrower question — raise `limit` or tighten the filters to see the rest.

The `view_config` block scopes the query further; it is **filtering only**. This example asks the SEC shared repository for NVIDIA's annual revenue, restricted to three specific period members (NVIDIA's fiscal year ends in late January, so the members are its fiscal year ends, not calendar ones):

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/sec/operations/build-fact-grid" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "canonical_concepts": ["revenue"],
    "entities": ["NVDA"],
    "form": "10-K",
    "period_type": "annual",
    "view_config": {
      "columns": [{"type": "period",
                   "selected_members": ["2023-01-29", "2024-01-28", "2025-01-26"]}]
    }
  }'
```

The result is again a flat list — three facts, one per fiscal year, each carrying the entity and the duration window it came from:

```json
{
  "facts": [
    {"element_id": "us-gaap:Revenues", "element_name": "Revenues",
     "period_start": "2024-01-29", "period_end": "2025-01-26",
     "duration_type": "annual", "value": 130497000000.0, "unit": "USD",
     "entity_ticker": "NVDA", "entity_name": "NVIDIA CORP"},
    {"element_id": "us-gaap:Revenues", "element_name": "Revenues",
     "period_start": "2023-01-30", "period_end": "2024-01-28",
     "duration_type": "annual", "value": 60922000000.0, "unit": "USD",
     "entity_ticker": "NVDA", "entity_name": "NVIDIA CORP"},
    {"element_id": "us-gaap:Revenues", "element_name": "Revenues",
     "period_start": "2022-01-31", "period_end": "2023-01-29",
     "duration_type": "annual", "value": 26974000000.0, "unit": "USD",
     "entity_ticker": "NVDA", "entity_name": "NVIDIA CORP"}
  ]
}
```

A `ViewConfig` has exactly two fields: `rows[]` and `columns[]`, each a list of axis configs. An axis config has a `type` (`element`, `period`, or `entity`), an optional `selected_members[]`, and `include_null_dimension`. `rows` and `columns` are naming conventions only — both filter identically, and neither controls layout. There is no `values`, `aggregation_function`, `fill_value`, or `member_labels`: ordering, labelling, and aggregation belong to the consumer. A `dimension` axis type is rejected outright, because the fact query filters on `has_dimensions = false` — dimensional facts never reach the response, so a dimension axis could only ever be a silent no-op. The full schema is in [`models/api/views/`](https://github.com/RoboFinSystems/robosystems/tree/main/robosystems/models/api/views).

## Financial Statements

Two operations render a *complete* statement rather than a free-form slice. Which one you want depends on where the truth for that graph lives.

### live-financial-statement — tenant graphs

`live-financial-statement` is the authoritative path for a RoboLedger entity graph. It builds the statement directly from the tenant's OLTP ledger through the active CoA→GAAP mapping, so **no materialization is required** — a ledger you synced a minute ago is already reportable. It is rejected on shared-repository graphs.

It lives in a third router, [`reads.py`](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/routers/extensions/roboledger/reads.py), sibling to `operations.py` (command writes) and `views.py` (graph-backed views). All three mount under the same `/operations/` prefix, but `reads.py` is gated by `ROBOLEDGER_ENABLED` — unlike `views.py`, which stays mounted for SEC-only deployments.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/live-financial-statement" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"statement_type": "balance_sheet", "period_type": "annual", "fiscal_year": 2026}'
```

`statement_type` is one of `income_statement`, `balance_sheet`, `cash_flow_statement`, or `equity_statement`. Scope the window either explicitly (`period_start` / `period_end`) or by fiscal context (`period_type`, `fiscal_year`), anchored on the graph's fiscal calendar; explicit dates win. `limit` caps the fact rows returned. The result carries the resolved `periods[]` (current plus prior comparative), the rendered `facts[]` with `depth` and `is_subtotal` for indentation, and an `unmapped_count`.

### financial-statement-analysis — materialized and shared-repository graphs

`financial-statement-analysis` renders the same four statement types out of the hypercube (`Structure → FactSet → Fact`). Use it against the SEC shared repository, or against a tenant graph whose ledger has already been materialized to LadybugDB.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/financial-statement-analysis" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"statement_type": "balance_sheet", "report_id": "..."}'
```

The either/or is hard-enforced, and the two graph classes fail in opposite directions:

- On a **tenant** graph, omitting `report_id` returns `400 "report_id is required for tenant graphs."`
- On a **shared repository**, omitting both `ticker` and `report_id` returns `400 "ticker is required on shared-repository graphs (e.g. SEC)."` Supplying `ticker` auto-resolves the latest matching filing; a `fiscal_year` that resolves to nothing is a `404`, never a silent fallback to a different year.

How statements are rendered — the structures, the calc rollup, the footing — is the subject of [Reporting and Rendering](Reporting-and-Rendering).

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

`build-fact-grid` enforces three guards, each a `400`:

- No concept filter — omitting both `elements` and `canonical_concepts`.
- No period filter — omitting all of `periods`, `period_type`, and `fiscal_year`. `fiscal_period` alone does not satisfy this.
- No entity filter **on a shared-repository graph** — omitting both `entity` and `entities` against SEC returns `"entity or entities is required on shared-repository graphs (e.g. SEC)."` The same call against a tenant graph is fine, since the URL already scopes it.

### Fact Grid Only Sees Materialized Data

A tenant ledger must be materialized into LadybugDB before its facts appear in a grid. A ledger you just wrote will not surface until the blue/green materialization runs. The SEC shared repository is always materialized.

### Posted Entries Are Immutable

`update-journal-entry` and `delete-journal-entry` operate on **drafts** only. To correct a posted entry, record a reversal via `create-event-block` with `event_type='journal_entry_reversed'`. Library-seeded mapping associations are likewise immutable — deleting one returns `403`.

### Idempotency-Key Reuse With a Changed Body

Reusing an `Idempotency-Key` with a different request body returns `409`, not a silent overwrite. Use a fresh key per distinct operation.

## Self-hosted deployments

On your own stack, `just demo-roboledger` provisions a tenant with 16 months of synthetic books in about 60 seconds and writes the API key and graph id into `.local/config.json` — see the [RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough). To layer real synced data under the same workflow, and for the write-policy model behind the `entries_published_to_qb` / `entries_posted_locally` split, see [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally).

## Related Documentation

**Wiki Guides:**

- **[Extensions Surface Overview](Extensions-Surface-Overview)** - The full RoboLedger / RoboInvestor extensions surface and CQRS split
- **[GraphQL Reads](GraphQL-Reads)** - The typed-read sibling surface (`POST /extensions/{graph_id}/graphql`)
- **[Reporting and Rendering](Reporting-and-Rendering)** - How statements are rendered and footed from the hypercube
- **[RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough)** - See the operations surface end-to-end against a populated tenant

**API Reference:**

- **[API Documentation](https://api.robosystems.ai/docs)** - Live OpenAPI spec with every request body and response model

**Codebase Documentation:**

- **[RoboLedger operations router](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/routers/extensions/roboledger/operations.py)** - The registered command-write operations
- **[Analytical view router](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/routers/extensions/roboledger/views.py)** - `build-fact-grid` and `financial-statement-analysis`
- **[OLTP reads router](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/routers/extensions/roboledger/reads.py)** - `live-financial-statement`
- **[Operation envelope](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/operations.py)** - `OperationEnvelope`, idempotency, audit
- **[Operations kernel](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business logic patterns behind every operation

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
