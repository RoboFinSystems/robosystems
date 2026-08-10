# Reporting & Rendering

RoboSystems renders financial statements once, on the server, from atomic facts — and ships the result to any consumer as light JSON. The browser displays pre-computed rows; it never walks a calculation graph. This page explains the rendering engine, the conceptual model behind it, and how to request a rendered report.

## Table of Contents

- [Overview](#overview)
- [The Calc-DAG Model](#the-calc-dag-model)
- [What the Renderer Derives for You](#what-the-renderer-derives-for-you)
- [Reporting Style Decides Which Structures the Renderer Walks](#reporting-style-decides-which-structures-the-renderer-walks)
- [The View Projections](#the-view-projections)
- [Guard Rails](#guard-rails)
- [The Report Is the Package](#the-report-is-the-package)
- [How to Request a Rendered Report](#how-to-request-a-rendered-report)
- [The BlockView Frontend](#the-blockview-frontend)
- [How to Contribute: Writing Blocks](#how-to-contribute-writing-blocks)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

Three ideas run through the entire reporting layer:

- **One renderer, many sources.** A single Python algorithm renders statements for SEC filings, materialized tenant graphs, and live OLTP ledgers. Because the calc walk lives on the server, every consumer — the dashboard, an MCP-driven AI assistant, the Python client — gets identical numbers. One line of frontend code replaces the equivalent of hundreds of lines of client-side calculation.
- **One block, many shapes.** An Information Block is a data envelope around a set of facts. A *View* is one lens over that envelope. The rendered statement (rows, subtotals, period columns) is just one View; several others project the same molecule (see [The View Projections](#the-view-projections)).
- **Facts are structure-agnostic; a statement is a walk over a structure.** The same flat bundle of facts can be rendered as a GAAP Balance Sheet or a tax-basis Balance Sheet — two different walks over the same facts. Nothing about a fact knows what statement it belongs to.

Rendering happens in two phases:

1. **Phase 1 — fact generation** is structure-agnostic. It reads mapped chart-of-accounts balances, derives earnings, cash-flow, and subtotal facts, and emits a flat bundle.
2. **Phase 2 — structure rendering** walks a presentation tree and a calculation DAG over those facts to produce ordered rows with depth, subtotals, and one value per period column.

Because the two phases are separate, the same facts feed any number of structures.

## The Calc-DAG Model

A financial statement is a tree of roll-ups: leaves sum into subtotals, subtotals sum into bigger subtotals, all the way to a root (`Assets`, `Net Income`). Internally this is a **calculation DAG** — a parent equals the weighted sum of its children, recursively, down to the leaves.

The single most important rule:

> **Your chart of accounts maps only to leaves.** Subtotals (`Assets`, `Revenues`, `GrossProfit`) are always *derived*, never mapping targets — and they are also persisted as facts so that rules can bind to them.

This keeps the model honest. If a subtotal could be mapped directly, two sources of truth would exist for the same number — the mapped value and the computed roll-up — and they could disagree. By forbidding it, the roll-up is the only source of truth for any subtotal.

A practical consequence: **mapping a CoA account to a subtotal concept means the fact lands on a dead branch and never renders.** Every mapping target must reach a network root through the calc DAG. A catch-all "Other" leaf exists per section so that genuinely unclassified balances still foot.

## What the Renderer Derives for You

Phase 1 emits more facts than you mapped. Four derivations are worth understanding:

- **Auto-derived Retained Earnings.** Net Income (the sum of revenue minus expenses, netting dividends and buybacks) is folded into the equity close-target concept at render time, so `Assets = Liabilities + Equity` holds *without a posted closing journal entry*. This is the QuickBooks pattern, and it makes backdating safe: Retained Earnings is recomputed on every render, so a backdated transaction can never leave a stale balance behind.
- **Net Income as a standalone fact.** The same earnings figure is emitted as its own fact so the Income Statement bottom line and the Equity roll-forward agree by construction.
- **Flow-as-fact cash flow.** Investing and financing cash-flow facts are emitted directly from each ledger line's flow tag (`flow_element_id`). Operating cash flow is derived from period-over-period balance-sheet deltas (the indirect method). This is why untagged accounting data still renders a cash-flow statement — provided accounts are mapped at the grain the arcs key on (for example, PP&E Gross for capex).
- **PP&E net synthesis.** Gross property and accumulated depreciation roll up into a net line so the Balance Sheet presents the figure readers expect.

All of these are computed, not stored on the ledger — which is exactly why backdating, restatement, and re-mapping stay safe: the next render recomputes everything from current facts.

## Reporting Style Decides Which Structures the Renderer Walks

Two companies can hold the same facts and still present different statements — different ordering, different subtotal layout, a single-step versus multi-step income statement. That choice is the **Reporting Style**.

The Style is pinned **per Entity**, not per graph: `entities.reporting_style_id` is non-nullable and is defaulted from the entity's legal form when the entity is created (corporation → Default, partnership → PART, llc → LLC), so heterogeneous subsidiaries can each carry their own style while still resolving to the same canonical calc-DAG subtotals. Three Styles ship — `BSC-{CORP|PART|LLC}-IS02-CF1`, an equity-form axis over a fixed classified balance sheet, multi-step income statement, and indirect cash flow. Each maps to one **Network** — a presentation tree — per statement type. At render time the engine does one deterministic join: Style → Network → the leaves the calc DAG rolls up. It never guesses which structure to walk.

```text
Reporting Style  ──▶  Network (per statement type)  ──▶  leaves  ──▶  calc DAG roll-up
   (per entity)         (presentation tree)            (your CoA)     (derived subtotals)
```

Switching a Style later (`change-reporting-style`) changes future renders but never retroactively rewrites a filed report: each saved FactSet pins its own structure at create time, so frozen reports keep the presentation they were filed with.

## The View Projections

An Information Block carries facts plus the structural metadata around them. A **View** is one projection of that envelope. Seven are modeled; **two ship on the envelope today**:

| View | What it shows | Status |
| --- | --- | --- |
| **Rendering** | Ordered statement rows with subtotals, depth, and one value per period | Server-computed (needs the calc walk) |
| **Chart** | Panel and series configuration over the rendering's rows and periods | Server-computed, for metric blocks |
| **Fact Table** | The raw fact list (element, period, unit, value) | Derivable client-side from `facts`; backend arm pending |
| **Report Elements** | The element/concept definitions referenced | Backend support pending |
| **Verification Results** | Rule-engine outcomes for the block | Backend support pending |
| **Model Structure** | The presentation/calculation tree itself | Backend support pending |
| **Business Rules** | The rules bound to the block | Backend support pending |

`ViewProjections` — the `view` field on the envelope — carries exactly `rendering` and `chart`. The other five come online as their backend support lands; `fact_table` in particular is trivially derivable from the envelope's own `facts` list and may stay a frontend-only projection. Missing projections render as empty states rather than breaking the dispatcher.

In the GraphQL envelope, the rendered View lives at `view.rendering` and is populated for the statement family (Balance Sheet, Income Statement, Cash Flow, Equity, Comprehensive Income). Non-statement blocks such as Schedules group their facts by period and do not carry a server-computed `rendering`.

A `RenderingLite` payload is intentionally small:

```text
RenderingLite
  rows[]        element_id, element_qname, element_name, classification,
                balance_type, item_type, depth, is_subtotal, text_value,
                values[]  (one per period)
  periods[]     label, start, end, forecast
  validation    passed, checks[], failures[], warnings[]
  unmapped_count
```

Each row's `classification` follows FASB SFAC 6 (`asset`, `liability`, `equity`, `revenue`, `expense`), `depth` drives indentation, and `is_subtotal` marks derived roll-up rows. `element_id` / `element_qname` are the join key the chart arm keys off, `item_type` carries the value-domain format family (monetary, ratio, percent, multiple, days) that drives per-row formatting, and `text_value` holds a non-numeric row's text. On a period column, `forecast` is the machine-readable seam marker for a column sourced from a forecast scenario's FactSet — labels also carry a `(forecast)` suffix, but style off the flag, not the label. The browser renders all of this directly — no calculation required.

## Guard Rails

Rendering runs **synchronous footing checks** at the moment a statement is produced. These guard rails confirm the arithmetic holds right now:

- `Assets = Liabilities + Equity`
- `Net Income = Revenue − Expenses`
- The cash-flow statement foots (operating + investing + financing reconciles to the change in cash)

Guard-rail results surface in the `validation` block of a `RenderingLite` payload (`passed`, `checks`, `failures`, `warnings`).

**Do not conflate guard rails with rule-engine verification.** Guard rails are render-time footing checks — "does this statement balance right now." Rule-engine verification (`verificationResults` / `verificationSummary` on the envelope) is a separate, persisted audit-corpus outcome. Both can assert `Assets = L + E`, but they answer different questions and run at different times.

## The Report Is the Package

You do not *create* a Balance Sheet. You create a **Report**, and the statements are surfaced as Views of its facts.

The `reports` table **is** the package container. A Report row carries:

- Identity and period: `name`, `period_start`, `period_end`, `periods` (JSON), `taxonomy_id`
- Two independent lifecycles:
  - **Generation** — `generation_status` moves `pending → generating → complete → published`
  - **Filing** — `filing_status` moves `draft → under_review → filed → archived`, with `filed_at` / `filed_by`
- A **restatement chain** via `supersedes_id` / `superseded_by_id`, so a corrected report links to the one it replaces
- Sharing provenance: `source_graph_id`, `source_report_id`, `shared_at`

Package membership is **implicit in fact dual-stamping**: a Report owns FactSets (`fact_sets.report_id`), and FactSets own Facts (`facts.fact_set_id`). That dual stamp *is* the membership — there is no separate join table listing which statements belong to a report. The statement display order is fixed: Balance Sheet (1), Income Statement (2), Cash Flow (3), Equity (4), then Schedules (100).

This is why a statement is a view, not a stored artifact: the Report holds facts; the envelope renders them on demand.

## How to Request a Rendered Report

There are several addressable surfaces, depending on whether you want a saved Report, a live ad-hoc statement, or a pivot grid. All authenticated `curl` examples read the API key from `.local/config.json` (created by `just demo-user` or any demo command) and target `http://localhost:8000`. Replace `$GRAPH_ID` with your tenant graph id, or use `sec` for the shared SEC repository.

Full request/response schemas live in the [live OpenAPI spec](https://api.robosystems.ai/docs) — this page shows usage, not the full endpoint surface.

### Build a Fact Grid (Scoped Slice of the Hypercube)

`build-fact-grid` queries the XBRL hypercube in the graph. It does **not** produce a pivot table: it returns a scoped, deduplicated fact slice plus the aspects those facts span — the raw material a pivot is built from. Arranging cells is the consumer's job, deliberately, because summing across entities, units, or elements that merely share a local name is a presentation decision the endpoint refuses to make on the caller's behalf. The response is a `ViewResponse` carrying `metadata`, `dimensions`, `facts[]`, and an optional `summary`.

It works on the SEC shared repo and on materialized tenant graphs. The request must supply `elements` and/or `canonical_concepts`, **and** at least one of `periods` / `period_type` / `fiscal_year` — otherwise it returns a 400.

The `sec` examples below need a subscription to the shared repository; a fresh `just demo-user` key gets a 403 until you run `just demo-sec-subscribe`.

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/sec/operations/build-fact-grid" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{
    "elements": ["us-gaap:Assets"],
    "entities": ["NVDA"],
    "form": "10-K",
    "period_type": "annual"
  }'
```

A narrower slice puts entities on rows and pins the exact period columns you want back:

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/sec/operations/build-fact-grid" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{
    "canonical_concepts": ["revenue"],
    "entities": ["NVDA", "AMD"],
    "form": "10-K",
    "period_type": "annual",
    "view_config": {
      "rows": [{"type": "entity"}],
      "columns": [{"type": "period",
        "selected_members": ["2024-01-28", "2025-01-26"]}]
    }
  }'
```

`view_config` carries only `rows` and `columns`. Each is a list of axis configs whose `type` is one of `element`, `period`, or `entity` — `dimension` is rejected with a 422 — plus optional `selected_members` and `include_null_dimension`. **`view_config` filters; it does not format.** There are no ordering, labelling, value-field, or aggregation-function options, because ordering and labelling are presentation concerns belonging to the consumer that arranges the facts into a table. The SEC graph this runs against is the one set up in the [SEC XBRL Pipeline](SEC-XBRL-Pipeline) guide.

### Create a Saved Report

`create-report` renders the Balance Sheet, Income Statement, Cash Flow, and Equity facts for a tenant ledger and publishes them as a Report. `taxonomy_id` defaults to `rs-gaap`, but **`mapping_id` is required** — without a chart-of-accounts → GAAP mapping there is nothing to roll up.

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/create-report" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Q1 2026 Financials",
    "taxonomy_id": "rs-gaap",
    "mapping_id": "<your mapping id>",
    "period_start": "2026-01-01",
    "period_end": "2026-03-31",
    "period_type": "quarterly",
    "comparative": true
  }'
```

Obtain a real `mapping_id` from the `auto-map-elements` output or by creating a mapping association — see [RoboLedger Operations](RoboLedger-Operations). The operation returns an `OperationEnvelope` and accepts an `Idempotency-Key` header so retries are safe.

### Fetch a Rendered Statement via GraphQL

The rendered View is exposed on the Information Block envelope. GraphQL is served at `POST /extensions/{graph_id}/graphql` — the graph is the URL scope, so queries do **not** take a `graphId` argument.

```bash
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ informationBlock(id: \"<structure_id>\") { blockType displayName view { rendering { rows { elementName depth isSubtotal values } periods { label start end } validation { passed checks failures } } } verificationSummary { passed failed } } }"}'
```

`informationBlock(id)` returns the **latest** FactSet — the live closing-book view. To pin a specific frozen snapshot, query `reportPackage(reportId)`, which rehydrates a saved Report as a package of rendered envelopes with each member pinned to its own FactSet.

> **Live versus pinned reads differ.** `informationBlock(id)` always reflects current facts; `reportPackage(reportId)` reflects the facts as they were when the report was created. The snapshot unit is the FactSet, not the structure.

### Render a Live Ad-Hoc Statement

`live-financial-statement` renders straight off the OLTP ledger with no saved Report. It is **tenant-only** — rejected on shared-repository graphs like `sec`. Supported `statement_type` values are `income_statement`, `balance_sheet`, `cash_flow_statement`, and `equity_statement`.

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/live-financial-statement" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"statement_type": "income_statement", "period_type": "annual", "fiscal_year": 2026}'
```

The cash-flow path on this surface needs at least two periods so the indirect-method deltas can be computed. For comparative graph-hypercube analysis on the SEC repo, use `financial-statement-analysis` instead.

### From an MCP Client

The same operations are exposed as MCP tools — `build-fact-grid`, `financial-statement-analysis`, and `live-financial-statement`. On a shared SEC repository graph, `financial-statement-analysis` takes a `ticker` and `statement_type` to pull a company's statement straight from the graph hypercube. They delegate to the same operations layer as the REST surface, so an AI assistant gets identical numbers. See the [SEC XBRL Pipeline](SEC-XBRL-Pipeline) guide for MCP client setup.

## The BlockView Frontend

The dashboard's BlockView is an **envelope-driven dispatcher**. It receives an Information Block envelope and switches on `block_type` and the requested View mode to choose how to display it — rendered statement rows, a fact table, the model structure tree, and so on. Because the envelope already carries pre-computed `view.rendering` rows and `view.chart` config plus the atom lists the remaining View modes project from, the component dispatches and displays; it does not calculate. Switching View mode for a block is a client-side re-projection, not a new server request.

## How to Contribute: Writing Blocks

The reporting layer reads from three kinds of authored content, each with its own write path. Contributors extend the system by writing to the appropriate one rather than touching the renderer.

- **Information Blocks** — the data envelopes the renderer surfaces. You do not author a statement block directly; you create a Report (`create-report`) whose facts the envelope renders. Authored blocks such as Schedules are the place to originate intent. See [Information Blocks](Information-Blocks).
- **Taxonomy** — the frameworks (`rs-gaap`, `fac`) that define the calc DAG: which concepts exist, how leaves roll up into subtotals, and which presentation networks a Reporting Style can walk. Edits to a framework's `taxonomy.jsonld` flow into graphs through a library reseed.
- **Event / ledger** — the transactions, entries, and line items that become facts. The flow tag (`flow_element_id`) on a line item is what makes flow-as-fact cash flow work.

A worked example — request a rendered Income Statement after you have a mapped ledger:

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=<your tenant graph id>

# 1. Render live off the ledger to confirm the mapping foots
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/live-financial-statement" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"statement_type": "income_statement", "period_type": "annual", "fiscal_year": 2026}'

# 2. Once it foots, save it as a Report package
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/create-report" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{"name": "FY2026 Income Statement", "mapping_id": "<your mapping id>",
       "period_start": "2026-01-01", "period_end": "2026-12-31",
       "period_type": "annual", "comparative": true}'
```

If a number is wrong, the fix is almost never in the renderer — it is in the mapping (a CoA account pointed at a subtotal instead of a leaf), the taxonomy (a missing calc arc), or the ledger (an untagged flow line). To explore the running surface end-to-end, `just demo-roboledger` provisions a tenant with synthetic data and a mapped chart of accounts.

## Related Documentation

**Wiki Guides:**

- **[Information Blocks](Information-Blocks)** - The data envelope, block types, FactSets, and the View projections at the data layer
- **[RoboLedger Operations](RoboLedger-Operations)** - The command writes (`create-report`, `regenerate-report`, period close) that produce the facts this page renders
- **[Serialization & Export](Serialization-and-Export)** - Exporting and distributing rendered reports (iXBRL, package sharing)
- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - Load and query the SEC graph that `build-fact-grid` and `financial-statement-analysis` run against
- **[Architecture Overview](Architecture-Overview)** - How the reporting layer fits the broader platform

**Codebase Documentation:**

- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business-logic kernel; the operations the renderer delegates to
- **[GraphQL](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - The Strawberry extensions surface that serves `informationBlock` and `reportPackage`
- **[API Documentation](https://api.robosystems.ai/docs)** - API reference with machine-readable OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
