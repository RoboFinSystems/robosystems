# Reporting & Rendering

RoboSystems renders financial statements once, on the server, from atomic facts — and ships the result to any consumer as light JSON. The browser displays pre-computed rows; it never walks a calculation graph. This page explains the rendering engine, the conceptual model behind it, and how to request a rendered report.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development.md).

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

Two companies can hold the same facts and still present different statements — different ordering, different subtotal layout, a single-step versus multi-step income statement. That choice is the **Reporting Style**. A framework (the basis of accounting) decides what a number means and when it is recognized; the Reporting Style decides how the recognized numbers are laid out within that framework.

A Style is named by a four-segment code, `{BS-layout}-{equity-form}-{IS-layout}-{CF-method}`, for example `BSC-CORP-IS02-CF1`:

| Segment | Shipped | Reserved | Selects |
| --- | --- | --- | --- |
| BS-layout | `BSC` | `BSU`, `NET` | Balance-sheet layout (classified) |
| equity-form | `CORP`, `PART`, `LLC` | `SOLE`, `NFP` | Equity presentation |
| IS-layout | `IS02` | `IS01` | Income-statement layout (multi-step) |
| CF-method | `CF1` | `CF2` | Cash-flow method (indirect) |

Three Styles ship, `BSC-{CORP|PART|LLC}-IS02-CF1`: the equity form is the only axis with variants today. The reserved codes (other balance-sheet layouts, a single-step income statement, the direct cash-flow method) are room in the grammar, not shipped styles.

The Style is pinned **per Entity**, not per graph. Every entity has one, defaulted from its legal form when it is created (corporation → `CORP`, partnership → `PART`, LLC → `LLC`), so subsidiaries of different forms each carry their own style while resolving to the same calc-DAG subtotals. A Style composes one **Network** (a presentation tree) per statement type, and at render time the engine does one deterministic join: Style → Network → the leaves the calc DAG rolls up. It never guesses which structure to walk.

```text
Reporting Style  ──▶  Network (per statement type)  ──▶  leaves  ──▶  calc DAG roll-up
   (per entity)         (presentation tree)            (your CoA)     (derived subtotals)
```

Switch a Style with `change-reporting-style`, passing a `reporting_style_id` and optionally an `entity_id` (omit it for the graph's primary entity). The switch changes future renders only. Each saved FactSet pins its structure when it is created, so a filed report keeps the presentation it was filed with.

## The View Projections

An Information Block carries facts plus the structure around them. A **View** is one projection of that envelope. Two are computed on the server and ride on the envelope's `view` field; the rest are projections of lists the envelope already carries, done in the client:

| View | What it shows | Where it comes from |
| --- | --- | --- |
| **Rendering** | Ordered statement rows with subtotals, depth, and one value per period | `view.rendering`, computed on the server (it needs the calc walk). Filled for the statement family, disclosures, metrics, forecast blocks, and reconciliations (one column per period end). |
| **Chart** | Panel and series configuration over the rendering's rows and periods | `view.chart`, computed on the server for metric blocks |
| **Facts** | The raw fact list: element, period, unit, value | The envelope's `facts` |
| **Elements** | The accounts and concepts the block is built from | The envelope's `elements` |
| **Validation** | Each rule's last result, and the summary | The envelope's `verification_results`, `verification_summary`, and `rules` |
| **Rules** | The rules bound to the block and their severity, evaluated or not | The envelope's `rules` |

These are the six views the RoboLedger Explorer offers. A model-structure view of the presentation and calculation tree is not shipped. Schedules and rollforwards carry no server rendering; a client groups their facts by period.

A `RenderingLite` payload is intentionally small:

```text
RenderingLite
  rows[]        element_id, element_qname, element_name, classification,
                balance_type, item_type, depth, is_subtotal, text_value,
                values[]  (one per period)
  periods[]     label, start, end, forecast
  validation    passed, status, checks[], failures[], warnings[]
  unmapped_count
```

Each row's `classification` follows FASB SFAC 6 (`asset`, `liability`, `equity`, `revenue`, `expense`), `depth` drives indentation, and `is_subtotal` marks derived roll-up rows. `element_id` / `element_qname` are the join key the chart keys off, `item_type` carries the value-domain format family (monetary, ratio, percent, multiple, days) that drives per-row formatting, and `text_value` holds a non-numeric row's text. On a period column, `forecast` marks a column sourced from a forecast scenario's FactSet; labels also carry a `(forecast)` suffix, but style off the flag, not the label. The browser renders all of this directly, with no calculation.

## Guard Rails

Rendering runs **synchronous footing checks** at the moment a statement is produced. These guard rails confirm the arithmetic holds right now:

- `Assets = Liabilities + Equity`
- `Net Income = Revenue − Expenses`
- The cash-flow statement foots (operating + investing + financing reconciles to the change in cash)

Guard-rail results surface in the `validation` block of a `RenderingLite` payload (`passed`, `status`, `checks`, `failures`, `warnings`) and, with the same shape, on saved-report statements and the live statement. Every check runs on every rendered column; on a comparative statement each failure and warning is prefixed with the column it was found in (`[Prior] Balance sheet does not balance …`). `status` is `passed`, `failed`, or `inconclusive` — the last means no rules exist for that block type (the statement of equity today), nothing was checked, and `passed` is `false` rather than vacuously `true`.

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

There are several addressable surfaces, depending on whether you want a saved Report, a live ad-hoc statement, or a slice of facts. All authenticated `curl` examples target `https://api.robosystems.ai` and send your API key from `$ROBOSYSTEMS_API_KEY` in the `X-API-Key` header; to get an account, a key, and a graph, see [Quick Start](Quick-Start.md). Replace `$GRAPH_ID` with your tenant graph id. The read surfaces `build-fact-grid` and `financial-statement-analysis` also take `sec`, the shared SEC repository; `create-report` and `live-financial-statement` need a tenant graph with a RoboLedger ledger, so `sec` is not valid there.

Full request/response schemas live in the [live OpenAPI spec](https://robosystems.ai/docs/api) — this page shows usage, not the full endpoint surface.

### Build a Fact Grid (Scoped Slice of the Hypercube)

`build-fact-grid` returns a scoped, deduplicated slice of facts from the XBRL hypercube in the graph, plus the aspects they span. It works on the `sec` repository and on materialized tenant graphs, and it leaves arranging the facts into a pivot to the consumer. Request rules and a worked example are in [RoboLedger Operations § Worked Example: build-fact-grid](RoboLedger-Operations.md#worked-example-build-fact-grid).

### Create a Saved Report

`create-report` renders the Balance Sheet, Income Statement, Cash Flow, and Equity facts for a tenant ledger and publishes them as a Report. `taxonomy_id` defaults to `rs-gaap`, but **`mapping_id` is required** — without a chart-of-accounts → GAAP mapping there is nothing to roll up.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-report" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
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

The `mapping_id` is the id of the graph's chart-of-accounts mapping structure (a `struct_…` id); the `mappings` GraphQL field or the `list-mapping-structures` MCP tool returns it. See [Chart of Accounts Mapping](Chart-of-Accounts-Mapping.md). The operation returns an `OperationEnvelope` and accepts an `Idempotency-Key` header so retries are safe.

### Fetch a Rendered Statement via GraphQL

The rendered View is exposed on the Information Block envelope. GraphQL is served at `POST /extensions/{graph_id}/graphql` — the graph is the URL scope, so queries do **not** take a `graphId` argument.

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ informationBlock(id: \"<structure_id>\") { blockType displayName view { rendering { rows { elementName depth isSubtotal values } periods { label start end } validation { passed checks failures } } } verificationSummary { passed failed } } }"}'
```

`informationBlock(id)` returns the **latest** FactSet — the live closing-book view. To pin a specific frozen snapshot, query `reportPackage(reportId)`, which rehydrates a saved Report as a package of rendered envelopes with each member pinned to its own FactSet.

> **Live versus pinned reads differ.** `informationBlock(id)` always reflects current facts; `reportPackage(reportId)` reflects the facts as they were when the report was created. The snapshot unit is the FactSet, not the structure.

### Render a Live Ad-Hoc Statement

`live-financial-statement` renders straight off the OLTP ledger with no saved Report. It is **tenant-only** — rejected on shared-repository graphs like `sec`. Supported `statement_type` values are `income_statement`, `balance_sheet`, `cash_flow_statement`, and `equity_statement`. The MCP tool of the same name takes the first three and refuses `equity_statement`, because the live path renders equity balances rather than a rollforward; read the equity section of the balance sheet instead.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/live-financial-statement" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"statement_type": "income_statement", "period_type": "annual", "fiscal_year": 2026}'
```

The cash-flow path on this surface needs at least two periods so the indirect-method deltas can be computed. For comparative graph-hypercube analysis on the SEC repo, use `financial-statement-analysis` instead.

### From an MCP Client

The same operations are exposed as MCP tools — `build-fact-grid`, `financial-statement-analysis`, and `live-financial-statement`. On a shared SEC repository graph, `financial-statement-analysis` takes a `ticker` and `statement_type` to pull a company's statement straight from the graph hypercube. They delegate to the same operations layer as the REST surface, so an AI assistant gets identical numbers. Connecting Claude, ChatGPT, or any MCP client is covered in [AI Operators & MCP](AI-Operators-and-MCP.md).

## The BlockView Frontend

The app's BlockView is an **envelope-driven dispatcher**. It receives an Information Block envelope and switches on the chosen view and `block_type`: the rendered statement, schedule, metric, or text-block rows for Rendering, and a projection of the envelope's own lists for Facts, Elements, Validation, and Rules. Because the envelope already carries `view.rendering`, `view.chart`, and the atom lists the other views project from, switching views is a client-side re-projection, not a new request.

If a rendered number is wrong, the fix is almost never in the renderer. It is in the mapping (an account pointed at a subtotal instead of a leaf), the taxonomy (a missing calculation arc), or the ledger (a line without its flow tag).

## Related Documentation

**Guides:**

- **[Information Blocks](Information-Blocks.md)** - The data envelope, block types, FactSets, and the View projections at the data layer
- **[Information Block Reference](Information-Block-Reference.md)** - Envelope fields and view projection status
- **[RoboLedger Operations](RoboLedger-Operations.md)** - The command writes (`create-report`, `regenerate-report`, period close) that produce the facts this page renders
- **[Serialization & Export](Serialization-and-Export.md)** - Downloading a published report as the holon, Tavi, or XBRL 2.1 (there is no iXBRL output)
- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline.md)** - Load and query the SEC graph that `build-fact-grid` and `financial-statement-analysis` run against
- **[Architecture Overview](Architecture-Overview.md)** - How the reporting layer fits the broader platform

**Codebase Documentation:**

- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business-logic kernel; the operations the renderer delegates to
- **[GraphQL](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - The Strawberry extensions surface that serves `informationBlock` and `reportPackage`
- **[API reference](https://robosystems.ai/docs/api)** - API reference with machine-readable OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
