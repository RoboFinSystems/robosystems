# RoboLedger Demos

RoboLedger ships a suite of self-contained demos that exercise the full accounting stack — ingest, mapping, period close, reporting, and XBRL export — entirely against the local stack, with no live QuickBooks connection. They come in two flavors:

- **Synthetic data** (`just demo-roboledger`) — a fully-populated tenant generated from synthetic books: 16 months for a consulting firm, a filed annual report, a mapped Chart of Accounts, schedules, and one period queued for the AI-driven close. The fastest way to see RoboLedger end-to-end (~60 seconds), and the canonical surface for prospect demos, frontend evaluation, and regression testing. **This page walks through it in detail.**
- **Seattle Method / XBRL** (`just demo-seattle-method`, `just demo-world-online`) — built on Charlie Hoffman's *published* [Seattle Method](https://github.com/seattlemethod/prototypes) reference datasets. These prove cross-taxonomy projection and round-trip XBRL fidelity against an external reference, at both toy and realistic scale. See [Seattle Method XBRL Demos](#seattle-method-xbrl-demos) below.

Connecting a *real* QuickBooks company (sandbox or production) is a live OAuth connection rather than a demo — see [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally).

The synthetic demo needs no QuickBooks account, Intuit Developer setup, or ngrok tunnel. It emulates data arriving the same way a real QB sync produces it — same typed event vocabulary, same OLTP shape, same materialized graph — without the OAuth and external-API requirements.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [What Gets Created](#what-gets-created)
- [The Company — Cascade Advisory Group](#the-company--cascade-advisory-group)
- [The Filed Annual Report](#the-filed-annual-report)
- [The Close-Period Workflow](#the-close-period-workflow)
- [The Rule Engine](#the-rule-engine)
- [The Mapping View](#the-mapping-view)
- [Schedules](#schedules)
- [Frontend Tour](#frontend-tour)
- [Exploring with MCP](#exploring-with-mcp)
- [Resetting the Demo](#resetting-the-demo)
- [Customization](#customization)
- [Comparison to the QuickBooks Workflow](#comparison-to-the-quickbooks-workflow)
- [Seattle Method XBRL Demos](#seattle-method-xbrl-demos)
- [Next Steps](#next-steps)

## Overview

When `just demo-roboledger` finishes (~60 seconds), the tenant has:

- A new entity (or one provisioned into an existing graph) modeling **Cascade Advisory Group LLC**, a 3–5 person boutique consulting firm
- **16 months** of business data on a rolling window ending at the current month, so the demo always covers "recent history" regardless of when you run it
- A clean 27-account Chart of Accounts with 100% renderable mapping coverage to rs-gaap
- ~305 typed business events (`invoice_issued`, `payment_received`, `bill_received`, `bill_paid`, `journal_entry_recorded`) emulating data arriving via the same HTTP API a real adapter uses
- ~132 discharge chains linking payments to their originating invoices/bills (REA duality)
- A fiscal calendar with the prior month's prior month closed and the last completed month queued — **exactly one period ready to close**
- 6 schedules: 2 depreciation, 4 prepaid amortization, staggered across the window
- 4 policy documents (close procedures, depreciation, prepaid, revenue)
- An FY annual report — generated, packaged, and **filed** as a complete Report-Block lifecycle demonstration

After setup, the natural flow is:

1. Open the frontend, browse the entity, mapping coverage, and the filed annual report
2. Drop into the AI close workflow on the queued period via Claude Desktop or any MCP client
3. Inspect the rule engine output (the demo ships 38 validator checks; first run passes all 38)
4. Query the graph directly to explore the data shape

## Prerequisites

Just the local stack:

```bash
git clone https://github.com/RoboFinSystems/robosystems.git
cd robosystems
just start
```

You don't need Intuit credentials, ngrok, or any external service. The demo is fully self-contained and runs entirely against the local stack.

If you want to drive the AI close workflow at the end, you'll also want an MCP client (Claude Desktop, Claude Code, or a custom client). That's optional — everything else inspects via the frontend or the API.

## Quick Start

```bash
# In one terminal — start the backend stack if you haven't already.
cd robosystems
just start

# In another terminal — start the frontend (optional, but recommended for the tour).
cd ../roboledger-app
npm run dev

# Run the demo. ~60 seconds.
cd ../robosystems
just demo-roboledger
```

Output includes the API key + user credentials and the graph id of the new tenant — copy them or read them from `.local/config.json`. The last line prints the exact MCP prompt to use for the AI close on the queued period.

To load the demo into an existing graph (e.g. on a user / graph you've already provisioned):

```bash
just demo-roboledger <graph_id>
```

To validate the data without writing anything:

```bash
just demo-roboledger --dry-run
```

To provision a user + empty roboledger graph **without** the synthetic data (for connecting a real QuickBooks sandbox via the UI — see [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally)):

```bash
just demo-roboledger --skeleton
```

To swap the hardcoded CoA → GAAP mappings for AI-driven mapping via the MappingOperator (requires Bedrock):

```bash
just demo-roboledger --ai
```

## What Gets Created

| Component | Count | Detail |
|---|---|---|
| **Chart of Accounts** | 27 | Clean asset / liability / equity / revenue / expense breakdown with realistic small-business categories |
| **Agents** | 17 | 6 customers + 8 vendors + 2 employees + 1 government (IRS). Same shape a real QB sync produces. |
| **Business events** | ~305 | Typed event stream: invoice/payment pairs, bill/payment pairs, opening-balance and payroll journal entries. Mirrors the exact event vocabulary QB ingestion emits. |
| **Discharge chains** | ~132 | `payment_received → invoice_issued` and `bill_paid → bill_received` links — the REA duality that drives open-AR / open-AP aging |
| **CoA → rs-gaap mappings** | 27 | 100% coverage, all renderable under the active Reporting Style |
| **Fiscal calendar** | 1 | `closed_through = month_before_last`, `close_target = last_completed_month` |
| **Fiscal periods** | 16 | One per month, first 14 closed, last 2 open |
| **Schedules** | 6 | 2 depreciation (computer equipment, office furniture) + 4 prepaid amortization (insurance, software licenses, AWS prepay, etc.) |
| **Schedule facts** | mixed | Historical (pre-target, baked into opening balances) vs in_scope (target onward, surfaced by the close workflow) |
| **Documents** | 4 | Markdown policies: close procedures, depreciation, prepaid, revenue recognition |
| **Filed FY report** | 1 | Annual report — facts generated, package assembled, lifecycle stamped `filed`. Demonstrates the full Report-Block lifecycle end-to-end. |
| **Rules** | 38 | Validator + arithmetic checks; demo data is intentionally clean, so all 38 pass on first run |

Every event flows through the same HTTP API the frontend UI and MCP tools call. The only direct-DB path is `_reset.py`, used to clean up between re-runs.

## The Company — Cascade Advisory Group

The demo emulates **Cascade Advisory Group LLC**, a 3–5 person boutique management consulting firm. Grounding the data in a believable business shape makes the close workflow, the report numbers, and the policy documents all read as coherent — closer to what a real customer would walk in with than abstract test fixtures.

| Aspect | Value |
|---|---|
| **Service lines** | Three revenue streams: general consulting, strategy advisory, implementation services |
| **Monthly revenue** | $18K – $22K, with seasonality |
| **Customers** | 6 named customers across professional-services verticals |
| **Vendors** | 8 named vendors: landlord, payroll provider, AWS, software licenses, etc. |
| **Employees** | 2 — driving the monthly payroll JE |
| **Key expenses** | Payroll ($9,500/mo), rent ($2,500/mo), software ($450/mo), cloud hosting ($180/mo), professional services |
| **Fixed assets** | $4,800 computer equipment + $1,500 office furniture (driving depreciation schedules) |
| **Prepaids** | $1,200/yr insurance + $300/yr software + $600/yr AWS prepay (driving amortization schedules) |

The 16-month rolling window means the demo is always *running* a business — you're never in "future data" or "ancient history." That matches how real customers experience the product.

## The Filed Annual Report

The most material output: a fully-rendered FY annual report, **filed**, demonstrating the Report-Block lifecycle from generation through packaging through filing.

To inspect:

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=$(jq -r '.graphs | keys[0]' .local/config.json)

# List reports
curl "http://localhost:8000/v1/graphs/$GRAPH_ID/reports" \
  -H "X-API-Key: $API_KEY" | jq '.'

# Inspect facts on the filed report
docker exec robosystems-pg psql -U postgres -d extensions -tAc "
  SET search_path TO $GRAPH_ID;
  SELECT s.block_type AS statement,
         REPLACE(e.qname, 'rs-gaap:', '') AS concept,
         ROUND(f.value::numeric, 2) AS value
  FROM facts f
  JOIN fact_sets fs ON fs.id = f.fact_set_id
  JOIN elements e ON e.id = f.element_id
  JOIN structures s ON s.id = fs.structure_id
  WHERE fs.report_id = (SELECT id FROM reports WHERE filing_status = 'filed' LIMIT 1)
  ORDER BY s.block_type, ABS(f.value) DESC;"
```

You'll see facts distributed across all four statements (Balance Sheet, Income Statement, Cash Flow, Statement of Equity). The trial balance balances to the cent, retained earnings rolls forward correctly, and dividends/distributions net out of equity as expected.

In the frontend, navigate to **Reports** → the filed FY report → view the rendered BS / IS / CF / SE statements with drill-downs into Information Blocks for supporting detail.

## The Close-Period Workflow

The fiscal calendar is initialized so the prior month's prior month is `closed_through` and the **last completed month is the close target** — exactly one period ready to close. This matches what a real business onboarding mid-year does: wall off pre-onboarding periods as `historical`, start running the close workflow from a clean boundary.

The natural way to drive the close is via the AI workflow. With Claude Desktop or any MCP client connected to the local RoboSystems MCP server:

1. Paste the prompt from `examples/roboledger_demo/prompt.md` (or copy the close-month-specific prompt the setup script prints at the end)
2. Ask: *"Close the books for the most recent completed month."*

The agent walks through the standard 8-step close checklist (loaded into the tenant as the `close procedures` document):

1. **Review** the active close procedures — `search-documents` finds the checklist
2. **Check period status** — `get-period-close-status` shows the target month with any pending obligations
3. **Resolve open obligations** — schedules with in-scope facts pending approval; the agent proposes journal entries to record them
4. **Generate and review month-end accruals** — depreciation (PP&E rollforward), prepaid amortization (insurance, software, AWS)
5. **Validate** — `evaluate-rules` runs the 38 structural and arithmetic checks
6. **Close the period** — `close-period` flips the boundary forward
7. **Generate the close report** — month-over-month variance + balance sheet snapshot
8. **Document** — append any close notes to the documents corpus

Each step exercises a different RoboLedger surface: documents, schedules, journal entries, rule engine, period management, reports. The agent's run is auditable — every action is a typed operation in the event log, replayable on demand.

If you'd rather drive manually, the same operations are exposed via REST under `/extensions/roboledger/{graph_id}/operations/`.

## The Rule Engine

The demo ships with 38 rules covering structural validity (no cycles in the CoA, unique qnames, leaf accounts have classifications), arithmetic checks (debit-credit balance per entry, period rollforward integrity, accounting equation), and policy adherence (rolling forward accumulated depreciation, prepaid amortization completion).

To run:

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/evaluate-rules" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{}' | jq '.result | {passed, failed, total, by_severity}'
```

First-run output:

```json
{
  "passed": 38,
  "failed": 0,
  "total": 38,
  "by_severity": {"error": 0, "warning": 0, "info": 0}
}
```

The demo data is intentionally clean — the rule engine isn't supposed to find issues. The value is showing the *shape* of the rule engine output and the breadth of what it covers. Once you start the close workflow and the agent proposes adjustments, the rule engine catches anything that would break the books.

To see what's actually being checked:

```bash
curl "http://localhost:8000/v1/graphs/$GRAPH_ID/rules" \
  -H "X-API-Key: $API_KEY" | jq '.[] | {name, severity, target_kind}' | head -40
```

## The Mapping View

100% of the 27 CoA accounts are mapped to rs-gaap reporting concepts, and 100% of those mappings are **renderable** — they target concepts the active Reporting Style's structures actually walk.

From the frontend, **Chart of Accounts** shows each account with its rs-gaap target alongside. From the MCP:

```text
list-mapping-structures
→ returns the CoA→rs-gaap mapping structure id

get-mapping-summary --mapping_id=<id>
→ {
    "total_coa_elements": 27,
    "mapped_count": 27,
    "unmapped_count": 0,
    "coverage_percent": 100,
    "unreachable_count": 0,
    "confidence_distribution": { "high": 27, "medium": 0, "low": 0, "manual": 0 }
  }
```

Every mapping was machine-generated by the MappingOperator at demo-load time. The 27 accounts are chosen to have unambiguous canonical targets (Operating Cash → CashCashEquivalentsAndShortTermInvestments, Consulting Revenue → RevenueFromContractWithCustomerExcludingAssessedTax, etc.) — useful for showing how the workflow looks when coverage is complete, separately from the harder real-world mapping cases.

## Schedules

The demo creates 6 schedules to show RoboLedger's recurring-entry surface:

| Schedule | Type | Asset | Monthly amount |
|---|---|---|---|
| Computer equipment | Depreciation | $4,800 PP&E, 3-year life | $133.33 |
| Office furniture | Depreciation | $1,500 PP&E, 5-year life | $25 |
| Business insurance | Prepaid amortization | $1,200/year | $100 |
| Software licenses | Prepaid amortization | $300/year | $25 |
| AWS prepay | Prepaid amortization | $600/year | $50 |
| Professional services renewal | Prepaid amortization | $450/year, staggered | $37.50 |

Schedules generate **schedule facts** for each period in their window. Facts before `closed_through` are flagged `historical` (already baked into the closed-period opening balances). Facts at or after the close target are flagged `in_scope` — they're the obligations the close workflow proposes to materialize as actual entries.

To inspect:

```bash
curl "http://localhost:8000/v1/graphs/$GRAPH_ID/schedules" \
  -H "X-API-Key: $API_KEY" | jq '.[] | {id, name, classification, monthly_amount}'
```

## Frontend Tour

Once the demo is loaded, walk through these screens in roboledger-app at `http://localhost:3001`:

| Screen | What to look at |
|---|---|
| **Entity** | The Cascade Advisory Group profile, with fiscal year metadata and a recent activity summary |
| **Connections** | Empty for the synthetic demo (no QB connection — this is the whole point) |
| **Chart of Accounts** | 27 accounts, each with its rs-gaap mapping displayed inline. 100% mapped, 100% renderable. |
| **Transactions** | The full posted event log — invoices, payments, bills, payroll JEs |
| **Ledger / Journal Entries** | Manual JE form and the entry list grouped by transaction |
| **Schedules** | 6 schedules with their facts and in-scope state |
| **Documents** | 4 policy markdown files, searchable |
| **Reports** | The filed FY annual report — click in for the full BS / IS / CF / SE rendering with Information Block drill-downs |
| **Close** | The fiscal calendar view + the one queued period ready to close + blocker payload (in_scope schedule facts) |
| **Inbox** | Empty for the demo (events auto-commit for QB-style sources; demo events do the same) |
| **Agents** | The 17 parties: customers, vendors, employees, IRS |

The "Close" screen is the most product-distinctive surface — drives the AI-assisted close workflow and surfaces all the period's outstanding items in one place.

## Exploring with MCP

For agent-driven exploration, the demo is the easiest entrypoint to the RoboLedger MCP server. Connect Claude Desktop (or any MCP client) to the local server (`http://localhost:8000/v1/mcp`) and try:

```text
"What entities are in this graph?"
→ get-graph-info

"Show me the chart of accounts grouped by classification"
→ read-graph-cypher with a MATCH (e:Element) ... query

"How many transactions per year?"
→ read-graph-cypher with a date_part('year', t.date) aggregation

"What's the close status for the target period?"
→ get-period-close-status

"Search for the depreciation policy"
→ search-documents with "depreciation"

"Build a fact grid showing year-over-year revenue"
→ build-fact-grid

"Close the books for the most recent completed month"
→ multi-step workflow: get-period-close-status, list-period-drafts,
  preview-event-block, create-event-block, evaluate-rules, close-period
```

The full prompt for the AI close workflow lives at `examples/roboledger_demo/prompt.md` — paste it into the client to load the workflow context, then ask the close question.

## Resetting the Demo

To re-run cleanly:

```bash
# Re-run — the demo is idempotent and always wipes prior state on the tenant before loading
just demo-roboledger

# Or fully reset everything (drops local data + auth)
just reset-local
```

`just demo-roboledger` always runs the demo's `_reset.py` first to clear prior events, transactions, line items, mappings, schedules, documents, fiscal calendar, and reports from the tenant, then re-loads the full data set. Useful for getting back to a known-good state after experimenting with the close workflow or breaking something with manual entries.

`reset-local` is heavier — drops the entire local stack's data including all users, graphs, and credentials. Use when you want a fresh slate.

## Customization

The demo's content lives in `examples/roboledger_demo/`:

- `data.py` — event generators (the 305 transactions). Modify to change the business shape, transaction patterns, monthly volumes, or vendor / customer lists.
- `mappings.py` — the CoA → rs-gaap mapping table. Change targets to demo different statement compositions.
- `policies.py` — the four policy documents loaded as searchable content.
- `agents.py` — the 17 parties (customers, vendors, employees, government).
- `validate.py` — the 38 rules. Add or modify checks to demo different validation scenarios.
- `main.py` — the orchestration script. Idempotent (unconditionally clears prior tenant state before loading); re-running picks up your edits.

Common customizations:

- **Change the company name and shape** — edit `data.py` `COMPANY_NAME` and the revenue / expense generators
- **Add a failing rule** — modify `validate.py` to introduce a deliberate violation, then run `evaluate-rules` to see it caught
- **Demo a different industry** — replace the consulting revenue generators with retail, SaaS, or manufacturing patterns
- **Test multi-year history** — extend the rolling window beyond 16 months in `data.py`

## Comparison to the QuickBooks Workflow

| Aspect | Synthetic Demo | QuickBooks Local |
|---|---|---|
| Setup time | One command (~60 sec) | 5–15 minutes (Intuit + ngrok config) |
| External dependencies | None | Intuit Developer + ngrok + QBO subscription |
| Data shape | Designed for the product — clean, 100% mappable, balanced | Real-world messy — partial mapping coverage, edge-case transaction types |
| Use for | Product evaluation, regression testing, agent demos, learning the feature set | Real-data smoke testing, your own books, adapter debugging |
| Iteration speed | Fast — re-running the recipe wipes and reloads | Slow — re-sync takes minutes, data isn't deterministic |
| Mapping coverage | 100% renderable on first run | Variable — auto-map agent runs, partial coverage typical, manual remap for edge cases |

Both paths produce the same OLTP shape, the same materialized graph, and the same Report-Block lifecycle. The demo is for showing the product; QB is for using it on your own books. See the [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally) guide for the QB workflow.

## Seattle Method XBRL Demos

Beyond the synthetic demo, RoboLedger ships two demos built on Charlie Hoffman's **published [Seattle Method](https://github.com/seattlemethod/prototypes) reference datasets**. Where the synthetic demo shows the product surface, these prove **cross-taxonomy projection** and **round-trip XBRL fidelity**: RoboLedger ingests an external XBRL reporting taxonomy and a ledger tagged against it, decomposes each balance's period change via `rollforward` Information Blocks, renders the result in *both* the source vocabulary and our canonical `rs-gaap`, exports an XBRL 2.1 + JSON-LD bundle, and reconciles the output against Charlie's published reference.

Both exercise the three-block architecture end to end — the **Taxonomy Block** (load the external taxonomy as a Chart of Accounts), the **Event Block** (ingest the tagged journal entries), and the **Information Block** (author the rollforwards and render the statements). See [Information Blocks](Information-Blocks), [Taxonomy & Frameworks](Taxonomy-and-Frameworks), and [Event-Driven Ledger](Event-Driven-Ledger).

### Lemonade Stand (Test Case 1)

Charlie's 14-journal-entry Q1 2024 lemonade-stand dataset, tagged against his `mini` reporting framework — the smallest end-to-end proof of the methodology.

```bash
# Provision → load mini taxonomy → seed mini→rs-gaap mappings → ingest 14 JEs →
# author rollforwards → reconcile → materialize the 4-statement report.
just demo-seattle-method

# A single step (pull | load | seed-mappings | ingest | author-rollforwards | reconcile | create-report)
just demo-seattle-method --step ingest

# Re-render just the artifacts against an existing graph
just demo-seattle-method-reconcile <graph_id>
just demo-seattle-method-create-report <graph_id>
```

It produces 239 `mini` concepts loaded as Elements, ~36 `mini → rs-gaap` mapping Associations, 14 Events/Entries (~32 LineItems) each carrying its business-event flow tag, and 8 `rollforward` Information Blocks (one per balance-sheet leaf with activity). The run then renders a line-by-line reconciliation against Charlie's published facts, the rs-gaap 4-statement Report (BS / IS / CF / SE), and a JSON-LD + XBRL 2.1 bundle — validated on the host with **Arelle** (XBRL 2.1 structural parity) and **pySHACL** (against `frameworks/ontology/v1/shapes.ttl` for semantic parity).

### The World Online (at scale)

The scaled-up sibling — Charlie's *The World Online* dataset: **22,288 GL lines across 3,389 journal entries**, a real 239-account chart of accounts, and opening balances, tagged against the **MINI 2026** framework. Same methodology, realistic mid-size-company scale.

```bash
# Full pipeline. The 3,389-entry ingest is the intended per-entry path and takes a
# few minutes, with a live progress readout.
just demo-world-online

# Smoke-test the whole pipeline on a 50-entry subset first
just demo-world-online --limit 50

# Re-render individual artifacts against an existing graph
just demo-world-online-reconcile <graph_id>            # GL pivot vs the published summary
just demo-world-online-create-report <graph_id>        # rs-gaap 4-statement report
just demo-world-online-trial-balance <graph_id>        # trial balance (debits = credits)
just demo-world-online-statement-reconcile <graph_id>  # statement anchor totals vs the reference instance
```

Beyond the lemonade stand it adds a real chart of accounts collapsed to `mini` line items (the GL account and provenance preserved on each LineItem's metadata), a trial balance rendered via the `trialBalance` GraphQL query, and two reconciliations — a cell-by-cell `(line-item × business-event)` pivot against the published transaction summary, and a statement-level reconciliation of the four-statement anchor totals against the reference instance.

Its load-bearing design decision is **opening balances as ordinary transactions**: the 12/31/2023 beginning-balance lines flow through `Transaction → Entry → LineItem` like any other entry, tagged with an `OpeningBalance` flow concept, so the rollforward engine attributes them as the genesis event rather than synthesizing an opening number outside the pipeline.

> Both demos classify every reconciliation delta by owner — *matching*, *methodology gap*, *our bug*, or *source-data quality* — so a report with mixed categories is more informative than an all-green one: it makes explicit what each side owns.

## Next Steps

Once you're comfortable with the demo:

- **Connect a real QB company** — see [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally) to layer real data on top of the same workflow
- **Customize the demo** — modify `data.py` to demo a different vertical for a specific prospect
- **Extend the rule engine** — add domain-specific validation rules in `validate.py`
- **Build a custom MCP workflow** — the close prompt at `examples/roboledger_demo/prompt.md` is one starting point; you can build vertical-specific agent workflows on the same MCP surface
- **Go deeper on the model** — [RoboLedger Operations](RoboLedger-Operations) (command writes and the fact grid), [Reporting & Rendering](Reporting-and-Rendering) (how statements are built), and [Information Blocks](Information-Blocks) (the unifying envelope)

If you're evaluating RoboLedger for a real engagement: run the demo, drive the AI close, inspect the rule engine output, then connect one of your own QB companies via the QB Local guide. The full evaluation arc takes ~30 minutes if everything's smooth.
