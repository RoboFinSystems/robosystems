# RoboLedger Demos

The RoboLedger demos load synthetic books into a graph and exercise ingest, mapping, period close, reporting, and XBRL export, with no QuickBooks connection. They come in two flavors:

- **Synthetic data** (`just demo-roboledger`) — a fully-populated tenant generated from synthetic books: 16 months for a consulting firm, a filed annual report, a mapped Chart of Accounts, schedules, and one period queued for the AI-driven close. The fastest way to see RoboLedger end-to-end (a few minutes), and the canonical surface for prospect demos, frontend evaluation, and regression testing. **This page walks through it in detail.**
- **Showcase scenarios** (`just demo-coffee-roaster`, `just demo-saas-startup`) — the same scenario engine pointed at two businesses with a story: Driftline Coffee (profitable but cash-poor) and Cadence Labs (burning cash on a deferred-revenue runway). Use these when the *shape of the numbers* is the point. See [Showcase Scenarios](#showcase-scenarios) below.

Connecting a *real* QuickBooks company (sandbox or production) is a live OAuth connection rather than a demo — see [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally).

The synthetic demo needs no QuickBooks account, Intuit Developer setup, or ngrok tunnel. It emulates data arriving the same way a real QB sync produces it — same typed event vocabulary, same OLTP shape, same materialized graph — without the OAuth and external-API requirements.

## On the hosted platform

Both the Cascade demo and the showcase scenarios load into a graph on the hosted platform as well: every step but the reset is an ordinary API call. Provision a fresh RoboLedger graph there first — neither demo creates a user or a graph off-local, so the graph id is required — and save the account's API key where the run looks for it, a per-host file so a remote graph id can never leak into a local run:

```bash
echo "{\"api_key\": \"$ROBOSYSTEMS_API_KEY\"}" > .local/config.api-robosystems-ai.json
DEMO_API_URL=https://api.robosystems.ai just demo-roboledger <graph_id>
DEMO_API_URL=https://api.robosystems.ai just demo-coffee-roaster <graph_id>
```

The reset is a direct database delete and never runs off-local, so use a graph with nothing in it — one graph per demo: re-running over demo data duplicates it rather than replacing it. `--skeleton` is local-only, since it exists to create an empty graph.

The rest of this page runs against your own stack.

## Table of Contents

- [On the hosted platform](#on-the-hosted-platform)
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
- [Showcase Scenarios](#showcase-scenarios)
- [Next Steps](#next-steps)

## Overview

When `just demo-roboledger` finishes (a few minutes), the tenant has:

- A new entity (or one provisioned into an existing graph) modeling **Cascade Advisory Group LLC**, a 3–5 person boutique consulting firm
- **16 months** of business data on a rolling window ending at the current month, so the demo always covers "recent history" regardless of when you run it
- A clean 27-account Chart of Accounts, fully mapped to rs-gaap
- ~305 typed business events (`invoice_issued`, `payment_received`, `bill_received`, `bill_paid`, `journal_entry_recorded`) emulating data arriving via the same HTTP API a real adapter uses
- ~132 discharge chains linking payments to their originating invoices/bills (REA duality)
- A fiscal calendar with the prior month's prior month closed and the last completed month queued — **exactly one period ready to close**
- 6 schedules: 2 depreciation, 4 prepaid amortization, staggered across the window
- 4 policy documents (close procedures, depreciation, prepaid, revenue)
- An FY annual report — generated, packaged, and **filed** as a complete Report-Block lifecycle demonstration

After setup, the natural flow is:

1. Open the frontend, browse the entity, mapping coverage, and the filed annual report
2. Drop into the AI close workflow on the queued period from Claude, ChatGPT or any MCP client
3. Inspect the verification output — the rules attached to each Information Block, plus the rule tallies the close itself returns
4. Query the graph directly to explore the data shape

## Prerequisites

Just the local stack:

```bash
git clone https://github.com/RoboFinSystems/robosystems.git
cd robosystems
just start
```

You don't need Intuit credentials, ngrok, or any external service. The demo is fully self-contained and runs entirely against the local stack.

If you want to drive the AI close workflow at the end, you'll also want an MCP client (Claude, ChatGPT, Cursor, or a custom client). That's optional — everything else inspects via the frontend or the API.

## Quick Start

```bash
# In one terminal — start the backend stack if you haven't already.
cd robosystems
just start

# In another terminal — start the frontend (optional, but recommended for the tour).
cd ../roboledger-app
npm run dev

# Run the demo. A few minutes.
cd ../robosystems
just demo-roboledger
```

Output includes the API key + user credentials and the graph id of the new tenant — copy them or read them from `.local/config.json`. It ends with the period queued for close, the filed report's id, and nine numbered prompts to try from your MCP client, written for that period: orient on the fiscal calendar, find the close procedures, draft the closing entries, an optional one-off disposal, then review the drafts, close the period, and read the balance sheet.

To load the demo into an existing graph (e.g. on a user / graph you've already provisioned):

```bash
just demo-roboledger <graph_id>
```

To give the entity another legal form — which picks the equity-form Reporting Style the statements render with — pass `--entity-type` (with `=`):

```bash
just demo-roboledger --entity-type=llc   # corporation (default) | partnership | llc
```

Each form gets its own slot in `.local/config.json`, so it builds a separate graph rather than overwriting the corporation one.

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
| **CoA → rs-gaap mappings** | 27 | 100% coverage. 24 are renderable under the active Reporting Style; 3 PP&E-gross targets are unreachable — see [The Mapping View](#the-mapping-view) |
| **Fiscal calendar** | 1 | `closed_through = month_before_last`, `close_target = last_completed_month` |
| **Fiscal periods** | 16 | One per month, first 14 closed, last 2 open |
| **Schedules** | 6 | 2 depreciation (computer equipment, office furniture) + 4 prepaid amortization (business insurance, its year-two renewal, software subscription, cloud hosting) |
| **Schedule facts** | mixed | Historical (pre-target, baked into opening balances) vs in_scope (target onward, surfaced by the close workflow) |
| **Documents** | 4 | Markdown policies: close procedures, depreciation, prepaid, revenue recognition |
| **Filed FY report** | 1 | Annual report — facts generated, package assembled, lifecycle stamped `filed`. Demonstrates the full Report-Block lifecycle end-to-end. |
| **Verification rules** | ~134 | Not authored by the demo — auto-generated. One `SumEquals` proof per schedule, plus the `RollUp` / `EqualTo` / `Derive` rules that ship with the seeded rs-gaap statement structures. Count them yourself in [The Rule Engine](#the-rule-engine). |

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
GRAPH_ID=$(jq -r '.graphs.cascade_demo.graph_id' .local/config.json)   # the demo's slot; see below

# `.graphs` maps a demo slot name to {graph_id, graph_created_at}. `just demo-roboledger`
# writes the `cascade_demo` slot (`cascade_demo_<entity_type>` when you pick another
# entity type with `--entity-type`); `--skeleton` writes `roboledger_skeleton`.

# List reports — reads go through the graph-scoped GraphQL endpoint.
# Note the double nesting: the `reports` root field returns a ReportList,
# whose own `reports` field holds the rows.
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ reports { reports { id name filingStatus periodEnd } } }"}' | jq '.'

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

The natural way to drive the close is via the AI workflow. With Claude, ChatGPT or any MCP client connected to the local RoboSystems MCP server:

1. Paste the prompt from `examples/roboledger_demo/prompt.md` (or work through the numbered prompts the setup script prints at the end)
2. Ask: *"Close the books for the most recent completed month."*

Two checklists are in play, and it's worth keeping them apart.

**The tenant's accounting procedure** is a document loaded into the graph — `search-documents` for "close procedures" finds it. It is the human-readable policy for *this* company, and for Cascade Advisory Group it runs:

1. **Bank reconciliation** — reconcile Operating Checking to the statement
2. **Accounts receivable review** — invoices sent, AR aging, unbilled revenue
3. **Accounts payable review** — vendor bills entered, payroll taxes current
4. **Depreciation** — post the two fixed-asset schedules ($158.33/month combined)
5. **Prepaid expense amortization** — post the active prepaid schedules
6. **Review trial balance** — debits equal credits, compare to prior month
7. **Generate financial statements** — income statement, balance sheet, mapping rollups
8. **Period close** — close the fiscal period once everything is posted

**The executable tool sequence** is a different thing: the generic MCP workflow that actually drives the close. Don't reconstruct it from this page — ask the server. `get-close-playbook` returns it, version-locked to the deployed build, so it can never drift from the tool names and schemas it describes. As of the current build the recurring sequence is:

1. `get-fiscal-calendar` — orient: `closed_through`, `close_target`, `closeable_now`, blockers
2. `sync-connection` — only if the blockers include `sync_stale`; then poll until it clears
3. `get-period-close-status` — which schedules are pending / drafted / posted, and for how much
4. `promote-obligations` (`dispatch_handlers=true`) — drafts every matured schedule's closing entry in one sweep. **This is how schedule drafts come into being; there is no `create-closing-entry` tool.** Manual one-offs go through `create-event-block` with `event_type='journal_entry_recorded'`.
5. `list-period-drafts` — review every draft with DR/CR detail and the `all_balanced` check
6. Summarize to the user and get explicit approval
7. `close-period` — atomic: posts the drafts, runs the balance-sheet equation check, stamps the period's statement FactSets, advances `closed_through`. The close runs on the worker, so a heavy month can outlast the call: a response of `status: "in_progress"` with an `operation_id` means it is running, and is never a reason to call again — poll `get-period-close-status` until `period_status` reads closed and `close_receipt` fills in
8. Post-close verification — read the receipt back: `entries_posted`, `statements_stamped`, `rule_summary`, `statement_rule_summary`

The playbook also covers the steps a clean demo skips — reconciling items from a sync, unmapped accounts, schedules that end early.

**Account reconciliations.** The demo sets none up, so its close is not gated on them. To try them, ask for a refresh of the reconciliations for the target period (`refresh-reconciliations`): it compares each asset account a schedule carries a balance on with its schedules, and gives each its own reconciliation. One that ties on that first comparison starts out required for close, so from then on the close is blocked with `unreconciled_accounts` until it reconciles for the period (and is signed off, where its policy asks for review); `set-reconciliation-policy` changes that per reconciliation. Two other blockers can appear on a real ledger: `reconciling_items` and `unposted_source_events`.

Note what is *not* in it: `evaluate-rules` is not a close step. The close runs the rules itself and hands the tallies back on its own response (see [The Rule Engine](#the-rule-engine)).

Each step exercises a different RoboLedger surface: documents, schedules, journal entries, verification, period management, reports. The agent's run is auditable — every action is a typed operation in the event log, replayable on demand.

If you'd rather drive manually, the same operations are exposed via REST under `/extensions/roboledger/{graph_id}/operations/`.

## The Rule Engine

Rules are not a global corpus you run against the tenant. **Every rule hangs off an Information Block** — it is part of the block's envelope, alongside the block's elements, facts, and verification results. That is the single most important thing to internalize about verification in RoboLedger, and it's why there is no "list all rules" endpoint.

The demo authors none of them. They arrive two ways:

- **One `SumEquals` proof per schedule.** Give a schedule a cost basis (`original_amount`) and the platform generates the rule that proves the periodic amounts sum back to it. All six demo schedules carry one.
- **The seeded rs-gaap statement structures.** Balance sheet, income statement, cash flow and equity blocks arrive from the taxonomy library with their `RollUp` and `EqualTo` rules already attached — the calculation arcs, expressed as verifiable rules. The `Derive` rules on those blocks (and on the metric block) are the other half of the same grammar: same `$Variable` expressions, evaluated to *compute* a value rather than to check one, which is what `compute-metrics` runs.

To see them, walk the blocks:

```bash
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ informationBlocks(limit: 1000) { name blockType rules { rulePattern ruleSeverity ruleOrigin } } }"}' \
  | jq '[.data.informationBlocks[].rules[]] | group_by(.rulePattern)
        | map({pattern: .[0].rulePattern, count: length})'
```

`informationBlocks` returns 50 blocks unless you pass `limit` (up to 1000), which is why the query sets one. On a freshly loaded demo tenant it returns roughly 134 rules: ~88 `RollUp`, ~30 `Derive`, ~10 `EqualTo`, and the 6 schedule `SumEquals` proofs. The exact numbers move with the seeded rs-gaap library, which is why the query above is the answer rather than a table on this page.

The pattern vocabulary is broader than what the demo happens to exercise — `Adjustment`, `CoExists`, `Derive`, `EqualTo`, `Exists`, `GreaterThan`, `GreaterThanOrEqualToZero`, `LessThan`, `RollForward`, `RollUp`, `SumEquals`, `Variance`. Of those, `EqualTo` / `RollForward`, `RollUp`, `Exists`, `CoExists`, and `SumEquals` have evaluators today; the rest return `skipped` rather than failing.

### Evaluating a block

`evaluate-rules` is scoped to **one** Information Block. It takes a `structure_id` — posting an empty body returns a 422:

```bash
# Pick a block — here, the first of the six schedules
BLOCK_ID=$(curl -s -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ informationBlocks(blockType: \"schedule\") { id name } }"}' \
  | jq -r '.data.informationBlocks[0].id')

curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/evaluate-rules" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"structure_id\": \"$BLOCK_ID\"}" | jq '.result.summary'
```

The response is an `OperationEnvelope` whose `result` is `{structure_id, results[], summary}`. `summary` is a count keyed by outcome — `{"pass": N, "fail": N, "error": N, "skipped": N}` — and `results` carries one row per rule. Optional `period_start` / `period_end` narrow the fact-binding window; without them the engine binds the most recent in-scope fact per element.

### What the close runs for you

You rarely call `evaluate-rules` by hand during a close. `close-period` runs the engine itself — over every schedule structure with facts in the closed period, and over the statement blocks it just stamped — and returns the tallies on its own response:

- `rule_summary` — the schedule-rule tally (`pass` / `fail` / `error` / `skipped`), with `evaluated_structure_ids` naming the blocks
- `statement_rule_summary` — the same tally across the stamped canonical statements

That is the verification surface an operator actually reads: the close reports what passed, not a separate validation pass run beforehand. The demo data is intentionally clean, so the value here is seeing the *shape* of the output — once the agent starts proposing adjustments, these are the numbers that catch anything that would break the books.

## The Mapping View

All 27 CoA accounts are mapped to rs-gaap reporting concepts. **Coverage and renderability are two different measurements**, and the demo is deliberately set up to show the gap: 27 of 27 are mapped, 24 are renderable, and 3 target concepts the active Reporting Style's structures don't walk.

From the frontend, **Chart of Accounts** shows each account with its rs-gaap target alongside. From the MCP:

```text
list-mapping-structures
→ returns the CoA→rs-gaap mapping structure id

get-mapping-summary --mapping_id=<id>
→ {
    "mapping_id": "struct_…",
    "total_coa_elements": 27,
    "mapped_count": 27,
    "unmapped_count": 0,
    "coverage_percent": 100.0,
    "confidence_distribution": { "high": 27, "medium": 0, "low": 0, "manual": 0 },
    "unreachable_count": 3,
    "unreachable": [
      { "coa_code": "1300", "coa_name": "Computer Equipment",
        "target_qname": "rs-gaap:PropertyPlantAndEquipmentGross" },
      { "coa_code": "1310", "coa_name": "Office Furniture",
        "target_qname": "rs-gaap:PropertyPlantAndEquipmentGross" },
      { "coa_code": "1350", "coa_name": "Accumulated Depreciation",
        "target_qname": "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment" }
    ]
  }
```

The three unreachable mappings are the PP&E-gross pair and its contra — correct mappings at a grain the active Reporting Style presents net, so they never surface as their own statement lines. That's a real modelling situation, not a defect, and it's exactly the case `unreachable_count` exists to make visible.

By default the mappings come from the hardcoded table in `examples/roboledger_demo/mappings.py`. Pass `--ai` to have the **MappingOperator** generate them instead (requires Bedrock). The 27 accounts are chosen to have unambiguous canonical targets (Operating Cash → CashCashEquivalentsAndShortTermInvestments, Consulting Revenue → RevenueFromContractWithCustomerExcludingAssessedTax, etc.) — useful for showing how the workflow looks when coverage is complete, separately from the harder real-world mapping cases.

## Schedules

The demo creates 6 schedules to show RoboLedger's recurring-entry surface:

| Schedule | Type | Basis | Monthly amount |
|---|---|---|---|
| Computer Equipment Depreciation | Depreciation | $4,800 PP&E, 36-month life | $133.33 |
| Office Furniture Depreciation | Depreciation | $1,500 PP&E, 60-month life | $25.00 |
| Business Insurance | Prepaid amortization | $1,200 annual policy, 12 months | $100.00 |
| Business Insurance (Year 2 Renewal) | Prepaid amortization | $1,200 renewal, 12 months, starts after year one runs out | $100.00 |
| Software Subscription | Prepaid amortization | $300 annual license, 12 months | $25.00 |
| Cloud Hosting (AWS Savings Plan) | Prepaid amortization | $600 annual commitment, 12 months | $50.00 |

The start offsets and useful lives are staggered on purpose, so the set covers every case: schedules that straddle the `closed_through` boundary, a year-one policy that has already run out before it, and the year-two renewal that starts after it. That mix is what makes the first close draw only the months a real bookkeeper would still owe.

Schedules generate **schedule facts** for each period in their window. Facts before `closed_through` are flagged `historical` (already baked into the closed-period opening balances). Facts at or after the close target are flagged `in_scope` — they're the obligations the close workflow proposes to materialize as actual entries.

A schedule is an Information Block with `blockType: "schedule"`, so you inspect it the same way you inspect any other block:

```bash
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ informationBlocks(blockType: \"schedule\") { id name blockType } }"}' | jq '.'
```

For the mechanics behind a single schedule — the debit/credit template, the cost basis, the generated periods — call the `get-information-block` MCP tool with the block's `id`.

## Frontend Tour

Once the demo is loaded, walk through these screens in roboledger-app at `http://localhost:3001`:

| Screen | What to look at |
|---|---|
| **Home** | The landing dashboard for the tenant |
| **Entity → Entity Info** | The Cascade Advisory Group profile, with fiscal year metadata and a recent activity summary |
| **Entity → All Entities** | Every entity on the graph — one, for this demo |
| **Entity → Connections** | Empty for the synthetic demo (no QB connection — this is the whole point) |
| **Agents** | The 17 parties: customers, vendors, employees, IRS |
| **Ledger → Chart of Accounts** | 27 accounts, each with its rs-gaap mapping displayed inline |
| **Ledger → Inbox** | Empty for the demo (events auto-commit for QB-style sources; demo events do the same) |
| **Ledger → Journal** | Every journal entry — invoices, payments, bills, payroll JEs — with the source transactions as a secondary view |
| **Ledger → Trial Balance** | Debits and credits by account; balances to the cent |
| **Ledger → Statements** | Live statements rendered from the current ledger, without waiting on a report |
| **Ledger → Closing Book** | The fiscal calendar view + the one queued period ready to close + blocker payload (in_scope schedule facts) |
| **Reports → View Reports** | The filed FY annual report — click in for the full BS / IS / CF / SE rendering with Information Block drill-downs |
| **Reports → Create Report** | The report-authoring path that produced it |
| **Reports → Publish Lists / Blocked Senders** | Distribution controls for a filed report |
| **Explorer** | Ad-hoc traversal of the materialized graph |
| **Plan** | The FP&A surface — forecast blocks and scenarios over the same ledger |
| **Library** | The taxonomy library behind the mappings |
| **Console** | The bar at the bottom of every page: ask about the books, or start with `/do` to have it make a change |
| **Search** | Full-text search across the graph, including the 4 policy documents |

The **Closing Book** screen is the most product-distinctive surface — it drives the AI-assisted close workflow and surfaces all the period's outstanding items in one place.

## Exploring with MCP

For agent-driven exploration, the demo is the easiest entrypoint to the RoboLedger MCP server. MCP is graph-scoped — the connector URL carries the `graph_id`, so point your client at `http://localhost:8000/v1/graphs/$GRAPH_ID/mcp` with an `X-API-Key` header. For Claude Code:

```bash
claude mcp add --transport http robosystems-roboledger \
  http://localhost:8000/v1/graphs/<your-graph-id>/mcp \
  --header "X-API-Key: <your key>"
```

See [AI Operators & MCP](AI-Operators-and-MCP) for the header-less connector form and the stdio bridge. Once connected, try:

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
→ multi-step workflow: get-close-playbook, get-fiscal-calendar,
  get-period-close-status, promote-obligations, list-period-drafts,
  close-period
```

`get-close-playbook` is the one to start with — it returns the canonical sequence, the decisions the operator has to make, and the gotchas that aren't visible from any single tool's schema, all version-locked to the deployed build.

The full prompt for the AI close workflow lives at `examples/roboledger_demo/prompt.md` — paste it into the client to load the workflow context, then ask the close question.

## Resetting the Demo

To re-run cleanly:

```bash
# Re-run — the demo is idempotent and always wipes prior state on the tenant before loading
just demo-roboledger

# Or fully reset everything (drops local data + auth)
just reset-local
```

On your own stack, `just demo-roboledger` always runs the demo's `_reset.py` first to clear prior events, transactions, line items, mappings, schedules, documents, fiscal calendar, and reports from the tenant, then re-loads the full data set. Useful for getting back to a known-good state after experimenting with the close workflow or breaking something with manual entries.

`reset-local` is heavier — drops the entire local stack's data including all users, graphs, and credentials. Use when you want a fresh slate.

## Customization

The demo's content lives in `examples/roboledger_demo/`:

- `data.py` — event generators (the 305 transactions). Modify to change the business shape, transaction patterns, monthly volumes, or vendor / customer lists.
- `mappings.py` — the CoA → rs-gaap mapping table. Change targets to demo different statement compositions.
- `policies.py` — the four policy documents loaded as searchable content.
- `agents.py` — the 17 parties (customers, vendors, employees, government).
- `validate.py` — a self-check run right after the schedules are created, proving the Information Block machinery holds together (FactSets exist, the auto-generated `SumEquals` rules evaluate clean, the GraphQL envelope resolves, an asset disposal computes the right NBV). Everything it creates is throwaway and removed before it returns. It also runs standalone: `uv run python -m examples.roboledger_demo.validate <graph_id>`.
- `download_bundles.py` — renders the exported artifact set for the filed report.
- `main.py` — the orchestration script, and where `COMPANY_NAME` and the schedule definitions live. Idempotent (unconditionally clears prior tenant state before loading); re-running picks up your edits.

Common customizations:

- **Change the company name and shape** — edit `COMPANY_NAME` in `main.py` and the revenue / expense generators in `data.py`
- **Make a rule fail** — schedules generate their `SumEquals` proof from `original_amount`; change a schedule's basis in `main.py` so the periodic amounts no longer sum back to it, then run `evaluate-rules` on that block to see it caught
- **Demo a different industry** — replace the consulting revenue generators with retail, SaaS, or manufacturing patterns
- **Test multi-year history** — extend the rolling window beyond 16 months in `data.py`

## Comparison to the QuickBooks Workflow

| Aspect | Synthetic Demo | QuickBooks Local |
|---|---|---|
| Setup time | One command (a few minutes) | 5–15 minutes (Intuit + ngrok config) |
| External dependencies | None | Intuit Developer + ngrok + QBO subscription |
| Data shape | Designed for the product — clean, 100% mappable, balanced | Real-world messy — partial mapping coverage, edge-case transaction types |
| Use for | Product evaluation, regression testing, agent demos, learning the feature set | Real-data smoke testing, your own books, adapter debugging |
| Iteration speed | Fast — re-running the recipe wipes and reloads | Slow — re-sync takes minutes, data isn't deterministic |
| Mapping coverage | 100% mapped on first run (3 targets unreachable under the active Reporting Style) | Variable — the MappingOperator runs, partial coverage typical, manual remap for edge cases |

Both paths produce the same OLTP shape, the same materialized graph, and the same Report-Block lifecycle. The demo is for showing the product; QB is for using it on your own books. See the [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally) guide for the QB workflow.

## Showcase Scenarios

Cascade Advisory Group is deliberately unremarkable — a healthy consulting firm whose books balance and whose story is "here is the product." Two sibling demos run the same scenario engine against businesses that have a *financial* story, for when the numbers themselves are the point:

```bash
# Driftline Coffee — a roaster that is profitable on paper and short of cash
just demo-coffee-roaster

# Cadence Labs — a SaaS company burning cash against a deferred-revenue runway
just demo-saas-startup
```

Both take the same flags as `just demo-roboledger`: an optional `[graph_id]` to load into an existing graph, `--ai` to map the Chart of Accounts with the MappingOperator instead of the hardcoded table, and `--dry-run` to validate without writing. Either also loads into a graph on the hosted platform — see [On the hosted platform](#on-the-hosted-platform).

There is also a RoboInvestor demo — Meridian Ventures Fund I, including the cross-graph report share between a fund and an issuer:

```bash
just demo-roboinvestor
```

See [RoboInvestor Operations](RoboInvestor-Operations) for what it exercises.

## Next Steps

Once you're comfortable with the demo:

- **Connect a real QB company** — see [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally) to layer real data on top of the same workflow
- **Customize the demo** — modify `data.py` to demo a different vertical for a specific prospect, or start from one of the [showcase scenarios](#showcase-scenarios)
- **Author your own Information Blocks** — `create-information-block` builds `schedule` and `rollforward` blocks, each carrying its own verification rules
- **Build a custom MCP workflow** — the close prompt at `examples/roboledger_demo/prompt.md` is one starting point; you can build vertical-specific agent workflows on the same MCP surface
- **Go deeper on the model** — [RoboLedger Operations](RoboLedger-Operations) (command writes and the fact grid), [Reporting & Rendering](Reporting-and-Rendering) (how statements are built), and [Information Blocks](Information-Blocks) (the unifying envelope)

If you're evaluating RoboLedger for a real engagement: run the demo, drive the AI close, read the rule tallies the close hands back, then connect one of your own QB companies via the QB Local guide. The full evaluation arc takes ~30 minutes if everything's smooth.
