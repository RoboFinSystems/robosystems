# RoboLedger End-to-End Demo

Demonstrates the full RoboLedger workflow — bulk OLTP import, taxonomy & schedule blocks, fiscal calendar, a filed FY 2025 annual report, and an AI-driven month-end close — using synthetic data for a boutique consulting firm (Cascade Advisory Group LLC).

Data is generated on a **rolling 16-month window ending at the current month**, so the demo always covers "recent history" no matter when it's run. All content flows in through the same HTTP API the frontend UI and MCP tools use — emulating data arriving from outside the system the way a real customer's integration would. The only direct-DB path is `_reset.py`, which exists only to clean up between demo re-runs.

The demo also initializes a **fiscal calendar** with `closed_through = month_before_last`, so that `close_target = last_completed_month` — exactly **one period ready to close** on first run. Schedule facts are scoped against this boundary: periods ≤ `closed_through` are flagged `historical` (already reflected in opening balances, ignored by the close workflow), everything after is `in_scope`. This models the real onboarding flow — a business joining mid-year walls off prior periods and starts running the close workflow from a clean boundary.

## Quick Start

```bash
# Make sure the stack is running
just start

# Run the demo setup (creates graph, loads data, creates schedules, uploads policies)
just demo-roboledger

# Or load into an existing graph
just demo-roboledger <graph_id>

# Dry run (validate data only)
just demo-roboledger --dry-run

# Skeleton mode: create user + empty roboledger graph only, then exit.
# Use this to log into the UI and connect a real QuickBooks sandbox manually.
just demo-roboledger --skeleton

# Use the MappingOperator instead of hardcoded mappings (requires Bedrock)
just demo-roboledger --ai
```

The `just demo-roboledger` recipe sets `UV_ENV_FILE={{_local_env}}` so the script picks up `ROBOSYSTEMS_API_URL` and other settings from `.env.local`. `ROBOLEDGER_ENABLED=true` lives in `.env`/`env.local` and is read by the API container at startup — the script doesn't need it set on its own process.

## What Gets Created

| Component | Count | Description |
|---|---|---|
| **Accounts** | 27 | Clean chart of accounts (assets, liabilities, equity, revenue, expenses) |
| **Agents** | 17 | 6 customers + 8 vendors + 2 employees + 1 government (IRS). Same shape a real QB sync produces. |
| **Business Events** | ~305 | Typed event stream emulating data arriving from outside: `invoice_issued` + `payment_received` pairs (with `discharges_event_id` chain), `bill_received` + `bill_paid` pairs, `journal_entry_recorded` for opening balances / payroll / settlements |
| **Discharge chains** | ~132 | `payment_received → invoice_issued` and `bill_paid → bill_received` REA duality links |
| **Mappings** | 27 | CoA → US GAAP reporting concept associations |
| **Fiscal Calendar** | 1 | `closed_through = month_before_last`, `close_target = last_completed_month` |
| **Fiscal Periods** | 16 | One per month in the data window, first 14 marked `closed`, last 2 `open` |
| **Schedules** | 6 | 2 depreciation + 4 prepaid amortization schedules (staggered renewals) |
| **Schedule Facts** | mixed | Historical (pre-target) vs in_scope (target onward) — close workflow only acts on in_scope |
| **Documents** | 4 | Close procedures, depreciation policy, prepaid policy, revenue policy |
| **FY 2025 Report** | 1 | Annual report — generated, packaged, and **filed** as a Plan C capstone (Report Block lifecycle end-to-end). The current period stays queued for the AI close workflow. |

### Event-type vocabulary

The demo emits the same typed vocabulary a QuickBooks sync produces, exercising the REA duality chain:

| Event type | Category | When | Discharges |
|---|---|---|---|
| `invoice_issued` | sales | Customer invoiced (DR AR, CR revenue) | — |
| `payment_received` | sales | Customer pays an invoice (DR cash, CR AR) | `invoice_issued` event_id |
| `bill_received` | purchase | Vendor bill arrives (DR expense/asset, CR AP) | — |
| `bill_paid` | purchase | Bill settled (DR AP, CR cash) | `bill_received` event_id |
| `journal_entry_recorded` | adjustment | Opening balances, payroll, settlements not from external counterparties | — |

## The Company

**Cascade Advisory Group LLC** — a boutique management consulting firm with 3-5 people.

- Revenue: $18K-22K/month across three service lines (consulting, strategy advisory, implementation)
- Key expenses: Payroll ($9,500/mo), rent ($2,500/mo), software ($450/mo), cloud hosting ($180/mo)
- Fixed assets: Computer equipment ($4,800), office furniture ($1,500)
- Prepaids: Insurance ($1,200/yr), software licenses ($300/yr), AWS prepay ($600/yr)

## AI Close Workflow

After running the setup script, use Claude Desktop or any MCP client.

**Quick start**: Paste the prompt from `prompt.md` and say "Close the books for the most recent completed month." The setup script also prints the exact close-month prompt at the end of its run.

### 1. Review Close Procedures

```
"Search for month-end close procedures"
→ search-documents finds the close checklist with 8 steps
```

### 2. Check Period Status

```
"What's the close status for the most recent completed month?"
→ get-period-close-status shows pending schedules for the period
```

### 3. Draft Closing Entries

```
"Draft all closing entries for the most recent completed month"
→ create-closing-entry for each pending schedule (amounts depend on which
  schedules are active in that period — depreciation runs continuously,
  while prepaid insurance / software / cloud hosting each have their own
  12-month cycles and staggered renewal dates):
  - Computer Equipment Depreciation: $133.33
  - Office Furniture Depreciation: $25.00
  - Business Insurance Amortization: $100.00
  - Software Subscription Amortization: $25.00
  - Cloud Hosting Amortization: $50.00
```

### 4. Review Financial Statements

```
"Show me the balance sheet"
→ live-financial-statement returns cumulative BS with all accounts

"Show me the income statement"
→ live-financial-statement returns period IS with revenue and expenses
```

### 5. Verify in the UI

Open RoboLedger at `http://localhost:3001` → select Cascade Advisory Group → Closing Book

## Data Design

All transactions are clean double-entry — every journal entry has balanced DR/CR line items. This differs from QuickBooks General Ledger exports which show one side per account section.

Monthly transaction pattern:
- 1st: Rent payment, software subscriptions, cloud hosting
- 5th: Payroll run (salary + payroll tax + health insurance)
- 8th: Prior month payroll tax deposit
- 10th-18th: Client invoices (3 clients)
- 18th-26th: Client payments
- 15th: Office supplies
- 20th: Travel & entertainment (variable)
- 22nd: Professional development (variable)

## Schedules

Schedules are anchored to month offsets from the demo start date, so they stay aligned with the rolling transaction window. Multiple insurance policies simulate year-over-year renewal:

| Schedule | Start Offset | Life | Monthly | Entry |
|---|---|---|---|---|
| Computer Equipment Depreciation | month 0 | 36 mo | $133.33 | DR Depreciation Exp / CR Accum Depreciation |
| Office Furniture Depreciation | month 2 | 60 mo | $25.00 | DR Depreciation Exp / CR Accum Depreciation |
| Business Insurance | month 2 | 12 mo | $100.00 | DR Business Insurance / CR Prepaid Insurance |
| Business Insurance (Year 2 Renewal) | month 14 | 12 mo | $100.00 | DR Business Insurance / CR Prepaid Insurance |
| Software Subscription | month 5 | 12 mo | $25.00 | DR Software Subscriptions / CR Prepaid Software |
| Cloud Hosting (AWS Savings Plan) | month 8 | 12 mo | $50.00 | DR Cloud Hosting / CR Prepaid Cloud Hosting |

## Files

| File | Purpose |
|---|---|
| `main.py` | Single entry point — creates graph, loads everything via HTTP API |
| `data.py` | Chart of accounts + synthetic transaction generator (evergreen dates) |
| `agents.py` | Seed counterparty agents (customers, vendors, employees) referenced by event-block `agent_id` |
| `mappings.py` | CoA → GAAP mapping definitions |
| `policies.py` | Accounting policy document content (markdown) |
| `prompt.md` | Claude prompt for the close workflow — paste into Claude Desktop |

## Output Artifacts

After the filed FY 2025 Report stamps, the demo downloads both
serialization flavors of the Report bundle via the published Python SDK
(`LedgerClient.download_report_bundle`) into `output/`:

| File | Format |
|---|---|
| `output/roboledger-demo.jsonld` | JSON-LD bundle — the canonical projection of the v1.0 ontology |
| `output/roboledger-demo.zip` | XBRL 2.1 report package — `instance.xml` + `report.xsd` + presentation/calc/definition linkbases |

`output/` is gitignored (each run stamps fresh graph/report IDs).
Committed reference copies of both bundles live in
[`sample_output/`](sample_output/) so a reviewer can inspect a clean
run without spinning up the platform — these are point-in-time
snapshots, not synced every commit. Refresh with
`cp output/roboledger-demo.{jsonld,zip} sample_output/`.
