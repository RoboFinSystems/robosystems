# Connecting QuickBooks Locally

This tutorial walks through connecting a QuickBooks Online company to a local RoboSystems stack and using the full close + reporting workflow against the synced data. It covers two distinct setup paths, the QB sync pipeline architecture, the mapping workflow, and how to generate your first financial report.

## Two Paths for Local Development

There are two ways to develop against QuickBooks data locally. Pick the path that matches what you need:

| Path | Companies | Setup | When to use |
| --- | --- | --- | --- |
| **A. Sandbox + localhost** | Intuit Sample Company (pre-populated canned data) | Minutes — no tunnel, no production keys | Validating the OAuth handshake, exploring the sync flow, demoing the close/report UX without your own books |
| **B. Production + public tunnel** | Your own real QB Online companies | ~15 minutes — tunnel install + Intuit production setup | Real-data smoke tests, customer-facing demos against your own QB, debugging adapter edge cases that only surface in messy real data |
| _Neither_ — synthetic data only | N/A | One command | When you don't need Intuit at all — see the synthetic demo walkthrough (separate guide) for the `just demo-roboledger` workflow which generates fake books for testing close-period, reports, and rules without OAuth |

Both QB paths use the same RoboSystems stack, the same sync pipeline, and the same reporting surface. The only differences are which Intuit OAuth credentials you use and whether the redirect URI points at localhost or a public tunnel.

Path B has two interchangeable tunnel implementations:

| Tunnel | Setup | When to use |
| --- | --- | --- |
| **ngrok** | Free; reserve one `*.ngrok-free.dev` static domain | You don't already own a domain; want the lowest-friction first run |
| **cloudflared** | Requires a domain managed by Cloudflare (cloudflared login, named tunnel, DNS route) | You already own a Cloudflare-managed domain; want your own branded hostname with no interstitial warning |

Pick whichever you prefer — Intuit and the RoboSystems backend don't care which produced the public HTTPS URL.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Path A — Sandbox + Localhost](#path-a--sandbox--localhost)
- [Path B — Production + public tunnel](#path-b--production--public-tunnel)
  - [Option B.1 — ngrok](#option-b1--ngrok)
  - [Option B.2 — cloudflared](#option-b2--cloudflared)
- [Running the Stack](#running-the-stack)
- [Connecting Your First Company](#connecting-your-first-company)
- [What Happens Under the Hood](#what-happens-under-the-hood)
- [Sync Patterns](#sync-patterns)
- [The Mapping Workflow](#the-mapping-workflow)
- [Generating Reports](#generating-reports)
- [Inspecting the Graph](#inspecting-the-graph)
- [Troubleshooting](#troubleshooting)
- [Costs and Cleanup](#costs-and-cleanup)

## Overview

When you connect QuickBooks to RoboSystems, four things happen end-to-end:

1. **Sync** — A Dagster job pulls accounts, transactions, customers, vendors, employees, invoices, bills, payments, and purchases from the QB API, normalizes them with dbt, and loads them into a per-tenant OLTP database.
2. **Materialize** — A sensor detects the stale graph state and projects the OLTP data into LadybugDB, the columnar graph backend used for analytical reads.
3. **Map** — The MappingOperator proposes associations between your QB Chart of Accounts and canonical `rs-gaap:*` reporting concepts. You can accept, remap, or extend these.
4. **Report** — `create-report` generates facts across Balance Sheet, Income Statement, Cash Flow, and Statement of Equity. The renderer walks the active Reporting Style's structures and stamps every reportable fact onto the right line item.

By the end of this guide you'll have all four working against either a QB sandbox or your own real company.

## Prerequisites

Both paths require an Intuit Developer account with a registered app:

1. Sign up at [developer.intuit.com](https://developer.intuit.com/)
2. Create an app under **My Apps → Create an app**
3. Note the two relevant pages for your app:
   - **App Settings → Redirect URIs** — where you register callback URLs
   - **Keys & OAuth → Production Keys** / **Development Keys** — where you grab Client ID + Secret

Intuit treats Development and Production as separate environments with their own keys, their own redirect URI lists, and their own consent flows. Sandbox companies authenticate against Development keys; real companies authenticate against Production keys.

**Path-specific prerequisites:**

| Path | Additional requirements |
| --- | --- |
| **A. Sandbox** | A QB sandbox company (Intuit auto-creates a "Sample Company" for new developer accounts; additional sandboxes via [developer.intuit.com/app/developer/sandbox](https://developer.intuit.com/app/developer/sandbox)) |
| **B. Production** | A public tunnel — either an [ngrok](https://dashboard.ngrok.com/signup) account + reserved static domain, OR a Cloudflare-managed domain + `cloudflared` — and a real QB Online company you own |

## Quick Start

### Path A — Sandbox in five minutes

```bash
# 1. Get sandbox credentials
#    Intuit Developer Portal → your app → Keys & OAuth → Development Keys
#    Copy Client ID and Client Secret

# 2. Register the localhost callback URL
#    Intuit Developer Portal → your app → App Settings → Redirect URIs
#    Switch to "Development" tab → add: http://localhost:3001/connections/qb-callback

# 3. Fill in robosystems/.env
echo 'INTUIT_CLIENT_ID=<sandbox client id>' >> .env
echo 'INTUIT_CLIENT_SECRET=<sandbox client secret>' >> .env
echo 'INTUIT_ENVIRONMENT=sandbox' >> .env
echo 'INTUIT_REDIRECT_URI=http://localhost:3001/connections/qb-callback' >> .env

# 4. Start the stack
just start                                       # backend (terminal 1)
cd ../roboledger-app && npm run dev                # frontend (terminal 2)

# 5. Open localhost:3001 in your browser, log in (just demo-user), navigate to
#    Connections → Connect QuickBooks → sign in with sandbox credentials →
#    pick "Sample Company" → Connect.
```

The sandbox company comes pre-populated with synthetic QB data. After connect, trigger a sync and you'll have ~100 transactions to explore.

### Path B — Real QB in fifteen minutes

```bash
# 1. Get production credentials
#    Intuit Developer Portal → your app → Keys & OAuth → Production Keys

# 2. Set up a public tunnel (pick one):
#    a) ngrok:
brew install ngrok
ngrok config add-authtoken <your-token>
# Then visit dashboard.ngrok.com/domains and reserve a free static domain
#    b) cloudflared (requires a Cloudflare-managed domain):
brew install cloudflared
cloudflared tunnel login
cloudflared tunnel create roboledger-local
cloudflared tunnel route dns roboledger-local qb.your-domain.com

# 3. Register the public callback URL in Intuit Developer Portal
#    App Settings → Redirect URIs → Production tab →
#    add: https://<your-domain>/connections/qb-callback

# 4. Fill in robosystems/.env (production keys + tunnel URL + CORS allowlist)
# 5. Fill in roboledger-app/.env:
#       PUBLIC_TUNNEL_DOMAIN=<your-domain>
#       (cloudflared also needs CLOUDFLARED_TUNNEL_NAME)

# 6. Start the stack (three terminals: backend, frontend, tunnel)
just start                                              # terminal 1
cd ../roboledger-app && npm run dev                       # terminal 2
cd ../roboledger-app && npm run tunnel:ngrok            # terminal 3 (or)
cd ../roboledger-app && npm run tunnel:cloudflared      # terminal 3

# 7. Open the *tunnel URL* (not localhost) → log in → Connections → Connect
#    QuickBooks → pick your real company.
```

The longer-form sections below walk through each step in detail.

## Path A — Sandbox + Localhost

The sandbox path is the simplest local-dev experience. Intuit accepts `http://localhost:3001` as a redirect URI for Development keys, and Chrome doesn't enforce Private Network Access against same-origin localhost calls, so no tunnel is needed.

### Path A — Intuit Developer Portal setup

In your Intuit app:

1. **App Settings → Redirect URIs → Development** tab → add:

   ```text
   http://localhost:3001/connections/qb-callback
   ```

   Save.

2. **Keys & OAuth → Development** tab → copy:
   - Client ID
   - Client Secret

### Path A — Environment configuration

In `robosystems/.env`:

```bash
INTUIT_CLIENT_ID=<development client id>
INTUIT_CLIENT_SECRET=<development client secret>
INTUIT_ENVIRONMENT=sandbox
INTUIT_REDIRECT_URI=http://localhost:3001/connections/qb-callback
```

No additional `roboledger-app/.env` changes needed for sandbox.

`INTUIT_ENVIRONMENT=sandbox` tells the QB API client to hit `sandbox-quickbooks.api.intuit.com` rather than `quickbooks.api.intuit.com`. The wrong value here produces `unauthorized_client` errors that look like credential problems but are environment mismatches.

### Sample Company contents

Each Intuit developer account gets one "Sample Company" sandbox auto-provisioned, pre-populated with ~100 transactions across normal business categories (sales, expenses, payroll, etc.). Useful for end-to-end testing but not customizable — you can't add your own historical data, just void or duplicate existing entries.

For additional sandboxes (e.g. to test on a clean book), use [developer.intuit.com/app/developer/sandbox](https://developer.intuit.com/app/developer/sandbox) → **Add a sandbox company**. Free tier allows up to 5.

## Path B — Production + public tunnel

Required for connecting your own real QuickBooks Online companies. Intuit's Production OAuth flow rejects `localhost` callbacks, so we need a public HTTPS hostname that proxies to your local frontend.

> **The tunnel is only needed for the OAuth handshake itself.** Once the connection is established and tokens are stored, all subsequent work — syncing, mapping, reports, close-period — runs against `localhost:3001` directly. You can shut down the tunnel after each successful connect; only spin it up again when adding another QB company or re-authenticating.

### What the tunnel needs to provide

Intuit's redirect URI must be:

- Publicly resolvable HTTPS (no localhost)
- Registered in advance in the Developer Portal
- Stable across sessions (otherwise you re-register every time)

Both options below satisfy these requirements. They're interchangeable — pick one and follow that sub-section.

### Option B.1 — ngrok

The lowest-friction option if you don't already own a domain. Free tier includes a single static `*.ngrok-free.dev` hostname. Browsers see a one-time interstitial warning page (per session, per browser); paid plans remove it.

#### One-time ngrok setup

```bash
brew install ngrok
```

Sign up at [dashboard.ngrok.com/signup](https://dashboard.ngrok.com/signup), then:

```bash
ngrok config add-authtoken <YOUR_TOKEN>
```

Get the token from [dashboard.ngrok.com/get-started/your-authtoken](https://dashboard.ngrok.com/get-started/your-authtoken).

#### Reserve a static domain

Free tier includes one static domain. Without it, ngrok randomizes the URL on each restart and you'd re-register the redirect URI in Intuit every session.

1. Go to [dashboard.ngrok.com/domains](https://dashboard.ngrok.com/domains)
2. Click **New Domain**
3. ngrok auto-assigns something like `cuddly-otter-1234.ngrok-free.dev`
4. Copy the exact hostname (no `https://` prefix)

#### ngrok — Intuit Developer Portal setup

1. **App Settings → Redirect URIs → Production** tab → add:

   ```text
   https://<your-domain>.ngrok-free.dev/connections/qb-callback
   ```

   Save. Note: this entry is independent of any prod/staging URLs you may have registered for deployed environments.

2. **Keys & OAuth → Production** tab → copy:
   - Client ID
   - Client Secret

#### ngrok — environment configuration

In `robosystems/.env`:

```bash
INTUIT_CLIENT_ID=<production client id>
INTUIT_CLIENT_SECRET=<production client secret>
INTUIT_ENVIRONMENT=production
INTUIT_REDIRECT_URI=https://<your-domain>.ngrok-free.dev/connections/qb-callback

# CORS allowlist for the public origin
EXTRA_CORS_ORIGINS=https://<your-domain>.ngrok-free.dev
```

In `roboledger-app/.env`:

```bash
# Public hostname — read by `npm run tunnel:ngrok` (to know what static domain
# to forward to) and by next.config.js (to wire the dev-server API proxy).
PUBLIC_TUNNEL_DOMAIN=<your-domain>.ngrok-free.dev
```

`PUBLIC_TUNNEL_DOMAIN` is the hostname only (no `https://`).

### Option B.2 — cloudflared

Use this option if you already own a domain managed by Cloudflare (e.g. `robofinsystems.com`). You get a stable, branded hostname (e.g. `qb.robofinsystems.com`), no interstitial warning page, and free DNS routing through Cloudflare's edge.

#### One-time cloudflared setup

```bash
brew install cloudflared
cloudflared tunnel login
```

The login command opens a browser to the Cloudflare dashboard; pick the zone (`robofinsystems.com`) you want to attach the tunnel to. This drops a cert into `~/.cloudflared/cert.pem`.

#### Create a named tunnel and route DNS

```bash
cloudflared tunnel create roboledger-local
cloudflared tunnel route dns roboledger-local qb.robofinsystems.com
```

`tunnel create` provisions a new tunnel + credentials file in `~/.cloudflared/<tunnel-id>.json`. `tunnel route dns` creates a proxied CNAME in your Cloudflare zone so the public hostname resolves to the tunnel.

#### cloudflared — Intuit Developer Portal setup

1. **App Settings → Redirect URIs → Production** tab → add:

   ```text
   https://qb.robofinsystems.com/connections/qb-callback
   ```

   Save.

2. **Keys & OAuth → Production** tab → copy Client ID + Client Secret.

#### cloudflared — environment configuration

In `robosystems/.env`:

```bash
INTUIT_CLIENT_ID=<production client id>
INTUIT_CLIENT_SECRET=<production client secret>
INTUIT_ENVIRONMENT=production
INTUIT_REDIRECT_URI=https://qb.robofinsystems.com/connections/qb-callback

# CORS allowlist for the public origin
EXTRA_CORS_ORIGINS=https://qb.robofinsystems.com
```

`EXTRA_CORS_ORIGINS` is a comma-separated list, so if you keep ngrok configured too, just add the cloudflared host alongside it.

In `roboledger-app/.env`:

```bash
# The public hostname next.config.js uses to set up the dev-server API proxy.
PUBLIC_TUNNEL_DOMAIN=qb.robofinsystems.com

# The cloudflared tunnel name from `cloudflared tunnel create` —
# read by npm run tunnel:cloudflared.
CLOUDFLARED_TUNNEL_NAME=roboledger-local
```

`PUBLIC_TUNNEL_DOMAIN` is the hostname only (no `https://`).

## Running the Stack

The stack runs in your terminals — counts depend on path:

| Terminal | Path A (sandbox) | Path B (production) |
| --- | --- | --- |
| 1 | `cd robosystems && just start` | same |
| 2 | `cd roboledger-app && npm run dev` | same |
| 3 | _(not needed)_ | `npm run tunnel:ngrok` (Option B.1) or `npm run tunnel:cloudflared` (Option B.2) — **only during OAuth establishment**; shut down after each successful connect |

Wait for `just start` to bring up Postgres, Valkey, LadybugDB, the API, and Dagster (the dashboard at [localhost:8002](http://localhost:8002) is a useful sign the stack is up).

`npm run dev` uses Turbopack and is the normal choice. Switch to `npm run dev:webpack` if either of these applies:

- You have the TypeScript SDK installed locally as a symlinked `file:` dependency (`npm install ../robosystems-typescript-client`) — Turbopack doesn't resolve symlinked `file:` deps cleanly. The published SDK from npm works fine with Turbopack.
- **You're on Path B and the tunnel proxy isn't working** — i.e. `/v1/*` calls still hit the public host or 404 even though `PUBLIC_TUNNEL_DOMAIN` is set. The `next.config.js` rewrites that make the tunnel work haven't proven reliable under Turbopack; webpack proxies them deterministically. See the troubleshooting note below.

For Path B, verify the tunnel is reachable:

```bash
# ngrok (Option B.1) — the header skips the interstitial for the curl probe
curl -s -o /dev/null -w "%{http_code}\n" \
  -H "ngrok-skip-browser-warning: true" \
  https://<your-domain>.ngrok-free.dev/login
# Expect: 200

# cloudflared (Option B.2) — no interstitial header needed
curl -s -o /dev/null -w "%{http_code}\n" https://qb.your-domain.com/login
# Expect: 200
```

## Connecting Your First Company

Open the app:

- **Path A**: [http://localhost:3001](http://localhost:3001)
- **Path B**: the tunnel URL — `https://<your-domain>.ngrok-free.dev` or `https://qb.your-domain.com`, **not** localhost (the OAuth redirect URI has to match the user-facing origin)

On Path B with ngrok, click through the interstitial warning page once per browser session on first visit. Cloudflared has no equivalent interstitial.

1. Log in (run `just demo-user` from `robosystems/` for fresh credentials; they're written to `.local/config.json`)
2. Create or select an entity / graph
3. Navigate to **Connections**
4. Click **Connect QuickBooks**
5. Authenticate with the appropriate Intuit account (sandbox for Path A, real for Path B)
6. On the consent screen, pick which QB Online company to connect
7. Click **Connect**

Intuit redirects back to your callback URL with the OAuth `code` parameter. The frontend exchanges it for tokens via the backend, stores them in `connection_credentials`, and creates a `Connection` row scoped to the graph.

Trigger the initial sync from the Connections page UI or via the API:

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=<your graph id>
CONN_ID=<your connection id>

curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/connections/$CONN_ID/sync" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"full_rebuild": true}'
```

`full_rebuild: true` pulls all history from `2000-01-01`. Omit for an incremental 60-day sync.

The response is a `pending` operation envelope with a status URL. The sync runs async in the worker; track progress at [localhost:8002](http://localhost:8002) (Dagster UI) or poll `/v1/operations/{op_id}/status`.

## What Happens Under the Hood

The QB sync is a three-step Dagster job (`qb_sync`):

```text
┌────────────┐     ┌──────────────┐     ┌─────────┐
│ qb_extract │ ──▶ │ qb_transform │ ──▶ │ qb_load │
└────────────┘     └──────────────┘     └─────────┘
       │                  │                  │
   raw parquet         dbt models         OLTP rows
   /tmp/qb_pipeline    DuckDB staging     PostgreSQL
                       + marts            (tenant schema)
```

**Step 1: `qb_extract`** pulls everything via the QB Python SDK:

- `Account` (paginated, active + inactive — historical journal lines may reference deactivated accounts)
- `JournalReport` (the unified transaction stream — all entry types in double-entry shape)
- `Customer`, `Vendor`, `Employee` (the party / agent list)
- `Invoice`, `Bill`, `Payment`, `BillPayment`, `SalesReceipt`, `Purchase` (per-class headers for richer event type detection and agent linkage)
- `CompanyInfo` (entity metadata: name, address, fiscal year, primary contact)

Flatteners normalize the nested Intuit objects into flat dicts and write parquet files under `/tmp/qb_pipeline/<graph_id>/extract/`.

**Step 2: `qb_transform`** runs `dbt build` against the parquet files using DuckDB as the local engine. The dbt project has:

- Staging models (`stg_qb_*`) that cast types and normalize column names
- Marts (`accounts`, `transactions`, `entries`, `line_items`, `agents`, etc.) that join the staging tables into the canonical event shape
- Data tests including `assert_accounting_equation` (every transaction balances), `assert_debits_equal_credits` (trial balance check), `assert_unique_identifiers` (no dupes)

If any data test fails, the entire transform fails — surfacing data-quality issues early rather than letting them silently corrupt the OLTP load.

**Step 3: `qb_load`** reads the marts and writes to the tenant's PostgreSQL schema:

- Inserts `Element` rows (Chart of Accounts as graph elements)
- Inserts `Agent` rows (customers, vendors, employees as parties)
- For each transaction, creates an `Event` with `status='captured'` and rich metadata
- For QB-authoritative connections (default), immediately fires the event handler which creates `Transaction → Entry → LineItem` rows in one transaction
- Triggers `_bootstrap_fiscal_calendar_if_needed` (so close-period UX works on a fresh sync)
- Triggers `_trigger_auto_map_if_needed` (the MappingOperator walks unmapped CoA elements)
- Marks the graph stale (`graph_stale=true`) so the materializer knows there's new data

After `qb_load` returns, the **stale graph sensor** (a Dagster sensor running every 60 seconds) picks up the stale flag, submits an `extensions_materialize_job`, and the materializer projects the OLTP data into LadybugDB:

```text
PostgreSQL                    LadybugDB
(extensions schema)           (graph)
─────────────────────         ──────────────────
events ─────────────────────▶ Event nodes  (inbox)
transactions ──────────────▶  Transaction nodes
entries ───────────────────▶  Entry nodes
line_items ────────────────▶  LineItem nodes
elements ──────────────────▶  Element nodes
associations ──────────────▶  Association nodes
                              + relationships:
                              ENTITY_HAS_TRANSACTION
                              TRANSACTION_HAS_ENTRY
                              ENTRY_HAS_LINE_ITEM
                              LINE_ITEM_RELATES_TO_ELEMENT
                              ASSOCIATION_HAS_FROM_ELEMENT
                              ASSOCIATION_HAS_TO_ELEMENT
```

The graph is now queryable via Cypher. Reports, AI agents, and analytical views read from LadybugDB; the OLTP database remains the source of truth for transactional reads and writes.

## Sync Patterns

| Pattern | Trigger | Date range | Use when |
| --- | --- | --- | --- |
| **Incremental** | UI button or POST without flags | Last 60 days from today | Day-to-day; default |
| **Full rebuild** | `{"full_rebuild": true}` | `2000-01-01` to today | First sync, after schema changes, after fixing an importer bug |
| **Since-date** | `{"since_date": "2024-01-01"}` | From explicit date to today | Catching up after a gap |

The sync is idempotent: events are upserted by `(source, external_id)`, so re-syncing the same data updates rather than duplicates. For full rebuilds, the loader first deletes existing CoA elements and associations scoped to the connection, then re-inserts — safe for QB-authoritative connections where the GL is a mirror of QB.

**Auto-sync** can be enabled on the connection (`auto_sync_enabled=true`); a Dagster schedule then runs incremental syncs on a cadence.

## The Mapping Workflow

Your QB Chart of Accounts uses tenant-specific account names ("Truist Checking 6764", "Consulting Services", "Subscription Services"). Financial reports need canonical concepts (`rs-gaap:CashCashEquivalentsAndShortTermInvestments`, `rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax`, etc.). The mapping workflow bridges the two.

### The MappingOperator

On first sync, RoboSystems auto-triggers the MappingOperator (`_trigger_auto_map_if_needed` in the QB loader). The agent:

1. Reads each unmapped CoA element along with its classification (asset / liability / equity / revenue / expense / gain / loss — derived from QB's `AccountType`)
2. Asks the suggester (`suggest-mapping`) for renderable `rs-gaap:*` candidates that match the classification AND appear on the active Reporting Style's rendering structures
3. Uses the LLM to pick the best candidate, with a confidence score
4. Confidence ≥0.90 → auto-creates the association
5. Confidence 0.70–0.89 → creates the association flagged for review
6. Confidence <0.70 → skips (surfaces as unmapped in the CoA UI)

### Mapping coverage

Inspect via MCP:

```bash
# Through the MCP server (e.g., from Claude Code):
list-mapping-structures              # find the mapping structure id
get-mapping-summary --mapping_id=…   # see coverage stats + unreachable count
get-unmapped-elements                # find CoA elements still needing mapping
```

Or via the API:

```bash
curl "http://localhost:8000/v1/graphs/$GRAPH_ID/mappings/$MAPPING_ID/summary" \
  -H "X-API-Key: $API_KEY"
```

Key metric: **`unreachable_count`** — mappings to rs-gaap concepts that don't render under the active Reporting Style. Should be 0 after a clean auto-map run. If non-zero, either the mappings were created before the renderable filter was added or the Reporting Style structures have changed since.

### Manual remap

When the agent picks the wrong target — e.g., `Precious Metals` mapped to `InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings` instead of `LongTermInvestmentsAndReceivablesNet` — fix via the MCP:

```bash
# Get the source element id
list-mapping-structures
get-unmapped-elements    # or query the CoA UI / database

# Delete the wrong mapping (the REST endpoint /delete-mapping-association)
# Create the correct one via MCP
create-mapping-association \
  --from_element_id elem_… \
  --to_element_id <rs-gaap concept id> \
  --mapping_id struct_… \
  --confidence 1.0 \
  --suggested_by manual_correction
```

The corrected mapping is durable across re-syncs (delete + re-insert of CoA elements doesn't disturb the mappings).

### The Reporting Style and rendering structures

The active **Reporting Style** (`Default Style — Composition` by default; alternatives: `Small Private Company`, `Banking`) determines which rendering structures the renderer walks. The four standard structures are:

- `rs-gaap — Balance Sheet — Classified`
- `rs-gaap — Income Statement — Multi-step`
- `rs-gaap — Cash Flow Statement — Indirect`
- `rs-gaap — Statement of Changes in Equity — Roll Forward (Total)`

Each carries a curated set of rs-gaap concepts (~78 total across the four). The Default Style is filing-grade aggregation — `Cash and Cash Equivalents and Short Term Investments` is one line for all bank accounts; `Selling, General and Administrative Expense` is one line for many expense categories. Detail belongs in supporting Information Blocks below the statement face, not in the statement itself.

## Generating Reports

Once mapping coverage is non-zero, generate a report:

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/create-report" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "FY2025 Annual",
    "mapping_id": "<your mapping structure id>",
    "period_start": "2025-01-01",
    "period_end": "2025-12-31",
    "period_type": "annual",
    "comparative": false
  }'
```

`taxonomy_id` defaults to `"rs-gaap"` — accepts either the standard name (resolved server-side to the latest rs-gaap reporting taxonomy) or an exact tenant UUID. The operation runs synchronously and returns the `Report` with its facts already generated.

To inspect what the report produced:

```sql
-- Facts grouped by statement type
SELECT
  s.block_type AS statement,
  REPLACE(e.qname, 'rs-gaap:', '') AS concept,
  ROUND(f.value::numeric, 2) AS value
FROM facts f
JOIN fact_sets fs ON fs.id = f.fact_set_id
JOIN elements e ON e.id = f.element_id
JOIN structures s ON s.id = fs.structure_id
WHERE fs.report_id = '<report id>'
ORDER BY s.block_type, ABS(f.value) DESC;
```

Expected output: 20–30 facts per annual report, distributed across BS / IS / CF / SE.

**Sanity check**: total assets should equal total liabilities + equity. If off, the most common cause is a CoA account mapped to a flow concept (dividend, distribution) without the close logic netting it into Retained Earnings — see the troubleshooting section below.

## Inspecting the Graph

The materialized graph is queryable via Cypher and via the MCP server's read-graph-cypher tool.

### From the host shell

```bash
# Trial balance check via OLTP
docker exec robosystems-pg psql -U postgres -d extensions -tAc "
  SET search_path TO $GRAPH_ID;
  SELECT
    'debits: '  || SUM(debit_amount)  FROM line_items UNION ALL
  SELECT
    'credits: ' || SUM(credit_amount) FROM line_items;"

# Year-by-year transaction count via Cypher
just lbug-query $GRAPH_ID "
  MATCH (t:Transaction)
  WHERE t.date IS NOT NULL
  RETURN substring(string(t.date), 1, 4) AS year, count(t) AS txns
  ORDER BY year"

# Top accounts by activity volume
just lbug-query $GRAPH_ID "
  MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
  WHERE starts_with(e.qname, 'qb:')
  WITH e.name AS account, count(li) AS lines,
       sum(li.debit_amount) AS dr, sum(li.credit_amount) AS cr
  RETURN account, lines, round(dr, 2) AS debits, round(cr, 2) AS credits
  ORDER BY (dr + cr) DESC LIMIT 10"
```

### From the MCP

If you have an MCP client connected to the RoboSystems server (Claude Code, custom agents, etc.):

```bash
# Set the active graph
switch-workspace --workspace_id <graph_id>

# Inspect mapping state
get-mapping-summary --mapping_id <mapping_struct_id>
get-unmapped-elements

# Build a fact-grid pivot over BS line items
build-fact-grid --period_start 2025-01-01 --period_end 2025-12-31

# Ad-hoc graph query
read-graph-cypher --cypher "MATCH (e:Entity) RETURN e.name, e.fiscal_year_end"
```

The MCP tools are designed for agent-driven workflows — they enforce the same auth + tenant-isolation as the REST API and return structured envelopes the LLM can reason over.

## Troubleshooting

### `redirect_uri_mismatch` from Intuit

The URI you sent in the OAuth init request doesn't match anything registered in the Developer Portal. Verify:

- Spelling is exact (case-sensitive, no trailing slash)
- `https://` for Production, `http://` is fine for Development localhost
- Registered under the matching tab — Development tab for sandbox keys, Production tab for production keys
- Saved (the **Save** button is easy to miss after editing)

### Chrome `Permission was denied for this request to access the 'loopback' address space`

Symptom: login fails silently when you open the ngrok URL in Chrome, console shows:

```text
Access to fetch at 'http://localhost:8000/v1/auth/...' from origin
'https://<your-domain>.ngrok-free.dev' has been blocked by CORS policy:
Permission was denied for this request to access the `loopback` address space.
```

Chrome 146+ enforces Private Network Access on cross-origin calls to localhost. The backend grants it via `allow_private_network=is_development()` on the CORS middleware, but Chrome caches the preflight result aggressively and the usual cache-clearing tricks (DevTools "Disable cache", site-data delete, incognito) often don't actually flush the PNA preflight cache. Chrome 146 also removed the `chrome://flags` override that used to disable PNA.

`roboledger-app`'s `next.config.js` handles this automatically when `PUBLIC_TUNNEL_DOMAIN` is set in `roboledger-app/.env`: rewrites under `/v1/*` and `/extensions/*` proxy through the Next dev server to `http://localhost:8000`, and `NEXT_PUBLIC_ROBOSYSTEMS_API_URL` is overridden to the tunnel origin so the browser only ever talks to one host. From the browser's perspective every request is same-origin, so PNA never fires.

This is **dev-server-only** — it works because `next dev` re-reads `next.config.js` at startup and the `env` override is applied to the bundle in dev mode. Running `roboledger-app` from inside Docker (`docker compose` with the `roboledger-app` service) uses `next start` against a pre-built bundle where `NEXT_PUBLIC_*` is baked at build time, so the override has no effect. **Run the frontend with `npm run dev` on the host for the OAuth connect step.**

**Fallback workaround:** if `npm run dev` isn't an option, **use Firefox for the OAuth connect step**, then go back to Chrome for normal use. Firefox doesn't enforce PNA the same way, so the OAuth callback completes cleanly. Once the connection is established, day-to-day use against `http://localhost:3001` (Path A — same-origin localhost) doesn't trigger PNA and Chrome works fine.

Only affects Path B (public tunnel → localhost) during the OAuth callback moment. Path A is same-origin localhost and bypasses PNA entirely.

### Tunnel proxy not working under Turbopack — `/v1/*` requests fail or 404

Symptom: on Path B the proxy described above is configured correctly — `PUBLIC_TUNNEL_DOMAIN` is set and the `rewrites()` block is present in `next.config.js` — but API calls still fail. Requests to `/v1/*` or `/extensions/*` aren't proxied to `localhost:8000`, so login or the OAuth callback hangs, even though the same config has worked in another run.

The default dev server (`npm run dev`) uses Turbopack, which has been observed not to honor the `next.config.js` `rewrites()` proxy to `http://localhost:8000` reliably during tunnel sessions. Switch to the webpack dev server:

```bash
npm run dev:webpack
```

The rewrites are a standard Next feature and webpack proxies them deterministically. If a tunnel setup that previously worked breaks after a Next upgrade, try webpack before assuming the tunnel or env config is wrong.

### `Invalid email or password` on login (Puppeteer / automation)

If you're driving the login via Puppeteer or similar, ensure the form fill dispatches React `input` events — `puppeteer_fill` alone doesn't trigger React's `onChange` for controlled inputs:

```javascript
const setNativeValue = (el, value) => {
  const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
  setter.call(el, value);
  el.dispatchEvent(new Event('input', { bubbles: true }));
};
setNativeValue(document.querySelector('input[type="email"]'), 'demo_user_…@example.com');
setNativeValue(document.querySelector('input[type="password"]'), '…');
```

### Sync fails with `dbt build failed`

Open the Dagster UI at [localhost:8002](http://localhost:8002), find the failed run, drill into the `qb_transform` step's stdout. Common causes:

- **`assert_accounting_equation` test failed** — line items reference an account the extract didn't pull. The most common cause is inactive accounts in QB that the API doesn't return by default; the adapter handles this with `WHERE Active IN (true, false)`, so if you see this on a fresh stack, you may be on a stale image — `just rebuild`.
- **New unseen transaction types** — drop counters in `qb_load` surface as `unbalanced_entries` and `empty_transactions`. Inspect parquet under `/tmp/qb_pipeline/<graph_id>/extract/` to see what came through.
- **Trait coverage gap** — some staging columns expected by the model aren't in the raw extract. Update `flatten_company_info` and the dbt staging model in tandem.

### Mapping coverage is 100% but reports show empty / mostly-zero values

The mappings may all be **unreachable** — targeting rs-gaap concepts that aren't in the active Reporting Style's rendering structures. Check via `get-mapping-summary`:

```bash
get-mapping-summary --mapping_id <id>
# Look for unreachable_count > 0
```

If unreachable, the MappingOperator ran against an earlier version of the renderable filter. Either:

1. Re-run auto-map (delete the existing mappings first via SQL, then trigger `auto-map-elements`)
2. Manually remap the unreachable accounts to renderable targets via `create-mapping-association`

The renderable filter walks `reporting_style_networks → presentation arcs` to compute the actual set of concepts the renderer will walk. Mappings to concepts outside that set are durable but invisible to the report renderer.

### Report doesn't balance — Assets ≠ Liabilities + Equity

Common causes:

- A CoA account mapped to a calculated subtotal (denylisted concept). The renderer ignores these to prevent double-counting; the account's facts won't show up at all.
- Manual journal entries that don't balance (one debit without matching credit). The trial balance test should catch this in dbt; if it leaked through, query line_items for the offending entry.

### `endpoint already online` / `tunnel already in use` error

A previous tunnel session is still running. Find and kill:

```bash
pkill -f 'ngrok http'         # ngrok
pkill -f 'cloudflared tunnel' # cloudflared
```

Re-launch with `npm run tunnel:ngrok` or `npm run tunnel:cloudflared`.

### Sandbox keys + production redirect URI (or vice versa)

Intuit's Development and Production environments don't share keys or redirect URI lists. A common mistake:

- Sandbox `INTUIT_CLIENT_ID` + Production-tab redirect URI → `redirect_uri_mismatch`
- Production `INTUIT_CLIENT_ID` + Development-tab redirect URI → `redirect_uri_mismatch`

Both halves of the OAuth handshake have to come from the same environment tab.

## Costs and Cleanup

| Component | Cost |
| --- | --- |
| Intuit Developer account | Free |
| Intuit sandbox companies | Free (up to 5) |
| ngrok free tier | $0/month, 1 static domain, occasional browser interstitial |
| ngrok paid plans | ~$10/month, removes interstitial, more domains |
| Cloudflare Tunnel (cloudflared) | $0/month for the tunnel itself; you bring your own domain (~$10/yr) |
| QuickBooks Online subscription | Paid (whatever you already pay for QBO) |

To clean up:

```bash
# Stop the tunnel (whichever you ran)
pkill -f 'ngrok http'
pkill -f 'cloudflared tunnel'

# Stop the stack (preserves data)
cd robosystems && just stop

# Full reset — wipes all local data, drops local databases
just reset-local
```

Disconnecting a QB connection from RoboSystems revokes the OAuth tokens but leaves synced data in place. To remove everything, delete the graph itself from the platform.

Intuit Developer Portal entries (apps, registered redirect URIs, keys) are durable. Keep them around for future sessions — they cost nothing and re-registering is a hassle.

## Next Steps

Now that you have a connected company, real data flowing, and a working report:

- Explore the **close-period** workflow — close the fiscal year, see what blocks the close (open AR, draft entries, missing mappings)
- Try the **rule engine** — `evaluate-rules` runs structural and arithmetic checks against your statements
- Set up the **CloseAgent** — AI-driven proposal of period-end accruals, depreciation, reclassification entries
- Connect a second company and explore **multi-entity reporting**

Each of these is covered in its own guide. For testing these surfaces without a connected QB account, see the synthetic demo walkthrough (which uses `just demo-roboledger` to generate fake books).
