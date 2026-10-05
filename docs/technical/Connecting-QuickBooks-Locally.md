# Connecting QuickBooks Locally

This tutorial connects a QuickBooks Online company to a **self-hosted** RoboSystems stack, from registering an Intuit app to watching the first sync run. It covers the Intuit app, the OAuth callback and tunnel, the environment, the OAuth handshake, and what the sync pipeline does on your machine. On robosystems.ai none of this is needed, because connecting QuickBooks is a button in the app.

What happens *after* a connection exists is the same on either deployment and is documented once, for the hosted API:

- **[QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy)**: sync windows, write policy, the close-time write-back outbox, reconciling items, disconnect vs. sever
- **[Chart of Accounts Mapping](Chart-of-Accounts-Mapping)**: auto-map on first sync, coverage vs. renderability, correcting a mapping
- **[Period Close](Period-Close)**: closing the synced books

## Two Paths for Local Development

There are two ways to develop against QuickBooks data locally. Pick the path that matches what you need:

| Path | Companies | Setup | When to use |
| --- | --- | --- | --- |
| **A. Sandbox + localhost** | Intuit Sample Company (pre-populated canned data) | Minutes — no tunnel, no production keys | Validating the OAuth handshake, exploring the sync flow, demoing the close/report UX without your own books |
| **B. Production + public tunnel** | Your own real QB Online companies | ~15 min mechanical setup **+ a one-time Intuit production-access approval** (app assessment questionnaire — see [Prerequisites](#prerequisites)) | Real-data smoke tests, customer-facing demos against your own QB, debugging adapter edge cases that only surface in messy real data |
| _Neither_ — synthetic data only | N/A | One command | When you don't need Intuit at all — see the [RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough) for the `just demo-roboledger` workflow which generates fake books for testing close-period, reports, and rules without OAuth |

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
- [Inspecting the Graph](#inspecting-the-graph)
- [Troubleshooting](#troubleshooting)
- [Costs and Cleanup](#costs-and-cleanup)
- [Next Steps](#next-steps)

## Overview

When you connect QuickBooks to a local stack, four things happen end to end:

1. **Sync.** A Dagster job pulls the chart of accounts, transactions, customers, vendors, employees and company info from the QuickBooks API, normalizes them with dbt, and loads them into the graph's tenant schema in the extensions database.
2. **Materialize.** A sensor sees the graph marked stale and projects the OLTP data into LadybugDB, the graph backend used for analytical reads.
3. **Map.** On the first sync, the MappingOperator maps the chart of accounts to `rs-gaap` reporting concepts. On a local stack this needs Bedrock credentials. See [Chart of Accounts Mapping](Chart-of-Accounts-Mapping).
4. **Report.** Live statements, the close and saved reports read the synced ledger through that mapping. See [Reporting and Rendering](Reporting-and-Rendering).

This page covers steps 1 and 2 and the setup in front of them. Steps 3 and 4 behave exactly as they do on the hosted platform.

## Prerequisites

### Before you start (both paths)

1. **Clone both repos as siblings** in the same parent folder — this guide `cd`s between them:

   ```bash
   git clone https://github.com/RoboFinSystems/robosystems.git
   git clone https://github.com/RoboFinSystems/roboledger-app.git
   ```

2. **Complete the base local setup first** so `just start` works — see [Local Development](Local-Development) / [Bootstrap Guide](Bootstrap-Guide) (Docker, `uv`, `just`, the generated `.env`, migrations).
3. **Install the frontend deps**: `cd roboledger-app && npm install` (Node 24+; `.nvmrc` pins the exact version). Run the frontend with `npm run dev` on the host for the OAuth step (see the Private-Network-Access note in [Troubleshooting](#troubleshooting)).

### Intuit Developer account (both paths)

1. Sign up at [developer.intuit.com](https://developer.intuit.com/)
2. Create an app under **My Apps → Create an app**
3. Two pages matter for your app:
   - **App Settings → Redirect URIs** — register callback URLs (separate lists for Development and Production)
   - **Keys & OAuth** — grab Client ID + Secret (separate **Development** and **Production** keys)

Intuit treats **Development** and **Production** as separate environments with their own keys, redirect-URI lists, and consent flows. Sandbox companies authenticate against **Development** keys; real companies authenticate against **Production** keys.

### Path A — Sandbox (frictionless)

A QB sandbox company. Intuit auto-creates a "Sample Company" for new developer accounts; add more (up to 5, free) via [developer.intuit.com/app/developer/sandbox](https://developer.intuit.com/app/developer/sandbox). **No approval needed — Development keys work immediately.**

### Path B — Production access (plan ahead: Intuit approval required)

This is the friction Path A doesn't have, and it's the most common reason a first Path-B run stalls. **Development/sandbox keys work instantly; production keys do not** — Intuit gates them behind a one-time **app assessment questionnaire**. Start it *before* you schedule the rest of Path B.

**Where:** in the Developer portal, open your app → switch the environment to **Production** → fill in **App details** → open the **Compliance / App assessment questionnaire** section (also reachable as *Production Settings → App assessment questionnaire*) and **Start questionnaire**. Intuit reviews your answers before releasing production keys.

**Have these ready** (missing any one stalls the questionnaire):

- A public **EULA URL** and **Privacy Policy URL**
- **Host domain**, **launch / sign-in URL**, and **disconnect URL**
- App details (name, description, website, redirect URIs) + app category
- Hosting-location and security/compliance answers; regulated-industry disclosures (only the sections relevant to your app appear)
- Acceptance of the **Intuit Developer Terms / App Partner Program** + a valid payment method on file

**Time:** Intuit estimates **~20–30 minutes** to complete the questionnaire. Production keys are **not granted instantly** — budget for Intuit's review and possible follow-up questions.

**Not the same as publishing to the QuickBooks App Store.** Connecting *your own* companies only needs **production keys** (the questionnaire above). Listing on the public App Store is a separate path with a **technical review that averages ~20 days** — you do **not** need that to self-host and connect your own books.

> **Self-host reality:** the questionnaire expects production-app artifacts — a public EULA, a privacy policy, and hosted URLs — which a pure localhost/tunnel dev setup doesn't naturally have. You'll need at least minimal hosted EULA/privacy pages and a real host or tunnel domain to supply. This is why Path B is "attainable, but not fifteen minutes."

You'll also need a **public tunnel** for the OAuth handshake itself — either an [ngrok](https://dashboard.ngrok.com/signup) account + reserved static domain, OR a Cloudflare-managed domain + `cloudflared` — and a real QB Online company you own.

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

The sandbox company comes pre-populated with synthetic QB data. Connecting starts the first sync on its own, a full-history pull, and when it finishes you'll have ~100 transactions to explore.

### Path B — Real QB (~15 min setup, after one-time Intuit production approval)

```bash
# 1. Get production credentials — REQUIRES one-time Intuit production access:
#    complete the app assessment questionnaire first (see Prerequisites → Path B).
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

> **Before the tunnel:** confirm your Intuit app already has **production access** — that one-time approval (the app assessment questionnaire), not the tunnel, is the long pole on Path B. See [Prerequisites → Path B — Production access](#prerequisites).

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

The callback starts the first sync on its own. To run another, use the Connections page or the API:

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=<your graph id>
CONN_ID=<your connection id>

curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/connections/$CONN_ID/sync" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"full_rebuild": true}'
```

For a connection that has never synced, that first sync pulls full history. `full_rebuild: true` pulls all history, `{}` pulls the last 60 days, and `since_date` pulls from a date. See [QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy#syncing) for what each window does.

The response is a `pending` envelope carrying the Dagster run id as `task_id`. Follow the run in the Dagster UI at [localhost:8002](http://localhost:8002). When it finishes, the outcome is on the connection's `metadata.last_sync_result`.

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
- Marts (`elements`, `transactions`, `entries`, `line_items`, `agents`, `dimensions`) that join the staging tables into the canonical event shape
- Data tests including `assert_accounting_equation` (every transaction balances), `assert_debits_equal_credits` (trial balance check), `assert_unique_identifiers` (no dupes)

If any data test fails, the entire transform fails — surfacing data-quality issues early rather than letting them silently corrupt the OLTP load.

**Step 3: `qb_load`** reads the marts and writes to the tenant's PostgreSQL schema:

- Inserts `Element` rows (Chart of Accounts as graph elements)
- Inserts `Agent` rows (customers, vendors, employees as parties)
- For each transaction, creates an `Event` with `status='captured'` and rich metadata
- Because QuickBooks is the book of record for these transactions, immediately fires the event handler, which creates `Transaction → Entry → LineItem` rows in one transaction and lands the event `fulfilled`. A transaction that can't post stays `captured` in the inbox.
- Flags any already-posted transaction whose payload changed upstream as a reconciling item instead of overwriting it (see [Reconciling Items](QuickBooks-Sync-and-Write-Policy#reconciling-items))
- Records `last_sync` and `last_sync_result` on the connection
- Triggers `_bootstrap_fiscal_calendar_if_needed` (so the close workflow works on a fresh sync)
- Triggers `_trigger_auto_map_if_needed` (enqueues the MappingOperator when the mapping has no associations yet)
- Refreshes the next period's reconciliations on a ledger that already reconciles its accounts (best-effort; see [Period Close](Period-Close#reconciliations))
- Marks the graph stale so the materializer knows there's new data

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

The graph is now queryable via Cypher. Reports, AI Operators, and analytical views read from LadybugDB; the OLTP database remains the source of truth for transactional reads and writes.

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

If you have an MCP client connected to the RoboSystems server (Claude, ChatGPT, or any MCP client):

```bash
# Inspect mapping state
get-mapping-summary --mapping_id <mapping_struct_id>
get-unmapped-elements

# Pull specific facts out of the hypercube (needs elements AND a period
# filter; returns a flat fact list, not a pivot). A ledger's graph is queried
# by element qname: canonical concepts and fiscal-year filters exist only on
# shared repositories such as SEC.
build-fact-grid --elements '["rs-gaap:Assets"]' --period_type instant

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

### Sync fails with `dbt build failed`

Open the Dagster UI at [localhost:8002](http://localhost:8002), find the failed run, drill into the `qb_transform` step's stdout. Common causes:

- **`assert_accounting_equation` test failed** — line items reference an account the extract didn't pull. The most common cause is inactive accounts in QB that the API doesn't return by default; the adapter handles this with `WHERE Active IN (true, false)`, so if you see this on a fresh stack, you may be on a stale image — `just rebuild`.
- **New unseen transaction types** — drop counters in `qb_load` surface in the connection's `last_sync_result.counts` as `dropped_unbalanced_entries` and `dropped_empty_transactions`. Inspect parquet under `/tmp/qb_pipeline/<graph_id>/extract/` to see what came through.
- **Trait coverage gap** — some staging columns expected by the model aren't in the raw extract. Update `flatten_company_info` and the dbt staging model in tandem.

### Statements come out empty or unbalanced after a sync

These are mapping problems, not sync problems, and they look the same on a local stack as on the hosted platform: unmapped accounts with balances, mappings to concepts the Reporting Style doesn't render (`unreachable_count > 0`), or a mapping to a subtotal. Diagnose with `get-mapping-summary` and `get-unmapped-elements`. See [Chart of Accounts Mapping](Chart-of-Accounts-Mapping). On a local stack, also confirm that auto-map actually ran: it needs Bedrock credentials, and without them the first sync leaves the chart unmapped.

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
| Intuit production access | Free — but requires the one-time app assessment questionnaire (~20–30 min + Intuit review) |
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

Once a company is connected and synced, the rest of the workflow is the hosted one, pointed at `http://localhost:8000`:

- **[Chart of Accounts Mapping](Chart-of-Accounts-Mapping)**: check coverage and renderability before anything else
- **[QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy)**: re-syncing, write-back, and settling transactions edited in QuickBooks
- **[Period Close](Period-Close)**: close the synced months, and see what writes back to QuickBooks
- **[Reporting and Rendering](Reporting-and-Rendering)**: live statements and saved reports over the synced ledger
- **[AI Operators and MCP](AI-Operators-and-MCP)**: drive the close from an MCP client
- **[Event-Driven Ledger](Event-Driven-Ledger)** and **[GraphQL Reads](GraphQL-Reads)**: how the synced data is modeled and read

To exercise the same surfaces without Intuit at all, `just demo-roboledger` generates synthetic books. See the [RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough).
