RoboSystems is an open-source, AI-native financial intelligence platform for accounting, financial reporting, and investment management. It gives AI agents and analysts a ledger-grade system of record they can both query and operate — closing the books, producing reports, and analyzing portfolios across accounting, market, and SEC data. The platform powers [RoboLedger](https://roboledger.ai) for accounting and [RoboInvestor](https://roboinvestor.ai) for investment research, with knowledge graphs managed through the [RoboSystems](https://robosystems.ai) app.

## Core Features

- **Unified Graph Architecture**: A transactional core with a materialized analytical graph under one schema and Cypher surface
- **Graph Database**: Build knowledge graphs with LadybugDB for modeling financial relationships and multi-dimensional analytics
- **Multi-Tenant Architecture**: Isolated database instances with tier-based resource allocation
- **AI Operator System**: Autonomous financial Operators (Claude/MCP executors) with automatic credit tracking and SSE progress streaming
- **DuckDB Staging**: High-performance data validation and bulk ingestion pipeline with Parquet optimization
- **Data Integration**: Connect [QuickBooks](https://quickbooks.intuit.com/partners/affiliates?cid=par_pim_4TcakSEFQs73) and SEC XBRL filings in a unified graph
- **Document Search**: Upload, index, and search documents with full-text and semantic search via OpenSearch
- **Shared Repositories**: Access to curated SEC filing data and other shared knowledge graphs
- **Credit-Based Billing**: AI operations consume token-based credits; database and MCP operations are free

## API Modules

### Graph Operations

The core platform surface for querying and managing graphs. Reads are REST `GET`s; every write is a named `OperationEnvelope` operation (`/operations/{op_name}`) with `Idempotency-Key` support, audit logging, and SSE progress at `/v1/operations/{id}/stream`.

**Query and data access:**

- **Query**: Execute Cypher queries with NDJSON streaming for large results
- **Schema**: View node types, relationship types, and property definitions
- **Tables**: DuckDB staging tables — file upload, SQL query, and import workflows
- **Health**: Database connectivity, staleness indicators, and materialization status

**Graph and infrastructure state:**

- **Subgraphs**: List subgraphs, quota, and storage information
- **Backups**: List backups, download URLs, and storage statistics (each backup reports its `download_extension` — `.lbug.zip` or zstd `.lbug.zst`)
- **Usage**: Graph content metrics and consumption usage (storage, credits)

**Lifecycle commands** (`/operations/{op_name}`):

- **create-subgraph**: Initialize a subgraph with optional fork of parent data
- **delete-subgraph**: Remove a subgraph with optional pre-delete backup
- **create-backup**: Encrypted backup with configurable format and retention
- **restore-backup**: Restore from backup (blocked for entity graphs — use `materialize` instead)
- **change-tier**: Change graph infrastructure tier with Stripe billing integration
- **materialize**: Ingest DuckDB-staged tables or OLTP data into the graph (direct or Dagster-orchestrated)

### Documents, Search & Memory

Reads are REST `GET`s; content writes share the same `/operations/{op_name}` envelope as lifecycle commands.

- **Documents**: List and retrieve documents attached to a graph; write via `index-document` / `delete-document`
- **Search**: Full-text and semantic (BM25 + KNN) search across graph documents via OpenSearch, with section-level retrieval
- **Files**: Stage uploaded files — `create-file-upload`, `ingest-file`, and `delete-file` commands with list and inspect reads
- **Memory**: Per-graph semantic memory for AI agents — ranked `recall` plus list/get reads, with `remember` / `forget` / `update-memory` commands

### MCP & AI Operators

- **MCP**: Model Context Protocol — schema-aware graph tools and queries for AI agents. Every graph serves the MCP Streamable HTTP transport at `POST /v1/graphs/{graph_id}/mcp`, alongside the REST tool surface (`/mcp/tools`, `/mcp/call-tool`)
- **AI Operators**: Autonomous Claude/MCP executors for financial analysis and report generation, with automatic credit tracking and SSE progress (sync, SSE, or background worker)

### Data Synchronization

- **Connections**: Provider connections with OAuth flows, sync triggers, and status
- **SEC Filings**: Process XBRL documents and build filing knowledge graphs
- **QuickBooks**: Sync transactions, accounts, and financial reports

### Extensions Surface

Domain extensions (RoboLedger, RoboInvestor) bring their own schema and OLTP tables on a schema-per-tenant PostgreSQL database, and materialize to the graph for analytics. Content is authored as **block molecules** — self-describing envelopes bundling atomic facts with their structure, rules, and verification. The surface is **graph-scoped at the URL level** (`graph_id` is a path parameter, never a query argument), split by transport:

- **Reads** → GraphQL at `POST /extensions/{graph_id}/graphql` — schema composed dynamically from enabled domains
- **Writes** → `POST /extensions/{domain}/{graph_id}/operations/{operation_name}` — named `OperationEnvelope` commands with `Idempotency-Key` and SSE progress
- **Views** → `POST /extensions/{domain}/{graph_id}/operations/{view_name}` — read-only analytics over the materialized graph

### RoboLedger

[RoboLedger](https://roboledger.ai) is an accounting and financial reporting extension — a ledger-grade system of record that AI and analysts can both query and operate, broadly implementing the [Seattle Method](http://xbrlsite.com/seattlemethod/). Three block molecules are the authoring substrate:

- **Information Blocks** — reportable content (schedules, statements, metrics) bundled with period-versioned fact sets, typed mechanics, and rules; `evaluate-rules` runs arithmetic checks over materialized facts, and `assert-metrics` writes asserted metric series
- **Event Blocks** — REA event capture: record what happened via an action-verb vocabulary, and a handler registry derives debits/credits across the three-level ledger (Transaction → Entry → LineItem); external systems post events through registered event sources
- **Taxonomy Blocks** — accounting frameworks as data: Elements, Associations (presentation / calculation / mapping), Structures, and structural rules in one write; Ships with `rs-gaap` (~2,000 curated US-GAAP concepts) as the initial base taxonomy

Built on the blocks:

- **Reads** (GraphQL) — chart of accounts and account trees, events/transactions, trial balances, financial statements, taxonomies, mappings, reports, schedules, and fiscal calendar
- **Close lifecycle** — fiscal calendar (`closed_through` / `close_target`) with period close/reopen gated on the balance equation and QuickBooks sync-staleness
- **Mapping** — CoA→GAAP associations plus AI-assisted bulk mapping via the **MappingOperator** (auto-approve / review / skip)
- **Reporting** — multi-period statements through a Reporting Style, with a draft → under_review → filed → archived lifecycle and publish lists
- **Forecasting** — operating-plan scenarios projected through the same statement structures: rule-driven forecasts, per-line growth trajectories, and manual line assertions, with forecast periods returned alongside actuals on statement reads
- **Analytical views** — `live-financial-statement` from the OLTP ledger; `build-fact-grid` and `financial-statement-analysis` over the materialized XBRL graph hypercube
- **Serialization** — reports to **JSON-LD** (SHACL-validatable) and **XBRL 2.1** (Arelle-validated)
- **Pipelines** — QuickBooks ELT via dbt/Dagster with a configurable `write_policy`

### RoboInvestor

[RoboInvestor](https://roboinvestor.ai) is a portfolio management and investment tracking extension — tracks investor holdings and links them back to the companies behind them.

- **Portfolio Blocks** — a portfolio with its positions and securities written as one validated envelope (cost basis as integer cents); positions move through an active / disposed / archived lifecycle. Reads expose `portfolios`, `positions`, `holdings` (rolled up by issuer), and the assembled `portfolioBlock`
- **Securities** — ownership instruments (common stock, warrants, convertible notes, …) with an extensible `terms` blob for instrument-specific detail
- **Cross-graph research** — a security links to its issuer via a mutual handshake (the issuer shares a report that materializes its entity in the investor's graph), joining private holdings to SEC public-company data in the shared repository

### User & Access

- **Authentication**: JWT tokens and API key management
- **User Management**: Manage user account settings and profile
- **Subscriptions**: Shared repository subscription access & AI credits
- **Limits**: Rate limiting and usage tracking for shared repositories
- **Organizations**: Multi-user orgs — invitations, member roles, org-billed subscriptions, and per-graph membership

## Connect via MCP

Every graph is an MCP server, and the graph's URL is the preferred way to connect — Claude, Claude Code, Cursor, or any MCP client that supports HTTP transports, no install required. The URL picks the graph (`sec` for the public SEC repository, your graph id for your own); your API key goes in the `X-API-Key` header:

```
https://api.robosystems.ai/v1/graphs/{graph_id}/mcp
```

For example, in Claude Code:

```
claude mcp add --transport http robosystems-sec \
  https://api.robosystems.ai/v1/graphs/sec/mcp \
  --header "X-API-Key: <your key>"
```

For claude.ai and Claude Desktop — whose custom connectors cannot send an API-key header — generate a connector URL from the MCP page in the app (`/connect`): the URL carries its own graph-scoped, revocable API key, so it pastes straight into Settings → Connectors → Add custom connector. Clients without HTTP transport support can use the [stdio bridge](https://www.npmjs.com/package/@robosystems/mcp) in proxy mode.

## Clients

RoboSystems provides official Clients for easy integration with the API in popular languages:

### Python Client

Full-featured Python client library for all API operations - [robosystems-client](https://pypi.org/project/robosystems-client/)

**Installation**: `pip install robosystems-client`

### TypeScript/JavaScript Client

TypeScript client for Node.js and browser applications - [@robosystems/client](https://www.npmjs.com/package/@robosystems/client)

**Installation**: `npm install @robosystems/client`

## Authentication

All API endpoints require authentication using API keys. Include your API key in the request headers:

```
X-API-Key: rfs*
```
