RoboSystems is a knowledge graph platform for enterprise financial and operational data. The platform powers [RoboLedger](https://roboledger.ai) for accounting analytics and [RoboInvestor](https://roboinvestor.ai) for investment research, while the knowledge graphs are managed using the [RoboSystems](https://robosystems.ai) app. Build financial knowledge graphs, integrate accounting systems, analyze SEC filings, and leverage AI-powered insights with enterprise-grade security.

## Core Features

- **Graph Database**: Build knowledge graphs with LadybugDB for modeling financial relationships and multi-dimensional analytics
- **Multi-Tenant Architecture**: Isolated database instances with tier-based resource allocation
- **AI Agent Integration**: MCP (Model Context Protocol) support enables AI agents to query and analyze your knowledge graphs
- **DuckDB Staging**: High-performance data validation and bulk ingestion pipeline with Parquet optimization
- **Data Integration**: Connect QuickBooks and SEC XBRL filings in a unified graph
- **Shared Repositories**: Access to curated SEC filing data and other shared knowledge graphs

## API Modules

### Graph Operations

The core platform surface for querying and managing graphs

**Query and data access:**

- **Query**: Execute Cypher queries with NDJSON streaming for large results
- **Schema**: View node types, relationship types, and property definitions
- **Tables**: DuckDB staging tables — file upload, SQL query, and import workflows
- **Health**: Database connectivity, staleness indicators, and materialization status

**Graph and infrastructure state:**

- **Subgraphs**: List subgraphs, quota, and storage information
- **Backups**: List backups, download URLs, and storage statistics
- **Analytics**: Usage and content analytics

**Lifecycle commands** (`/operations/{op_name}`):

- **create-subgraph**: Initialize a subgraph with optional fork of parent data
- **delete-subgraph**: Remove a subgraph with optional pre-delete backup
- **create-backup**: Encrypted backup with configurable format and retention
- **restore-backup**: Restore from backup (blocked for entity graphs — use `materialize` instead)
- **change-tier**: Change graph infrastructure tier with Stripe billing integration
- **materialize**: Ingest DuckDB-staged tables or OLTP data into the graph (direct or Dagster-orchestrated)

### MCP & Agents

- **MCP**: Model Context Protocol for AI agent graph tools and queries
- **Agents**: Claude-powered financial analysis and report generation

### Data Synchronization

- **SEC Filings**: Process XBRL documents and build filing knowledge graphs
- **QuickBooks**: Sync transactions, accounts, and financial reports

### Extensions Surface

Domain extensions (RoboLedger, RoboInvestor) are graph-scoped with a clear split between reads, writes, and view operations:

- **Reads** → GraphQL at `POST /extensions/{graph_id}/graphql` with a schema composed dynamically from enabled domains
- **Writes** → named command operations at `POST /extensions/{domain}/{graph_id}/operations/{operation_name}`
- **Views** → graph-backed read-only analytics views at `POST /extensions/{domain}/{graph_id}/operations/{view_name}`

### RoboLedger

- **Chart of Accounts**: View accounts and hierarchical account trees synced from connected systems
- **Transactions**: List and inspect transactions with entries and line items
- **Trial Balance**: Generate trial balances with optional date filtering
- **Taxonomies**: US GAAP reporting taxonomy with structures, elements, and associations
- **Mappings**: Map chart of accounts to GAAP reporting concepts with AI auto-mapping
- **Reports**: Create, view, and share multi-period financial statements (income statement, balance sheet)
- **Schedules**: Depreciation, amortization, and accrual schedules with monthly fact generation and period close workflow
- **Fiscal Calendar**: Track close cadence with `closed_through` / `close_target` pointers, period close gates, and reopen workflow
- **Publish Lists**: Share reports to other graphs via managed distribution lists

### RoboInvestor

- **Portfolios**: Create and manage investment portfolios with metadata and classification
- **Securities**: Track securities with optional entity linking for cross-graph research
- **Positions**: Record holdings with cost basis, quantity, and date tracking
- **Holdings**: Aggregate portfolio holdings grouped by entity with current valuations

### User & Access

- **Authentication**: JWT tokens and API key management
- **User Management**: Manage user account settings and profile
- **Subscriptions**: Shared repository subscription access & AI credits
- **Limits**: Rate limiting and usage tracking for shared repositories
- **Organizations**: Team collaboration and permission management

## MCP Client

Model Context Protocol client for AI agent integration - [@robosystems/mcp](https://www.npmjs.com/package/@robosystems/mcp)

**Usage**: `npx -y @robosystems/mcp`

```
{
  "mcpServers": {
    "robosystems": {
      "command": "npx",
      "args": ["-y", "@robosystems/mcp"],
      "env": {
        "ROBOSYSTEMS_API_URL": "https://api.robosystems.ai",
        "ROBOSYSTEMS_API_KEY": "rfs*",
        "ROBOSYSTEMS_GRAPH_ID": "kg*"
      }
    }
  }
}
```

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
