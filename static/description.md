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

- **Create**: Initialize knowledge graphs with customizable schemas and extensions
- **Query**: Execute Cypher queries with NDJSON streaming for large results
- **Tables**: DuckDB staging tables for data ingestion with file upload, query, and import workflows
- **Schema**: View and analyze node types, relationship types, and property definitions
- **Backup**: Encrypted backups with retention policies and download support
- **Analytics**: Graph analytics for understanding contents and usage

### MCP & Agents

- **MCP**: Model Context Protocol for AI agent graph tools and queries
- **Agents**: Claude-powered financial analysis and report generation

### Data Synchronization

- **SEC Filings**: Process XBRL documents and build filing knowledge graphs
- **QuickBooks**: Sync transactions, accounts, and financial reports


### Extensions Surface

Domain extensions (RoboLedger, RoboInvestor) are graph-scoped under
`/extensions/{graph_id}/...` with a clear split between reads and
writes:

- **Reads** → GraphQL at `POST /extensions/{graph_id}/graphql`. One
  unified schema covers both domains; the GraphiQL playground is
  served at the same URL in development.
- **Writes** → named command operations at
  `POST /extensions/{roboledger|roboinvestor}/{graph_id}/operations/{operation_name}`.
  Every write returns a typed `OperationEnvelope` with an
  `operationId`, supports `Idempotency-Key` for safe retries, and is
  audit-logged. Long-running commands (e.g. `auto-map-elements`)
  return `status: "pending"` and stream progress via the existing
  `/v1/operations/{operation_id}/stream` SSE endpoint.

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
