RoboSystems is a knowledge graph platform for enterprise financial and operational data. The platform powers [RoboLedger](https://roboledger.ai) for accounting analytics and [RoboInvestor](https://roboinvestor.ai) for investment research, while the knowledge graphs are managed using the [RoboSystems](https://robosystems.ai) app. Build financial knowledge graphs, integrate accounting systems, analyze SEC filings, and leverage AI-powered insights with enterprise-grade security.

## Core Features

- **Graph Database**: Build knowledge graphs with Kuzu for modeling financial relationships and multi-dimensional analytics
- **Multi-Tenant Architecture**: Isolated database instances with tier-based resource allocation
- **AI Agent Integration**: MCP (Model Context Protocol) support enables AI agents to query and analyze your knowledge graphs
- **DuckDB Staging**: High-performance data validation and bulk ingestion pipeline with Parquet optimization
- **Data Integration**: Connect QuickBooks, Plaid banking, and SEC XBRL filings in a unified graph
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
- **Plaid**: Import bank transactions and account balances

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
