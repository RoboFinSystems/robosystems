# RoboSystems

RoboSystems is a financial intelligence platform with a base + extensions architecture. Each extension defines a domain ontology that drives dedicated APIs, data pipelines, and AI agent tools.

- **LadybugDB Graph Database**: Embedded columnar graph database with native DuckDB staging, LanceDB vector search, and tiered infrastructure
- **Schema Extensions**: Domain schemas that model business domains as graph structures — the foundation for everything built on top
- **[RoboLedger](https://roboledger.ai)**: Accounting and financial reporting extension with OLTP general ledger, QuickBooks ELT pipeline, SEC XBRL reporting, and chart of accounts
- **[RoboInvestor](https://roboinvestor.ai)**: Portfolio management and investment tracking extension (schema + frontend, OLTP planned)
- **Document Search**: Full-text and semantic search across SEC filings, uploaded documents, and connected sources via OpenSearch
- **AI-Native Architecture**: Context graphs with embeddings, semantic enrichment, and confidence scoring for LLM-powered analytics
- **Model Context Protocol (MCP)**: Standardized server and [client](https://www.npmjs.com/package/@robosystems/mcp) for LLM integration with schema-aware tools
- **Multi-Source Data Integration**: SEC XBRL filings, QuickBooks accounting data via dbt pipelines, and custom financial datasets
- **Enterprise-Ready Infrastructure**: Multi-tenant architecture with tiered scaling and production-grade query management
- **Developer-First API**: RESTful API designed for integration with financial applications

## Platform

The platform provides the core infrastructure that all extensions build on:

- **Dedicated Infrastructure**: Tiered graph infrastructure with dedicated instances and configurable memory allocation
- **Subgraphs (Workspaces)**: AI memory graphs, data workspaces with fork & publish, and isolated environments for development and team collaboration
- **AI Agent Interface**: Natural language financial analysis with text-to-Cypher via Model Context Protocol (MCP)
- **Shared Repositories**: SEC XBRL filings knowledge graph for context mining and benchmarking
- **Document Management**: Upload, index, and search documents with full-text and semantic search via OpenSearch
- **DuckDB Staging System**: High-performance data validation and bulk ingestion pipeline
- **Dagster Orchestration**: Data pipeline orchestration for SEC filings, QuickBooks sync, backups, billing, and scheduled jobs
- **Credit-Based Billing**: Flexible credits for AI operations based on token usage

## Extensions

Extensions start as graph schemas and grow into full products with OLTP databases, API routes, data pipelines, and dedicated frontend apps. See [Schema Extensions](/robosystems/schemas/README.md) for details.

### [RoboLedger](https://roboledger.ai)

Accounting and financial reporting extension. OLTP general ledger with schema-per-tenant PostgreSQL (accounts, transactions, journal entries, line items, dimensions), QuickBooks ELT pipeline via dbt/Dagster, SEC XBRL financial reporting, and chart of accounts. API routes under `/v1/ledger/*`, feature-flagged via `LEDGER_ENABLED`.

### [RoboInvestor](https://roboinvestor.ai)

Portfolio management and investment tracking extension with securities, positions, trades, benchmarks, market data, and risk. Dedicated frontend app. OLTP database and API routes planned.

## Quick Start

### Docker Development Environment

```bash
# Install uv and just
brew install uv just

# Start robosystems backend api
just start

# Start frontend apps - robosystems-app, roboledger-app, roboinvestor-app
just start apps
```

This initializes the `.env` file and starts the complete RoboSystems stack with:

- Graph API with LadybugDB and DuckDB backends
- Dagster for data pipeline orchestration
- PostgreSQL for graph metadata, IAM and Dagster
- Valkey for caching, SSE messaging, and rate limiting
- OpenSearch for full-text and semantic document search
- Localstack for S3 and DynamoDB emulation

**Service URLs:**

| Service    | URL                   |
| ---------- | --------------------- |
| Main API   | http://localhost:8000 |
| Graph API  | http://localhost:8001 |
| Dagster UI | http://localhost:8002 |

With `just start apps` (frontend apps):

| App              | URL                   |
| ---------------- | --------------------- |
| RoboSystems App  | http://localhost:3000 |
| RoboLedger App   | http://localhost:3001 |
| RoboInvestor App | http://localhost:3002 |

### Local Development

```bash
# Setup Python environment (uv automatically handles Python versions)
just init
```

## Examples

See RoboSystems in action with runnable demos that create graphs, load data, and execute queries with the `robosystems-client`:

```bash
just demo-sec               # Loads NVIDIA's SEC XBRL data via Dagster pipeline
just demo-accounting        # Creates chart of accounts with 6 months of transactions
just demo-custom-graph      # Builds custom graph schema with relationship networks
```

Each demo has a corresponding [Wiki article](https://github.com/RoboFinSystems/robosystems/wiki) with detailed guides.

## Development Commands

### Testing

```bash
just test-all               # Tests with code quality
just test                   # Default test suite
just test adapters          # Test specific module
just test-cov               # Tests with coverage
```

### Log Monitoring

```bash
just logs container=api                 # View API logs (last 100 lines)
just logs container=graph-api           # View Graph API logs (last 100 lines)
just logs container=dagster-webserver   # View Dagster Webserver logs
just logs container=dagster-daemon      # View Dagster Daemon logs
```

**See [justfile](justfile) for 50+ development commands** including database migrations, CloudFormation linting, graph operations, administration, and more.

### Prerequisites

#### System Requirements

- Docker & Docker Compose
- 8GB RAM minimum
- 20GB free disk space

#### Required Tools

- `uv` for Python package and version management
- `just` for project command runner

#### Deployment Requirements

- Fork this repo
- AWS account with IAM Identity Center (SSO)
- Run `just bootstrap` to configure OIDC and GitHub variables

See the **[Bootstrap Guide](https://github.com/RoboFinSystems/robosystems/wiki/Bootstrap-Guide)** for complete instructions.

## Architecture

RoboSystems is built on a modern, scalable architecture with:

**Application Layer:**

- FastAPI REST API with versioned endpoints
- Extension API routes feature-flagged by base name (e.g., `LEDGER_ENABLED` → `/v1/ledger/*`)
- MCP Server for AI-powered graph database access with schema-aware tools
- Agent Interface for text-to-Cypher natural language queries
- Dagster for data pipeline orchestration and background jobs

**LadybugDB Graph Database:** ([configuration](/.github/configs/graph.yml))

- Embedded columnar graph database purpose-built for financial analytics
- Base + extension schema architecture — extensions define domain models
- Native DuckDB integration for high-performance staging and ingestion
- LanceDB vector search for semantic element resolution (IVF-PQ indexes, 384-dim embeddings)
- Tiered infrastructure with configurable memory, rate limits, and subgraph allocations
- Shared tier hosts public repositories with read replicas

**Data Layer:**

- PostgreSQL for IAM, graph metadata, Dagster, and extension OLTP databases (schema-per-tenant)
- OpenSearch for full-text and semantic document search (BM25 + KNN)
- Valkey for caching, SSE messaging, and rate limiting
- AWS S3 for data lake storage and static assets
- DynamoDB for instance/graph/volume registry

**Infrastructure:**

- ECS Fargate for API and Dagster
- EC2 ASG for LadybugDB writer clusters
- EC2 ALB + ASG for LadybugDB shared replica clusters
- RDS PostgreSQL + ElastiCache Valkey
- OpenSearch for full-text and semantic document search
- CloudFormation infrastructure deployed via GitHub Actions with OIDC

**For detailed architecture documentation, see the [Architecture Overview](https://github.com/RoboFinSystems/robosystems/wiki/Architecture-Overview) in the Wiki.**

## SEC Shared Repository

A curated knowledge graph of US public company financial data from SEC EDGAR XBRL filings. Runs on the shared LadybugDB tier, accessible via MCP tools, Cypher queries, and the AI agent.

- **Pipeline**: EDGAR → Download → Process (Parquet) → Stage (DuckDB) → Enrich (fastembed) → Materialize (LadybugDB) → Index + Embed (OpenSearch)
- **Graph**: 14 node types and 24 relationship types modeling the full XBRL reporting hierarchy
- **Search**: Hybrid BM25 + KNN vector search across XBRL text blocks, narrative sections, and iXBRL disclosures
- **Enrichment**: Semantic element mapping, statement classification, and disclosure tagging via the [Seattle Method](http://xbrlsite.com/seattlemethod/SeattleMethod.pdf) taxonomy

```bash
just sec-load NVDA 2025  # Load NVIDIA filings for 2025
just sec-health          # Check SEC database health
```

See [SEC Adapter](/robosystems/adapters/sec/README.md) and [SEC Pipeline](/robosystems/adapters/sec/pipeline/README.md) for detailed documentation.

## AI

### Model Context Protocol (MCP)

- **Financial Analysis**: Natural language queries across enterprise data and public benchmark data
- **Cross-Database Queries**: Compare user graph data against SEC shared repository data
- **Tools**: Rich toolkit for graph queries, schema introspection, fact discovery, financial analysis, document search, and AI memory operations
- **Handler Pool**: Managed MCP handler instances with resource limits

### Agent System

- Multi-agent architecture with intelligent routing
- Dynamic agent selection based on query context
- Parallel query processing with context-aware responses
- Extensible framework for custom domain expertise

### Credit System

- **AI Operations Only**: Credits are consumed exclusively by AI agent calls (Anthropic Claude via AWS Bedrock)
- **Token-Based Billing**: ~1-2 credits per text-to-Cypher call based on actual token usage and cost
- **MCP Tool Access**: No credits consumed for external MCP calls not using agent-based tools

## Client Libraries

RoboSystems provides comprehensive client libraries for building applications:

### MCP (Model Context Protocol) Client

AI integration client for connecting Claude and other LLMs to RoboSystems.

```bash
npx -y @robosystems/mcp
```

- **Features**: Claude Desktop integration, natural language queries, graph traversal, financial analysis
- **Use Cases**: AI agents, chatbots, intelligent assistants, automated research
- **Documentation**: [npm](https://www.npmjs.com/package/@robosystems/mcp) | [GitHub](https://github.com/RoboFinSystems/robosystems-mcp-client)

### TypeScript/JavaScript Client

Full-featured SDK for web and Node.js applications with TypeScript support.

```bash
npm install @robosystems/client
```

- **Features**: Type-safe API calls, automatic retry logic, connection pooling, streaming support
- **Use Cases**: Web applications, Node.js backends, React/Vue/Angular frontends
- **Documentation**: [npm](https://www.npmjs.com/package/@robosystems/client) | [GitHub](https://github.com/RoboFinSystems/robosystems-typescript-client)

### Python Client

Native Python SDK for backend services and data science workflows.

```bash
pip install robosystems-client
```

- **Features**: Async/await support, pandas integration, Jupyter compatibility, batch operations
- **Use Cases**: Data pipelines, ML workflows, backend services, analytics
- **Documentation**: [PyPI](https://pypi.org/project/robosystems-client/) | [GitHub](https://github.com/RoboFinSystems/robosystems-python-client)

## Documentation

### User Guides (Wiki)

- **[Getting Started](https://github.com/RoboFinSystems/robosystems/wiki)** - Quick start and overview
- **[Bootstrap Guide](https://github.com/RoboFinSystems/robosystems/wiki/Bootstrap-Guide)** - Fork and deploy to your AWS account
- **[Architecture Overview](https://github.com/RoboFinSystems/robosystems/wiki/Architecture-Overview)** - System design and components
- **[Data Pipeline Guide](https://github.com/RoboFinSystems/robosystems/wiki/Pipeline-Guide)** - Dagster data orchestration and custom integrations
- **[SEC XBRL Pipeline](https://github.com/RoboFinSystems/robosystems/wiki/SEC-XBRL-Pipeline)** - Working with SEC financial data
- **[Custom Graph Demo](https://github.com/RoboFinSystems/robosystems/wiki/Custom-Graph-Schema)** - Guide for creating a custom schema graph demo

### Developer Documentation (Codebase)

**Core Services:**

- **[Adapters](/robosystems/adapters/README.md)** - External service integrations
- **[Operations](/robosystems/operations/README.md)** - Business workflow orchestration
- **[Schemas](/robosystems/schemas/README.md)** - Graph schema definitions
- **[Configuration](/robosystems/config/README.md)** - Configuration management
- **[Dagster](/robosystems/dagster/README.md)** - Data pipeline and task orchestration
- **[IAM Models](/robosystems/models/iam/README.md)** - Database models and migrations
- **[API Models](/robosystems/models/api/README.md)** - API request/response models

**Graph Database System:**

- **[Graph API](/robosystems/graph_api/README.md)** - Graph API overview
- **[Backends](/robosystems/graph_api/backends/README.md)** - Backend abstraction layer
- **[Client Factory](/robosystems/graph_api/client/README.md)** - Client factory system
- **[Core Services](/robosystems/graph_api/core/README.md)** - Core services layer

**Middleware Components:**

- **[Authentication](/robosystems/middleware/auth/README.md)** - Authentication and authorization
- **[Graph Routing](/robosystems/middleware/graph/README.md)** - Graph routing layer
- **[MCP](/robosystems/middleware/mcp/README.md)** - MCP tools and pooling
- **[Billing](/robosystems/middleware/billing/README.md)** - Subscription and billing management
- **[Observability](/robosystems/middleware/otel/README.md)** - OpenTelemetry observability
- **[Robustness](/robosystems/middleware/robustness/README.md)** - Circuit breakers and retry policies

**Infrastructure:**

- **[CloudFormation](/cloudformation/README.md)** - AWS infrastructure templates
- **[Setup Scripts](/bin/setup/README.md)** - Bootstrap and configuration scripts

**Development Resources:**

- **[Examples](/examples/README.md)** - Runnable demos and integration examples
- **[Tests](/tests/README.md)** - Testing strategy and organization
- **[Admin Tools](/robosystems/admin/README.md)** - Administrative utilities and cli

**Security & Compliance:**

- **[SECURITY.md](/SECURITY.md)** - Security features and compliance configuration

## API Reference

- [API reference](https://api.robosystems.ai)
- [API documentation](https://api.robosystems.ai/docs)
- [OpenAPI specification](https://api.robosystems.ai/openapi.json)

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

Apache-2.0 © 2026 RFS LLC
