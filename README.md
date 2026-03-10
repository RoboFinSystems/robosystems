# RoboSystems

RoboSystems is an enterprise-grade financial knowledge graph platform that transforms complex financial and operational data into actionable intelligence through graph-based analytics and AI-powered insights.

- **Graph-Based Financial Intelligence**: Leverages graph database technology to model complex financial relationships
- **AI-Native Architecture**: Context graphs built with embeddings, semantic enrichment, and confidence scoring for LLM-powered analytics
- **Model Context Protocol (MCP)**: Standardized server and [client](https://www.npmjs.com/package/@robosystems/mcp) for LLM integration
- **Multi-Source Data Integration**: SEC XBRL filings, QuickBooks accounting data, and custom financial datasets
- **Enterprise-Ready Infrastructure**: Multi-tenant architecture with tiered scaling and production-grade query management
- **Developer-First API**: RESTful API designed for integration with financial applications

## Core Features

- **LadybugDB Graph Database**: Purpose-built embedded graph database with columnar storage optimized for financial analytics
- **Dedicated Infrastructure**: Tiered graph infrastructure with dedicated instances and configurable memory allocation
- **Subgraphs (Workspaces)**: AI memory graphs, data workspaces with fork & publish, and isolated environments for development and team collaboration
- **AI Agent Interface**: Natural language financial analysis with text-to-Cypher via Model Context Protocol (MCP)
- **Entity & Generic Graphs**: Curated schemas for RoboLedger/RoboInvestor, plus custom schema support
- **Shared Repositories**: SEC XBRL filings knowledge graph for context mining and benchmarking
- **QuickBooks Integration**: Complete accounting synchronization with trial balance creation
- **DuckDB Staging System**: High-performance data validation and bulk ingestion pipeline
- **Dagster Orchestration**: Data pipeline orchestration for SEC filings, backups, billing, and scheduled jobs
- **Credit-Based Billing**: Flexible credits for AI operations based on token usage

## Quick Start

### Docker Development Environment

```bash
# Install uv and just
brew install uv just

# Start robosystems backend api
just start

# Start robosystems with frontend apps (robosystems-app, roboledger-app, roboinvestor-app)
just start all
```

This initializes the `.env` file and starts the complete RoboSystems stack with:

- Graph API with LadybugDB and DuckDB backends
- Dagster for data pipeline orchestration
- PostgreSQL for graph metadata, IAM and Dagster
- Valkey for caching, SSE messaging, and rate limiting
- Localstack for S3 and DynamoDB emulation

**Service URLs:**

| Service    | URL                   |
| ---------- | --------------------- |
| Main API   | http://localhost:8000 |
| Graph API  | http://localhost:8001 |
| Dagster UI | http://localhost:8002 |

With `just start all` (frontend apps):

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

## Examples & Demos

See RoboSystems in action with runnable demos that create graphs, load data, and execute queries with the `robosystems-client`:

```bash
just demo-sec               # Loads NVIDIA's SEC XBRL data via Dagster pipeline
just demo-accounting        # Creates chart of accounts with 6 months of transactions
just demo-custom-graph      # Builds custom graph schema with relationship networks
```

- **[SEC Demo](/examples/sec_demo)** - Real public company financials from SEC XBRL filings
- **[Accounting Demo](/examples/accounting_demo)** - Double-entry bookkeeping with trial balance and financial statements
- **[Custom Graph Demo](/examples/custom_graph_demo)** - Generic graph with custom schema and relationship patterns

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
just logs api                        # View API logs (last 100 lines)
just logs api lines=200              # View more lines
just logs api follow=1               # Tail API logs
just logs-grep api "pipeline" 500    # Search API logs
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

- FastAPI REST API with versioned endpoints (`/v1/`)
- MCP Server for AI-powered graph database access
- Agent Interface for text-to-Cypher natural language queries
- Dagster for data pipeline orchestration and background jobs

**LadybugDB Graph Database:** ([configuration](/.github/configs/graph.yml))

- Embedded columnar graph database purpose-built for financial analytics
- Native DuckDB integration for high-performance staging and ingestion
- Tiered infrastructure with configurable memory, rate limits, and subgraph allocations
- Shared tier hosts public repositories (SEC, industry, economic) with read replicas

**Data Layer:**

- PostgreSQL for IAM, graph metadata, and Dagster
- Valkey for caching, SSE messaging, and rate limiting
- AWS S3 for data lake storage and static assets
- DynamoDB for instance/graph/volume registry

**Infrastructure:**

- ECS Fargate for API and Dagster (ARM64/Graviton with Spot capacity)
- EC2 ASG for LadybugDB writer clusters
- EC2 ALB + ASG for LadybugDB shared replica clusters
- RDS PostgreSQL + ElastiCache Valkey
- CloudFormation infrastructure deployed via GitHub Actions via OIDC

**For detailed architecture documentation, see the [Architecture Overview](https://github.com/RoboFinSystems/robosystems/wiki/Architecture-Overview) in the Wiki.**

## SEC Shared Repository

A curated knowledge graph of US public company financial data from SEC EDGAR XBRL filings. Runs on the shared LadybugDB tier, accessible via MCP tools, Cypher queries, and the AI agent.

**Pipeline**: EDGAR → Download → Process (Parquet) → Stage (DuckDB) → Enrich (fastembed) → Materialize (LadybugDB)

**Graph**: 14 node types (`Entity`, `Report`, `Fact`, `Element`, `Structure`, `Association`, ...) and 24 relationship types modeling the full XBRL reporting hierarchy — from company filings down to individual financial facts with their taxonomy relationships and disclosure classifications.

**Enrichment**: Every element is mapped to ~50 canonical financial concepts (revenue, net_income, total_assets, etc.) via fastembed cosine similarity. Structures are classified by statement type. Associations are tagged with disclosure types from the [Seattle Method](http://xbrlsite.com/seattlemethod/SeattleMethod.pdf) disclosure-mechanics taxonomy. Offline knowledge artifacts (PageRank, BFS classification, cross-filing consensus) refine confidence scores using an [icebug](https://github.com/Ladybug-Memory/icebug) graph built from the full corpus.

```bash
just sec-load NVDA 2025  # Load NVIDIA filings for 2025
just sec-health          # Check SEC database health
```

See [SEC Adapter](/robosystems/adapters/sec/README.md) and [SEC Pipeline](/robosystems/adapters/sec/pipeline/README.md) for detailed documentation.

## AI

### Model Context Protocol (MCP)

- **Financial Analysis**: Natural language queries across entity and benchmark data
- **Cross-Database Queries**: Compare entity data against SEC public data
- **Tools**: Rich toolkit for graph queries, schema introspection, fact discovery, AI memory operations, workspace management, and cross-database financial analysis
- **Handler Pool**: Managed MCP handler instances with resource limits

### Agent System

- Multi-agent architecture with intelligent routing
- Dynamic agent selection based on query context
- Parallel query processing with context-aware responses
- Extensible framework for custom domain expertise

### Credit System

- **AI Operations Only**: Credits are consumed exclusively by AI agent calls (Anthropic Claude via AWS Bedrock)
- **Token-Based Billing**: ~1 credits per agent call, billed on actual token usage
- **Storage Included**: All database operations, queries, backups, restores, and storage are included in tier pricing
- **MCP Tool Access**: Unlimited — no credits consumed for external MCP tool calls

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
- **[SEC XBRL Pipeline](https://github.com/RoboFinSystems/robosystems/wiki/SEC-XBRL-Pipeline)** - Working with SEC financial data
- **[Accounting Demo](https://github.com/RoboFinSystems/robosystems/wiki/Accounting-Demo)** - Complete guide to graph-based accounting workflows

### Developer Documentation (Codebase)

**Core Services:**

- **[Operations](/robosystems/operations/README.md)** - Business workflow orchestration
- **[Adapters](/robosystems/adapters/README.md)** - External service integrations (SEC, QuickBooks)
- **[Schemas](/robosystems/schemas/README.md)** - Graph schema definitions
- **[IAM Models](/robosystems/models/iam/README.md)** - Database models and migrations
- **[API Models](/robosystems/models/api/README.md)** - API request/response models
- **[Configuration](/robosystems/config/README.md)** - Configuration management
- **[Dagster](/robosystems/dagster/README.md)** - Data pipeline and task orchestration

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

- **[SECURITY.md](/SECURITY.md)** - Security features
- **[COMPLIANCE.md](/COMPLIANCE.md)** - SOC 2 compliance features

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
