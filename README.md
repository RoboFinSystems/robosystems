# RoboSystems

RoboSystems is an open-source, AI-native financial intelligence platform for accounting, financial reporting, and investment management. It gives AI agents and analysts a ledger-grade system of record they can both query and operate — closing the books, producing reports, and analyzing portfolios across accounting, market, and SEC data. Powers [RoboLedger](https://roboledger.ai) and [RoboInvestor](https://roboinvestor.ai).

## Platform

The platform provides the core infrastructure that all extensions build on:

- **Dedicated Infrastructure**: Tiered graph infrastructure with dedicated instances and configurable memory allocation
- **AI Operator System**: Autonomous financial Operators (Claude/MCP executors) with automatic credit tracking and SSE progress streaming.
- **Shared Repositories**: SEC XBRL filings knowledge graph for context mining and benchmarking
- **Document Management**: Upload, index, and search documents with full-text and semantic search via OpenSearch
- **DuckDB Staging System**: High-performance data validation and bulk ingestion pipeline
- **Dagster Orchestration**: Data pipeline orchestration for SEC filings, QuickBooks sync, backups, billing, and scheduled jobs
- **Credit-Based Billing**: Flexible credits for AI operations based on token usage
- **Subgraphs (Workspaces)**: AI memory graphs and isolated environments for development and team collaboration

The **core platform API** lives at `/v1` — auth, orgs, billing, graph lifecycle (subgraphs, backups, materialize, tier changes), Cypher, and MCP — with reads as REST `GET`s. Every write — across both the core and extensions surfaces — is a named **`OperationEnvelope`** operation with `Idempotency-Key` support, audit logging, and SSE progress streaming via `/v1/operations/{id}/stream`.

## Extensions

Extensions are domain-specific subsystems that bring their own schema, OLTP tables, API routes, data pipelines, and dedicated frontend apps. They share a single PostgreSQL database with schema-per-tenant isolation and materialize to the graph for analytical queries. Domain content is authored as **block molecules** — self-describing envelopes bundling atomic facts with their structure, rules, and verification — never bare rows.

The extensions API surface is **graph-scoped at the URL level** — `graph_id` is always a path parameter, never a query argument — and splits reads from writes by transport:

- **Reads** → `POST /extensions/{graph_id}/graphql` — Strawberry GraphQL, GraphiQL in dev, schema composed dynamically from enabled domains
- **Writes** → `POST /extensions/{roboledger|roboinvestor}/{graph_id}/operations/{operation_name}` — named REST commands

Behind the API is a CQRS operations kernel (`reads/` + `commands/` per domain) that's the single source of truth for business logic — GraphQL resolvers, REST operation routes, and MCP tools all delegate to the same functions. Per-domain feature flags (`ROBOLEDGER_ENABLED`, `ROBOINVESTOR_ENABLED`) gate both the routers and the GraphQL schema composition.

### [RoboLedger](https://roboledger.ai)

Accounting and financial reporting extension — a ledger-grade system of record that AI and analysts can both query and operate. Writes land as self-describing **molecules**: atomic facts bundled with their structural wiring, rules, and verification in one typed envelope, never bare rows. Three block molecules are the authoring substrate:

- **Information Blocks** — the envelope for reportable content: schedules, statements, and metrics bundled with their period-versioned fact sets, typed mechanics, and rules. `evaluate-rules` runs arithmetic checks (EqualTo, RollUp, RollForward, Exists, CoExists) over materialized facts; pinning a fact set separates a live closing book from a frozen report.
- **Event Blocks** — REA event capture: callers record what happened in the world (a sale, a payment, an asset disposal) through a structured action-verb vocabulary, and a handler registry derives the debits and credits across the three-level ledger (Transaction → Entry → LineItem). Preview handler resolution, execute to post GL atomically, and promote matured obligations (AR/AP, schedule entries) on demand.
- **Taxonomy Blocks** — accounting frameworks as data, not code: Elements, linkbase Associations (presentation / calculation / mapping), Structures, and auto-generated structural rules in one atomic write. Ships `fac` (fundamentals) and `rs-gaap` (~2,000 curated US-GAAP concepts) behind a two-tier public→tenant library, with CoA→GAAP mapping anchored to calc-DAG leaves.

Built on the blocks:

- **Close lifecycle** — fiscal calendar, close-target catch-up sequencing, and period close/reopen gated on the balance equation and QuickBooks sync-staleness
- **Mapping** — CoA→GAAP mapping associations plus AI-assisted bulk mapping via the **MappingOperator** (confidence-tiered: auto-approve / review / skip)
- **Reporting** — multi-period reports rendered from shared facts through a Reporting Style; a report lifecycle (draft → under_review → filed → archived) with publish lists for distribution
- **Analytical operations** — `live-financial-statement` renders a statement straight from the OLTP ledger (no materialization required); `build-fact-grid` and `financial-statement-analysis` query the materialized XBRL hypercube in the graph
- **Serialization** — reports serialize to web-native **JSON-LD** (stored, SHACL-validatable) and filing-grade **XBRL 2.1** (rebuilt on demand, Arelle-validated)
- **Pipelines & data** — QuickBooks ELT via dbt/Dagster with a configurable `write_policy`, and SEC XBRL financial reporting

Dedicated frontend app (`roboledger-app`).

### [RoboInvestor](https://roboinvestor.ai)

Portfolio management and investment tracking extension — tracks investor holdings and links them back to the companies behind them.

- **Portfolio Blocks** — the same molecule discipline as RoboLedger: a portfolio plus its positions and securities are validated and written as one envelope, with cost basis and current value held as integer cents and dollar totals computed at the boundary. Positions move through an active / disposed / archived lifecycle; reads expose `portfolios`, `positions`, `holdings` (rolled up by issuer), and the assembled `portfolioBlock`.
- **Securities** — register and maintain ownership instruments (common stock, warrants, convertible notes, …) with an extensible `terms` blob for instrument-specific detail (strike price, liquidation preference, vesting)
- **Cross-graph research** — a security links to its issuer through a mutual handshake: the investor records the issuer's `source_graph_id`, and the issuer shares a report that materializes its entity in the investor's graph. This joins private holdings to SEC public-company data in the shared repository — the differentiated capability — with authorization enforced at the report-sharing boundary, not the OLTP layer.

Dedicated frontend app (`roboinvestor-app`).

## SEC Shared Repository

A curated knowledge graph of US public company financial data from SEC EDGAR XBRL filings. Runs on the shared LadybugDB tier, accessible via MCP tools, Cypher queries, and the AI Operator.

- **Pipeline**: EDGAR → Download → Process (Parquet) → Stage (DuckDB) → Enrich (fastembed) → Materialize (LadybugDB) → Index + Embed (OpenSearch)
- **Graph**: 14 node types and 24 relationship types modeling the full XBRL reporting hierarchy
- **Search**: Hybrid BM25 + KNN vector search across XBRL text blocks, narrative sections, and iXBRL disclosures
- **Enrichment**: Semantic element mapping, statement classification, and disclosure tagging via the [Seattle Method](http://xbrlsite.com/seattlemethod/SeattleMethod.pdf) taxonomy

See [SEC Adapter](/robosystems/adapters/sec/README.md) for detailed documentation.

## AI

### Model Context Protocol (MCP)

- **Financial Analysis**: Natural language queries across enterprise data and public benchmark data
- **Cross-Database Queries**: Compare user graph data against SEC shared repository data
- **Tools**: Rich toolkit for graph queries, schema introspection, fact discovery, financial analysis, document search, and AI memory operations
- **Handler Pool**: Managed MCP handler instances with resource limits

### AI Operator System

- Unified architecture: stateless Operators (Claude/MCP executors) with protocol-based service injection
- Dual execution: API (sync/SSE) and background worker (Valkey queue + SSE progress)
- Automatic credit tracking per AI call — Operators cannot forget billing
- Extensible: add new Operators for new AI workflows; they inherit execution, credit tracking, and progress streaming automatically

### Credit System

- **AI Operations Only**: Credits are consumed exclusively by AI Operator calls (Anthropic Claude via AWS Bedrock)
- **Token-Based Billing**: Credits based on actual token usage and model cost
- **MCP Tool Access**: No credits consumed for MCP calls or database operations

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
- PostgreSQL for IAM, graph metadata, extensions and Dagster
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
just demo-roboledger        # End-to-end RoboLedger demo: bulk OLTP, schedules, FY 2025 filed report, AI close
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
just logs api                 # View API logs (last 100 lines)
just logs graph-api           # View Graph API logs (last 100 lines)
just logs dagster-webserver   # View Dagster Webserver logs
just logs dagster-daemon      # View Dagster Daemon logs
```

**See [justfile](justfile) for 80+ development commands** including database migrations, CloudFormation linting, graph operations, administration, and more.

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

Built end-to-end on open-source engines — PostgreSQL, a columnar graph database, DuckDB, LanceDB, OpenSearch, and Valkey — assembled into a transactional core with a materialized analytical graph and integrated vector search, with no proprietary database lock-in. The components:

**Application Layer:**

- FastAPI REST API with versioned endpoints
- Extension GraphQL read API plus named REST command operations (CQRS)
- MCP Server for AI-powered graph database access with schema-aware tools
- AI Operator System for autonomous financial operations with automatic credit tracking
- Dagster for data pipeline orchestration and background jobs

**LadybugDB Graph Database:**

- Embedded columnar graph database purpose-built for financial analytics
- Base + extension schema architecture — extensions define domain models
- Native DuckDB integration for high-performance staging and ingestion
- LanceDB vector search for semantic element resolution (IVF-PQ indexes, 384-dim embeddings)
- Tiered infrastructure with configurable memory, rate limits, and subgraph allocations
- Shared tier hosts public repositories with read replicas

**Data Layer:**

- PostgreSQL (RDS) for IAM, graph metadata, Dagster, and extension OLTP databases (schema-per-tenant)
- OpenSearch for full-text and semantic document search (BM25 + KNN)
- Valkey (ElastiCache) for caching, SSE messaging, and rate limiting
- S3 for data lake storage and static assets
- DynamoDB for instance/graph/volume registry

**Infrastructure:**

- CloudFormation deployed via GitHub Actions with OIDC
- ECS Fargate for API and Dagster
- EC2 (ASG) for LadybugDB writer clusters; EC2 (ALB + ASG) for shared replica clusters

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

### Documentation (Wiki)

**Getting Started & Platform:**

- [Home / Overview](https://github.com/RoboFinSystems/robosystems/wiki) · [Quick Start](https://github.com/RoboFinSystems/robosystems/wiki/Quick-Start) · [Core Concepts](https://github.com/RoboFinSystems/robosystems/wiki/Core-Concepts) · [Architecture Overview](https://github.com/RoboFinSystems/robosystems/wiki/Architecture-Overview) · [Bootstrap Guide](https://github.com/RoboFinSystems/robosystems/wiki/Bootstrap-Guide)

**Operations Layer:**

- [Graphs & Multi-Tenancy](https://github.com/RoboFinSystems/robosystems/wiki/Graphs-and-Multi-Tenancy) · [Authentication & API Keys](https://github.com/RoboFinSystems/robosystems/wiki/Authentication-and-API-Keys) · [Querying the Analytical Graph](https://github.com/RoboFinSystems/robosystems/wiki/Querying-the-Analytical-Graph) · [Graph Operations](https://github.com/RoboFinSystems/robosystems/wiki/Graph-Operations) · [AI Operators & MCP](https://github.com/RoboFinSystems/robosystems/wiki/AI-Operators-and-MCP) · [Shared Repositories](https://github.com/RoboFinSystems/robosystems/wiki/Shared-Repositories) · [Credits & Billing](https://github.com/RoboFinSystems/robosystems/wiki/Credits-and-Billing) · [Pipeline Guide](https://github.com/RoboFinSystems/robosystems/wiki/Pipeline-Guide)

**Extensions Layer:**

- [Extensions Surface Overview](https://github.com/RoboFinSystems/robosystems/wiki/Extensions-Surface-Overview) · [GraphQL Reads](https://github.com/RoboFinSystems/robosystems/wiki/GraphQL-Reads) · [RoboLedger Operations](https://github.com/RoboFinSystems/robosystems/wiki/RoboLedger-Operations) · [RoboInvestor Operations](https://github.com/RoboFinSystems/robosystems/wiki/RoboInvestor-Operations) · [Connecting QuickBooks Locally](https://github.com/RoboFinSystems/robosystems/wiki/Connecting-QuickBooks-Locally)

**Content & Contribution Fabric:**

- [Information Blocks](https://github.com/RoboFinSystems/robosystems/wiki/Information-Blocks) · [Taxonomy & Frameworks](https://github.com/RoboFinSystems/robosystems/wiki/Taxonomy-and-Frameworks) · [Event-Driven Ledger](https://github.com/RoboFinSystems/robosystems/wiki/Event-Driven-Ledger) · [Reporting & Rendering](https://github.com/RoboFinSystems/robosystems/wiki/Reporting-and-Rendering) · [Serialization & Export](https://github.com/RoboFinSystems/robosystems/wiki/Serialization-and-Export)

**Documents & Search:**

- [Search & AI Retrieval](https://github.com/RoboFinSystems/robosystems/wiki/Search-and-AI-Retrieval) · [Document Management](https://github.com/RoboFinSystems/robosystems/wiki/Document-Management) · [File Uploads](https://github.com/RoboFinSystems/robosystems/wiki/File-Uploads)

**Demos:**

- [RoboLedger Demo Walkthrough](https://github.com/RoboFinSystems/robosystems/wiki/RoboLedger-Demo-Walkthrough) · [SEC XBRL Pipeline](https://github.com/RoboFinSystems/robosystems/wiki/SEC-XBRL-Pipeline) · [Custom Graph Schema](https://github.com/RoboFinSystems/robosystems/wiki/Custom-Graph-Schema)

### Developer Documentation (Codebase)

**Core Services:**

- **[Adapters](/robosystems/adapters/README.md)** - External service integrations
- **[Operations](/robosystems/operations/README.md)** - Business workflow orchestration, CQRS reads/commands kernels for extensions
- **[AI Operators](/robosystems/operations/operators/README.md)** - AI Operator framework: Claude/MCP executors, credit tracking, SSE streaming
- **[Schemas](/robosystems/schemas/README.md)** - Graph schema definitions
- **[Extensions GraphQL](/robosystems/graphql/README.md)** - Strawberry GraphQL read surface, Pydantic auto-derivation, resolver patterns
- **[Configuration](/robosystems/config/README.md)** - Configuration management
- **[Dagster](/robosystems/dagster/README.md)** - Data pipeline and task orchestration

**Database Models:**

- **[Platform Models](/robosystems/models/core/README.md)** - SQLAlchemy models for the platform database
- **[Extensions Models](/robosystems/models/extensions/README.md)** - SQLAlchemy models for the extensions database with schema-per-graph tenancy
- **[API Models](/robosystems/models/api/README.md)** - Pydantic request/response models for core platform and extensions surfaces

**Graph Database System:**

- **[Graph API](/robosystems/graph_api/README.md)** - Graph API overview
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
