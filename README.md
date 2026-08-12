# RoboSystems

RoboSystems is an open-source, AI-native financial intelligence platform for accounting, financial reporting, and investment management. It models your financial data as a **knowledge graph** — transactions, facts, reporting elements, and the calculation structures that relate them are all nodes and edges, with the semantics preserved rather than flattened into rows you query around. On top of that graph it gives AI agents and analysts a ledger-grade system of record they can both query and operate — closing the books, producing reports, and analyzing portfolios across your own ledger, your holdings, and SEC public filings queryable alongside them. Powers [RoboLedger](https://roboledger.ai) and [RoboInvestor](https://roboinvestor.ai).

**Every tenant gets their own graph.** Not a row-level slice of a shared table — a dedicated graph database on its own instance, with a dedicated OLTP schema behind it. Your ontology, your taxonomies, and your calculation structures live in it as artifacts you can read, export, and take with you.

## Platform

The platform provides the core infrastructure that all extensions build on:

- **Dedicated Infrastructure**: Tiered LadybugDB graph infrastructure with dedicated instances and configurable memory allocation
- **AI Operator System**: Autonomous financial Operators (Claude/MCP executors) with automatic credit tracking and SSE progress streaming.
- **Shared Repositories**: SEC XBRL filings knowledge graph for context mining and benchmarking
- **Document Management**: Upload, index, and search documents with full-text and semantic search via OpenSearch
- **Credit-Based Billing**: Flexible credits for AI operations based on token usage
- **Subgraphs (Workspaces)**: AI memory graphs and isolated environments for development and team collaboration
- **Web Application**: Primary web interface — graph management, the AI query console (natural-language + Cypher over MCP), schema explorer, document search, shared-repository access, and billing — [`robosystems-app`](https://github.com/RoboFinSystems/robosystems-app)

The **core platform API** lives at `/v1` — auth, orgs, billing, graph lifecycle (subgraphs, backups, materialize, tier changes), Cypher, and MCP — with reads as REST `GET`. Every write — across both the core and extensions surfaces — is a named **`OperationEnvelope`** operation with `Idempotency-Key` support, audit logging, and SSE progress streaming via `/v1/operations/{id}/stream`.

## Extensions

Extensions are domain-specific subsystems that bring their own schema, OLTP tables, API routes, data pipelines, and dedicated frontend apps. They share a single PostgreSQL database with schema-per-tenant isolation and materialize to the graph for analytical queries. Domain content is authored as **block molecules** — self-describing envelopes bundling atomic facts with their structure, rules, and verification — never bare rows.

The extensions API surface is **graph-scoped at the URL level** — `graph_id` is always a path parameter, never a query argument — and splits reads from writes by transport:

- **Reads** → `POST /extensions/{graph_id}/graphql` — Strawberry GraphQL, GraphiQL in dev, schema composed dynamically from enabled domains
- **Writes** → `POST /extensions/{roboledger|roboinvestor}/{graph_id}/operations/{operation_name}` — named REST commands
- **Analytical views** → `POST /extensions/{domain}/{graph_id}/operations/{view_name}` — read-only analytical operations (e.g. `build-fact-grid`, `live-financial-statement`), same envelope as writes

Behind the API is a CQRS operations kernel (`reads/` + `commands/` per domain, plus graph-backed `views/`) that's the single source of truth for business logic — GraphQL resolvers, REST operation routes, and MCP tools all delegate to the same functions. Per-domain feature flags (`ROBOLEDGER_ENABLED`, `ROBOINVESTOR_ENABLED`) gate both the routers and the GraphQL schema composition.

### [RoboLedger](https://roboledger.ai)

Accounting and financial reporting extension — a ledger-grade system of record that AI and analysts can both query and operate. It broadly implements the [Seattle Method](http://xbrlsite.com/seattlemethod/), a declarative methodology for digital financial reporting. Writes land as self-describing **molecules**: atomic facts bundled with their structural wiring, rules, and verification in one typed envelope, never bare rows. Three block molecules are the authoring substrate:

- **Information Blocks** — the envelope for reportable content: schedules, statements, metrics, and text-block disclosures bundled with their period-versioned fact sets, typed mechanics, and rules. `evaluate-rules` runs arithmetic checks (EqualTo, RollUp, RollForward, SumEquals, Exists, CoExists) over materialized facts; pinning a fact set separates a live closing book from a frozen report.
- **Event Blocks** — REA event capture: callers record what happened in the world (a sale, a payment, an asset disposal) through a structured action-verb vocabulary, and a handler registry derives the debits and credits across the three-level ledger (Transaction → Entry → LineItem). Preview handler resolution, execute to post GL atomically, and promote matured obligations (AR/AP, schedule entries) on demand.
- **Taxonomy Blocks** — accounting frameworks as data, not code: Elements, linkbase Associations (presentation / calculation / mapping), Structures, and auto-generated structural rules in one atomic write. Ships `fac` (fundamentals) and `rs-gaap` (~2,000 curated US-GAAP concepts) behind a two-tier public→tenant library, with CoA→GAAP mapping anchored to calc-DAG leaves.

Built on the blocks:

- **Close lifecycle** — fiscal calendar, close-target catch-up sequencing, and period close/reopen gated on the balance equation, [QuickBooks](https://quickbooks.intuit.com/partners/affiliates?cid=par_pim_4TcakSEFQs73) sync-staleness, and outstanding schedule obligations; every blocker names what is holding the close
- **Mapping** — CoA→GAAP mapping associations plus AI-assisted bulk mapping via the **MappingOperator** (confidence-tiered: auto-approve / review / skip)
- **Reporting** — multi-period reports rendered from shared facts through a Reporting Style; a report lifecycle (draft → under_review → filed → archived) with publish lists for distribution
- **Forecasting** — operating-plan scenarios projected through the same statement structures: rule-driven forecasts, per-line growth trajectories, and manual line assertions, with forecast periods returned alongside actuals on statement reads
- **Analytical operations** — `live-financial-statement` renders a statement straight from the OLTP ledger (no materialization required); `build-fact-grid` and `financial-statement-analysis` query the materialized XBRL hypercube in the graph
- **Serialization** — reports serialize to web-native **JSON-LD** (stored, SHACL-validatable) and filing-grade **XBRL 2.1** (rebuilt on demand, Arelle-validated)
- **Pipelines & data** — QuickBooks ELT via dbt/Dagster with a configurable `write_policy`, and SEC XBRL financial reporting

Dedicated frontend app: [`roboledger-app`](https://github.com/RoboFinSystems/roboledger-app).

### [RoboInvestor](https://roboinvestor.ai)

Portfolio management and investment tracking extension — tracks holdings in private companies and links them back to the businesses that issued them.

- **Portfolio Blocks** — the same molecule discipline as RoboLedger: a portfolio plus its positions and securities are validated and written as one envelope, with cost basis and current value held as integer cents and dollar totals computed at the boundary. Positions move through an active / disposed / archived lifecycle; reads expose `portfolios`, `positions`, `holdings` (rolled up by issuer), and the assembled `portfolioBlock`.
- **Securities** — register and maintain ownership instruments (common stock, warrants, convertible notes, …) with an extensible `terms` blob for instrument-specific detail (strike price, liquidation preference, vesting)
- **Cross-graph research** — a security can point at the graph of the company that issued it, when that company also runs on the platform. The investor records the issuer's `source_graph_id` up front as a pre-association; when the issuer later shares a published report into the investor's graph, the issuer's entity is materialized there and any securities waiting on that `source_graph_id` link to it. A holding then traverses through to the issuer's own reported facts — `Portfolio → Position → Security → Entity → Report → Fact` — with authorization enforced at the report-sharing boundary, not the OLTP layer.

Dedicated frontend app: [`roboinvestor-app`](https://github.com/RoboFinSystems/roboinvestor-app).

## Quick Start

### Docker Development Environment

```bash
# Install uv and just (jq ships with macOS 15+; install it on older macOS or Linux)
brew install uv just jq

# Start robosystems backend api
just start

# Start frontend apps - robosystems-app, roboledger-app, roboinvestor-app
just start apps

# Fetch the latest images and recreate anything that changed (after a git pull)
just upgrade
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
just demo-coffee-roaster    # Synthetic manufacturing scenario
just demo-saas-startup      # Synthetic SaaS scenario
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

Developed and tested on macOS and Linux. On Windows, use WSL2 with the repo cloned inside the Linux filesystem — see the **[Windows Setup (WSL2) Guide](https://github.com/RoboFinSystems/robosystems/wiki/Windows-Setup-with-WSL2)**.

#### Deployment Requirements

- Fork this repo
- AWS account with IAM Identity Center (SSO)
- Run `just bootstrap` to configure OIDC and GitHub variables

See the **[Bootstrap Guide](https://github.com/RoboFinSystems/robosystems/wiki/Bootstrap-Guide)** for complete instructions.

## Architecture

Built end-to-end on open-source engines — PostgreSQL, LadybugDB, DuckDB, LanceDB, OpenSearch, and Valkey — assembled into a transactional core with a materialized analytical graph and integrated vector search, with no proprietary database lock-in.

That openness runs up the stack as well as down: the accounting ontology, reporting taxonomies, and calculation structures are inspectable, portable artifacts you own, not configuration trapped in a vendor platform — [semantic sovereignty](https://robosystems.ai/blog/semantic-sovereignty) for your financial data.

### Multi-Tenancy & Isolation

The tenancy model is one rule: **every isolation primitive keys on `graph_id` — never on an organization.** Session `search_path`, cache keys, idempotency keys, rate-limit buckets, and credit accounting all namespace on the graph, or on the user where the thing genuinely belongs to a person. No shared state is ever org-keyed. The consequence worth understanding is that two graphs inside the *same* organization are isolated by the identical mechanism that separates two unrelated customers, rather than by a weaker variant of it — there is no "internal" path that skips the boundary.

- **A dedicated graph database per tenant.** Every tier allocates one LadybugDB database per instance (`databases_per_instance: 1`) — no shared writer, no query contention from a neighbor, and a blast radius of one tenant. Tiers differ by instance size, not by how many tenants share it.
- **Schema-per-graph OLTP.** The extensions PostgreSQL database gives each graph its own schema, and `SET search_path` is re-stamped on entry to every request rather than assumed from a pooled connection. A CI structural test pins that contract so it can't quietly regress.
- **Two databases, two migration histories.** Platform state (identity, orgs, billing, graph metadata) is a separate database from extensions OLTP, with independent Alembic histories. A migration to one never touches the other.
- **The graph is a derived projection.** OLTP rows are the system of record; the analytical graph is rebuilt from them blue-green — built alongside the live graph and swapped only when the build is clean. Nothing in the graph is authoritative, which is what makes a rebuild routine instead of risky.
- **Subgraphs** are isolated data environments within a tenant — separate databases sharing the parent's credits and permissions — used for AI memory, development, and team workspaces.
- **Shared repositories** (SEC XBRL) are the one multi-reader surface: a separate read-only tier with its own replicas, queryable *alongside* your own graph, and never writable through it.

Because tenancy is enforced at the graph rather than in application predicates, the same codebase serves managed SaaS, a single-tenant dedicated deployment, and a fully self-hosted install with no fork. Details: [Graphs & Multi-Tenancy](https://github.com/RoboFinSystems/robosystems/wiki/Graphs-and-Multi-Tenancy).

### Components

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
- LanceDB as the semantic-modality engine — per-graph, on-disk vector stores (IVF-PQ, 384-dim embeddings) for AI memory (remember/recall) and vector-search offload
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

## SEC Shared Repository

A curated knowledge graph of US public company financial data from SEC EDGAR XBRL filings. Runs on the shared LadybugDB tier, accessible via MCP tools, Cypher queries, and the AI Operator.

- **Pipeline**: EDGAR → Download → Process (Parquet) → Stage (DuckDB) → Enrich (Icebug+fastembed) → Materialize (LadybugDB) → Index + Embed (OpenSearch)
- **Graph**: the base schema plus the `roboledger` extension — 20 node types and 41 relationship types modeling the full XBRL reporting hierarchy
- **Search**: Hybrid BM25 + KNN vector search across XBRL text blocks, narrative sections, and iXBRL disclosures
- **Enrichment**: Semantic element mapping, statement classification, and disclosure tagging — applying aspects of the Seattle Method to the shared repository's disclosures (the methodology RoboLedger implements more broadly)

See [SEC Adapter](/robosystems/adapters/sec/README.md) for detailed documentation.

## Client Libraries

RoboSystems provides comprehensive client libraries for building applications:

### MCP (Model Context Protocol)

Every graph is an MCP server, and the graph's URL is the preferred way to connect — Claude, Claude Code, Cursor, or any MCP client that supports HTTP transports, no install required. The URL picks the graph (`sec` for the public SEC repository, your graph id for your own); your API key goes in the `X-API-Key` header, or rides inside a generated connector URL for clients that cannot send headers.

**Claude Code** — one command:

```bash
claude mcp add --transport http robosystems-sec \
  https://api.robosystems.ai/v1/graphs/sec/mcp \
  --header "X-API-Key: <your key>"
```

**Cursor / VS Code** — add to `mcp.json`:

```json
"robosystems-sec": {
  "url": "https://api.robosystems.ai/v1/graphs/sec/mcp",
  "headers": { "X-API-Key": "<your key>" }
}
```

**Claude (claude.ai / Desktop)** — generate a connector URL from the **MCP page** in the app (`/connect`) and paste it into Settings → Connectors → Add custom connector. The URL carries its own graph-scoped API key (Claude's connectors can't send custom headers), valid only for that graph and revocable anytime from Settings → API Keys.

- **Documentation**: [Wiki guide](https://github.com/RoboFinSystems/robosystems/wiki/AI-Operators-and-MCP) | [stdio bridge](https://github.com/RoboFinSystems/robosystems-mcp-client) (proxy mode) for clients without HTTP transport support

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

- [Home / Overview](https://github.com/RoboFinSystems/robosystems/wiki) · [Quick Start](https://github.com/RoboFinSystems/robosystems/wiki/Quick-Start) · [Core Concepts](https://github.com/RoboFinSystems/robosystems/wiki/Core-Concepts) · [Architecture Overview](https://github.com/RoboFinSystems/robosystems/wiki/Architecture-Overview) · [Bootstrap Guide](https://github.com/RoboFinSystems/robosystems/wiki/Bootstrap-Guide) · [Windows Setup (WSL2)](https://github.com/RoboFinSystems/robosystems/wiki/Windows-Setup-with-WSL2) · [Security & Compliance](https://github.com/RoboFinSystems/robosystems/wiki/Security-and-Compliance)

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

Each package documents itself — read the README for a directory before working in it.

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
- **[Admin Tools](/robosystems/admin/README.md)** - Administrative utilities and CLI

**Security & Compliance:**

- **[SECURITY.md](/SECURITY.md)** - Security control catalog with implementation references
- **[Compliance](https://github.com/RoboFinSystems/robosystems/wiki/Security-and-Compliance)** - Compliance stacks, toggles, and SOC 2 posture
- **[Trust Center](https://trust.robosystems.ai)** - Live compliance posture and audit artifacts

## API Reference

- [API reference](https://api.robosystems.ai)
- [API documentation](https://api.robosystems.ai/docs)
- [OpenAPI specification](https://api.robosystems.ai/openapi.json)

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
- [CONTRIBUTING.md](/.github/CONTRIBUTING.md) — branch conventions, coding standards, and the pull request process

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

Apache-2.0 © 2026 RFS LLC
