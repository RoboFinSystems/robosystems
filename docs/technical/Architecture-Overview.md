# Architecture Overview

RoboSystems is built on a modern, scalable architecture designed for enterprise financial data processing and knowledge graph management.

## Documentation Map

This wiki is organized into six areas:

- **Getting Started & Platform** — orientation and self-hosting ([Quick Start](Quick-Start), [Core Concepts](Core-Concepts), [Bootstrap Guide](Bootstrap-Guide)).
- **Demos** — hands-on walkthroughs ([RoboLedger Demos](RoboLedger-Demo-Walkthrough), [SEC XBRL Pipeline](SEC-XBRL-Pipeline), [Custom Graph Schema](Custom-Graph-Schema)).
- **Operations Layer** — the core platform API and graph management ([Graph Operations](Graph-Operations), [Querying the Analytical Graph](Querying-the-Analytical-Graph), [AI Operators & MCP](AI-Operators-and-MCP)).
- **Extensions Layer** — RoboLedger and RoboInvestor ([Extensions Surface Overview](Extensions-Surface-Overview), [GraphQL Reads](GraphQL-Reads), [RoboLedger Operations](RoboLedger-Operations)).
- **Content & Contribution Fabric** — the semantic content model ([Information Blocks](Information-Blocks), [Taxonomy & Frameworks](Taxonomy-and-Frameworks), [Event-Driven Ledger](Event-Driven-Ledger), [Reporting & Rendering](Reporting-and-Rendering)).
- **Documents & Search** — unstructured content and retrieval ([Search & AI Retrieval](Search-and-AI-Retrieval), [Document Management](Document-Management)).

### Three Retrieval Planes

AI Operators — and any MCP client — read three distinct data planes, each with a query tool and a schema tool:

| Plane | Store | Query tool | Documented in |
| --- | --- | --- | --- |
| Operational | OLTP / extensions data | `query-graphql` | [GraphQL Reads](GraphQL-Reads) |
| Analytical | OLAP / LadybugDB graph | `read-graph-cypher` | [Querying the Analytical Graph](Querying-the-Analytical-Graph) |
| Unstructured | document index (BM25 + KNN) | `search-documents` | [Search & AI Retrieval](Search-and-AI-Retrieval) |

An operator composes across all three — pulling a policy from the unstructured plane, a live balance from the operational plane, and a reporting fact from the analytical plane. See [AI Operators & MCP](AI-Operators-and-MCP).

## Table of Contents

- [High-Level Architecture](#high-level-architecture)
- [Application Layer](#application-layer)
- [Graph Database System](#graph-database-system)
- [Data Processing Layer](#data-processing-layer)
- [Data Storage](#data-storage)
- [Infrastructure](#infrastructure)
- [CI/CD & Deployment](#cicd--deployment)
- [Frontend Applications](#frontend-applications)
- [External Integrations](#external-integrations)

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Client Applications                   │
│         (Web Apps, MCP Clients, API Clients)            │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│                  FastAPI REST API                       │
│            (Authentication, Rate Limiting)              │
└─────────────────────────────────────────────────────────┘
          ↓                                    ↓
┌──────────────────────────┐    ┌──────────────────────────┐
│   Operations Layer       │    │    Dagster Pipelines     │
│  (Business Logic, SSE)   │    │   (Data Orchestration)   │
└──────────────────────────┘    └──────────────────────────┘
          ↓                                    ↓
          └────────────────┬───────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│                   Adapters Layer                        │
│         (SEC, QuickBooks, External Integrations)        │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│                  Graph API Layer                        │
│          (Backend Abstraction, DuckDB Staging)          │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│                      LadybugDB                          │
│         (Embedded Columnar Graph Database)              │
└─────────────────────────────────────────────────────────┘
```

## Application Layer

### FastAPI Backend

**Location**: `/robosystems/main.py`, `/robosystems/routers/`

The FastAPI backend serves two distinct API surfaces:

**Core Platform API (REST, versioned):**

- **Versioned Endpoints**: Core platform routes under `/v1/` for API stability
- **Multi-Tenant Routing**: Database-scoped endpoints at `/v1/graphs/{graph_id}/*`
- **Authentication**: JWT tokens and API keys
- **OpenAPI Documentation**: Auto-generated at `/docs`
- **Async Operations**: Non-blocking I/O for high throughput

**Extensions API (GraphQL reads + named command writes):**

RoboLedger and RoboInvestor are product extensions served under a unified `/extensions/*` surface that splits reads from writes by transport:

- **GraphQL Reads**: `POST /extensions/{graph_id}/graphql` — Strawberry GraphQL endpoint. The schema is composed dynamically: `InformationBlockQuery` (`informationBlock` / `informationBlocks`) is always-on regardless of product flags; `LedgerQuery` and `InvestorQuery` are gated by `ROBOLEDGER_ENABLED` / `ROBOINVESTOR_ENABLED`. A ledger-only deployment exposes ledger + information-block fields but not investor fields — introspection never reveals disabled domains. Types auto-derive from Pydantic response models; adding a field on the backend exposes it on the wire with zero GraphQL-layer boilerplate.
- **Named Command Operations**: `POST /extensions/{roboledger|roboinvestor}/{graph_id}/operations/{op_name}` — ledger commands (`close-period`, `share-report`, `create-mapping-association`, `create-information-block`, `evaluate-rules`, etc.) and roboinvestor commands. Writes are explicit command operations rather than resource CRUD, matching the multi-step domain operations they actually perform. Every response is wrapped in an `OperationEnvelope` with a ULID `operationId` and supports `Idempotency-Key` headers for safe retries. Long-running commands return `status: "pending"` with HTTP 202 and stream progress through `/v1/operations/{operationId}/stream`.
- **Analytical View Operations**: `POST /extensions/{domain}/{graph_id}/operations/{view_name}` — graph-backed analytical operations that query LadybugDB rather than the extensions OLTP database. Share the same envelope contract as command operations but are read-only and routed to the materialized graph. `build-fact-grid` (multi-dimensional pivot tables over the XBRL hypercube schema) is the first one; this is the seam for any analytical operation that needs columnar/graph query power beyond what a Postgres read can provide. Gated independently from the OLTP domains so deployments without the corresponding roboledger/roboinvestor tenants can still use them.

Feature flags (`ROBOLEDGER_ENABLED`, `ROBOINVESTOR_ENABLED`, `EXTENSIONS_GRAPHQL_ENABLED`, `FACT_GRID_ENABLED`) gate each surface at schema-construction time, not request time — disabled domains are simply absent from the schema rather than throwing runtime errors.

**See**: [GraphQL Extensions Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md) in codebase

**Graph Lifecycle Operations (CQRS command surface):**

Graph lifecycle writes use the same `OperationEnvelope` / idempotency / audit infrastructure as the extensions surface, exposed at `POST /v1/graphs/{graph_id}/operations/{op_name}`:

- **create-subgraph** — Initialize a subgraph with optional fork of parent data (sync without fork, async with fork)
- **delete-subgraph** — Remove a subgraph database with optional pre-delete backup
- **create-backup** — Encrypted backup with configurable format and retention (async, returns pending envelope)
- **restore-backup** — Restore from backup; blocked for entity graphs (OLTP is source of truth — use `materialize` instead)
- **change-tier** — Change graph infrastructure tier with Stripe billing integration (async)
- **materialize** — Ingest DuckDB-staged tables into the graph, direct or Dagster-orchestrated (async)

Reads remain as REST GETs at their existing paths (`GET /v1/graphs/{g}/subgraphs`, `GET /v1/graphs/{g}/backups`, `GET /v1/graphs/{g}/health`, etc.). The shared dispatch infrastructure lives in `middleware/operations.py`; the extensions registrar (`middleware/extensions.py`) imports from it so both surfaces share idempotency, audit, and envelope semantics.

### MCP Server

**Server Location**: `/robosystems/routers/graphs/mcp/`
**Middleware Location**: `/robosystems/middleware/mcp/`

Model Context Protocol server for AI integration:

- **MCP Endpoints**: FastAPI router exposing MCP-compliant tool endpoints
- **Specialized Tools**: Cypher queries, schema introspection, fact grids, workspace management, memory graph operations (write Cypher, add node/relationship tables), semantic element resolution (LanceDB vector search), and document search (OpenSearch full-text + semantic)
- **Streaming Support**: SSE for memory-efficient large result processing
- **Query Validation**: Complexity scoring and timeout enforcement

**Client Integration**: External MCP client (`@robosystems/mcp` npm package) connects Claude Desktop to the server.

**See**: [MCP Middleware Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/mcp/README.md) in codebase

### Operator System

**Location**: `/robosystems/operations/operators/`

Unified operator architecture for autonomous financial operations:

- **Three-Layer Design**: Operator (domain logic), OperatorContext (services), Adapters (execution lifecycle)
- **Stateless Operators**: Declare capabilities via `OperatorSpec`, receive services via `OperatorContext` — no graph_id or user in constructor
- **Automatic Credit Tracking**: `TrackedAIClient` wraps every Bedrock call with token counting and credit consumption
- **Dual Execution**: API adapter (sync/SSE) and worker adapter (Valkey queue + SSE progress)
- **Protocol-Based Services**: `ToolAccess`, `ProgressReporter`, `CreditConsumer` — swap implementations per context
- **MCP Tool Integration**: Operators access graph queries, taxonomy operations, and document search via MCP tools

**See**: [Operator README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/operators/README.md) in codebase

### Background Worker

**Location**: `/robosystems/worker/`

Always-on ECS service for long-running operator tasks and background operations:

- **Valkey Queue**: BRPOP consumer loop on DB 6 with graceful shutdown
- **Task Registry**: `@register_task` decorator for handler discovery
- **SSE Progress**: Real-time streaming via OperationManager (Valkey DB 3)
- **Dagster Reporting**: Fire-and-forget AssetMaterialization events for observability
- **Tenant Isolation**: Connection pool disposal between tasks prevents cross-tenant leaks

**See**: [Worker README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/worker/README.md) in codebase

### Dagster Orchestration

**Location**: `/robosystems/dagster/`

Dagster is the orchestration system for scheduled tasks, event-driven triggers, and data pipelines. The background worker (Valkey queue) handles user-initiated agent tasks with real-time progress streaming.

**Architecture:**

`dagster/` handles platform orchestration (billing, infrastructure, provisioning, graph ops). Adapter-specific pipelines live inside their adapter packages and expose a `get_dagster_components()` function. `definitions.py` collects everything.

```
dagster/
├── definitions.py         # Collector: platform + adapter pipelines
├── resources/             # Shared Dagster resources (DB, S3, Graph)
├── assets/
│   ├── graphs.py          # User graph operation assets
│   └── shared_repositories/  # S3 publish + replica refresh (all shared repos)
├── jobs/                  # Platform jobs (billing, infrastructure, graph, provisioning)
└── sensors/
    └── provisioning.py    # Watch for pending subscriptions

adapters/sec/pipeline/     # SEC pipeline (assets, jobs, sensors, schedule)
adapters/custom_*/pipeline/ # Self-hosted-fork adapter namespace
```

**What Dagster Handles:**

| Category | Description |
|----------|-------------|
| **Billing** | Credit allocation, storage billing, usage collection |
| **Infrastructure** | Auth cleanup, health checks, instance monitoring |
| **Graph Operations** | Database creation, backup/restore, materialization |
| **Data Pipelines** | Adapter pipelines (SEC XBRL: download, process, stage, materialize) |
| **Event Triggers** | Subscription provisioning, repository sync, adapter sensors |

**Key Features:**
- **Pipeline Orchestration**: Scheduled and event-driven data pipelines
- **Asset Dependencies**: Declarative data lineage with automatic orchestration
- **Year Partitioning**: Process historical data by year
- **Observability**: Built-in UI at `localhost:8002` for monitoring
- **Scheduling**: Cron-based schedules for billing, cleanup, and pipelines
- **Sensors**: Event-driven job triggers (e.g., new subscription → provision graph)
- **Local Development**: `just sec-load NVDA 2025` triggers pipeline via Docker CLI

**Infrastructure (AWS):**
- **ECS Fargate**: Daemon, Webserver, and Run Workers - all serverless

See [Dagster CloudFormation](https://github.com/RoboFinSystems/robosystems/blob/main/cloudformation/dagster.yaml) for current infrastructure configuration.

## Graph Database System

### LadybugDB - Primary Graph Database

RoboSystems is built on **LadybugDB**, a high-performance embedded graph database purpose-built for financial knowledge graphs:

**Core Capabilities:**
- **Columnar Storage**: Optimized for analytical queries over financial time-series data
- **Native DuckDB Integration**: Direct staging-to-graph materialization via database extensions
- **Embedded Architecture**: No external database server - runs alongside the API for minimal latency

**Performance Features:**
- **Bulk Ingestion**: S3 Parquet → DuckDB staging → LadybugDB graph pipeline
- **Vector Search**: LanceDB IVF-PQ indexes for semantic similarity queries (~5ms latency over millions of elements)
- **Streaming Results**: NDJSON support for memory-efficient large query responses
- **Connection Pooling**: Efficient resource management per database
- **Admission Control**: CPU/memory backpressure to prevent overload

**Enterprise Features:**
- **Multi-Tenant Isolation**: Separate databases per customer with memory and storage isolation
- **Subgraph Support**: Isolated workspaces within a parent graph for AI memory, testing, and team collaboration (see [Subgraphs](#subgraphs-workspaces))
- **Automated Backups**: EBS snapshots with configurable retention
- **Auto-Scaling**: EC2 writer clusters scale based on demand

**Pluggable Architecture:**
The backend abstraction layer allows alternative implementations. Neo4j support exists in the codebase but is experimental and disabled by default.

### Graph API

**Location**: `/robosystems/graph_api/`

FastAPI microservice providing unified interface for all backends:

- **HTTP REST Interface**: Port 8001 (default)
- **Backend Abstraction**: Consistent API regardless of backend
- **Multi-Database Management**: Multiple databases per instance
- **Vector Search**: LanceDB-backed semantic similarity endpoints (build, search, export, delete)
- **Connection Pooling**: Efficient resource management
- **Streaming Support**: NDJSON for large query results
- **Admission Control**: CPU/memory backpressure

**See**: [Graph API Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/README.md) in codebase

### Client Factory System

**Location**: `/robosystems/graph_api/client/`

The client factory layer provides intelligent routing between application code and graph database infrastructure:

- **Backend-Agnostic**: Works seamlessly with any configured backend
- **Automatic Discovery**: Finds database instances via DynamoDB registry
- **Redis Caching**: Caches instance locations to reduce lookups
- **Circuit Breakers**: Prevents cascading failures with automatic recovery
- **Connection Reuse**: HTTP/2 connection pooling for efficiency
- **Retry Logic**: Exponential backoff with jitter for transient errors

**See**: [Client Factory Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/client/README.md) in codebase

### Backend Factory System

**Location**: `/robosystems/graph_api/backends/`

The backend factory provides pluggable graph database backends with a unified interface:

- **Backend Abstraction**: Common interface with pluggable backend implementations
- **Factory Pattern**: Dynamic backend selection based on configuration (`GRAPH_BACKEND_TYPE`)
- **Backend-Specific Optimizations**: Each backend implements optimal patterns for their architecture
- **Connection Management**: Backend-appropriate connection pooling and lifecycle management
- **Query Translation**: Cypher query execution with backend-specific optimizations
- **Ingestion Strategies**: Parquet-optimized bulk data loading for each backend type

**See**: [Backend Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/backends/README.md) in codebase

### DuckDB Staging System

**Location**: `/robosystems/graph_api/core/duckdb/`

High-performance data ingestion pipeline bridging raw data to LadybugDB:

- **Staging Tables**: DuckDB materialized tables for data validation and transformation
- **File-Based Workflow**: Users upload Parquet → DuckDB validates → LadybugDB ingests
- **Native Integration**: Direct DuckDB → LadybugDB materialization via database extensions (no intermediate files)
- **Schema-Driven**: Tables auto-created from graph schema DDL
- **S3 Integration**: Direct S3 file access via httpfs extension
- **SQL Preview**: Query staged data with full SQL before graph materialization

### LanceDB Vector Search

**Location**: `/robosystems/graph_api/core/lance/`

Embedded vector search on graph API instances — no additional infrastructure required:

- **IVF-PQ Indexes**: Approximate nearest neighbor search (cosine distance, 384-dim vectors)
- **Per-Graph Indexes**: Each graph/table pair gets its own index at `{LANCE_INDEX_PATH}/{graph_id}/{table_name}/`
- **Embedding Model**: BAAI/bge-small-en-v1.5 via fastembed
- **Lifecycle**: Build from DuckDB staging → Search on instance → Export as tar.gz to S3 → Replicas download on boot
- **Graceful Degradation**: Brute-force below 1,000 rows; falls back to canonical concept matching if index unavailable
- **Primary Use Case**: MCP `resolve-element` tool maps natural-language financial concepts to XBRL element names (~5ms latency). Gated by `MCP_VECTOR_SEARCH_ENABLED` (SSM).

### Infrastructure Design

**Cluster-Based Deployment:**
- **Writer Clusters**: EC2 auto-scaling groups for graph writes
- **Multi-Tenant Isolation**: Each entity gets dedicated database
- **Shared Repositories**: Public databases (SEC) accessible to all users
- **DynamoDB Registry**: Track database allocation across instances
- **API-First**: All access through REST APIs, no direct connections

**Tier System:**

The platform offers multiple tiers with different isolation levels and capabilities:


| Tier | Display Name | Instance | Subgraphs | Description |
|------|--------------|----------|-----------|-------------|
| `ladybug-standard` | LadybugDB Standard | Dedicated m7g.large (8 GB) | 3 max | Cost-efficient entry tier with subgraph support |
| `ladybug-large` | LadybugDB Large | Dedicated r7g.large (16 GB) | 10 max | Enhanced performance for growing teams |
| `ladybug-xlarge` | LadybugDB XLarge | Dedicated r7g.xlarge (32 GB) | 25 max | Maximum performance and scale |
| `ladybug-shared` | — | Platform r7g.2xlarge (64 GB) | 10 (platform-managed) | Public repositories (SEC) |

*Note: Neo4j backend tiers exist in the codebase but are experimental and disabled by default.*

### Subgraphs (Workspaces)

Subgraphs provide isolated database environments within a parent graph, available on all tiers:

**Use Cases:**
- **AI Memory**: Persistent agent memory with built-in Concept/Observation/Session schema that compounds across sessions
- **Data Workspaces**: Fork parent data, build mappings or transformations, then publish back
- **Development & Testing**: Experiment with schema changes or pipelines without affecting production
- **Team Collaboration**: Give teams isolated workspaces that share the parent's infrastructure

**Subgraph Types:**
- **Static** (default): Empty subgraph inheriting the parent's base schema
- **Memory**: Pre-built schema with Concept, Observation, and Session nodes plus relationship types — no base schema (Entity/Period), optimized for AI agent memory

**How Subgraphs Work:**
- Created on the same EC2 instance as the parent graph (no additional infrastructure cost)
- Share the parent's credit pool and subscription limits
- Static subgraphs inherit base schema from parent; memory subgraphs use the memory extension schema
- Optional `fork_parent` copies all parent data into the subgraph at creation
- Schema can be dynamically extended via MCP tools (add node tables, relationship tables)
- Fully isolated data - queries cannot cross subgraph boundaries

**Naming & Identification:**
- Subgraph names must be alphanumeric, 1-20 characters (no hyphens or underscores)
- Full ID format: `{parent_graph_id}_{subgraph_name}` (e.g., `kg123abc_dev`)
- Switch between parent and subgraphs via MCP tools

**Limits by Tier:**
| Tier | Max Subgraphs | Total Databases |
|------|---------------|-----------------|
| Standard | 3 | 4 (1 parent + 3 subgraphs) |
| Large | 10 | 11 (1 parent + 10 subgraphs) |
| XLarge | 25 | 26 (1 parent + 25 subgraphs) |

For current tier specifications including instance types, memory allocation, subgraph limits, and pricing, see:
- **[Graph Configuration](https://github.com/RoboFinSystems/robosystems/blob/main/.github/configs/graph.yml)** - Infrastructure specifications
- **[Billing Configuration](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/billing/)** - Subscription plans, AI costs, and storage pricing
- **[Shared Repository Registry](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/shared_repositories.py)** - Repository plans, rate limits, and credit costs (defined in adapter manifests)

## Data Processing Layer

### Operations Layer

**Location**: `/robosystems/operations/`

Business workflow orchestration and service layer. Two categories of code live here: platform services (unchanged for years) and extension domain logic (a CQRS-shaped subtree introduced with the GraphQL Extensions surface).

**Platform services** (`operations/graph/`, `operations/lbug/`, `operations/providers/`, top-level modules):

- **Entity Graph Service**: Entity-specific graph creation workflows with curated schemas
- **Generic Graph Service**: Custom schema graph creation with user-defined node/relationship types
- **Table Service**: Schema-driven DuckDB staging table management and file ingestion
- **Data Ingestion**: High-performance bulk data loading using COPY operations
- **Credit Service**: AI agent usage tracking with token-based consumption
- **Search Service**: OpenSearch-backed full-text and semantic document search
- **Connection Service**: Provider-agnostic connection management (QuickBooks, SEC) with encrypted credential storage

**Extension domain kernels** (`operations/roboledger/`, `operations/roboinvestor/`, `operations/information_block/`):

Each product extension keeps its business logic in a CQRS-style subtree with reads and commands as separate pure-function modules:

```
operations/roboledger/
├── reads/       # Pure functions: session + args → Pydantic response
│   └── accounts.py, entity.py, fiscal_calendar.py, reports.py, ...
├── commands/    # Pure functions: session + request → Pydantic response
│   └── fiscal_calendar.py, reports.py, schedules.py, taxonomies.py, ...
├── fiscal_calendar/  # Kept-in-place domain services (PeriodCloseService, ...)
├── reports/          # fact_grid, guard_rails
├── schedules/        # ScheduleService
└── views/            # Graph-backed analytical queries (fact-grid)

operations/roboinvestor/
├── reads/       # portfolios, securities, positions, holdings
└── commands/    # portfolios, securities, positions

operations/information_block/  # Cross-domain; not nested under roboledger
├── registry.py  # BlockTypeRegistryEntry, REGISTRY dict (frozen at import)
├── commands.py  # Generic create/update/delete dispatch by block_type
├── reads.py     # get/list_information_blocks — envelope reads
├── envelope.py  # Shared atom → Lite projection helpers
├── schedule.py  # Schedule handler (declarative construction mode)
├── statement.py # Statement handlers (compositional, stub)
├── metric.py    # Metric handler (derivative, stub)
└── rules/       # Rule evaluation engine (evaluate_rules_for_structure)
```

**Single source of truth.** `reads/*.py` and `commands/*.py` are the only place domain logic lives. Three different transports call into them:

- **GraphQL resolvers** (`graphql/resolvers/*`) delegate reads to `operations/{domain}/reads/*`
- **Named command operation routers** (`routers/extensions/{domain}/operations.py`) delegate writes to `operations/{domain}/commands/*`
- **MCP tools** (`middleware/mcp/tools/*`) call the same ops-layer modules directly

This invariant is load-bearing: if a caller hits the GraphQL endpoint, a named operation, or an MCP tool, the same function runs and the same business rules are enforced. Adding logic anywhere else — routers, resolvers, MCP handlers — is a mistake.

**Design contract for reads/commands:**

- Functions take an already-open extensions database session (`session: Session`) — the caller opens it via `extensions_session(graph_id)`
- Functions return Pydantic response models (or `None`), never raise HTTP errors
- Domain exceptions (`PeriodNotFoundError`, `MappingStructureNotFoundError`, etc.) raised here; the caller translates for its transport (HTTP 404 for REST, typed GraphQL error code, MCP tool error)

**See**: [Operations Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md) and [GraphQL Extensions Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md) in codebase

### Adapters

**Location**: `/robosystems/adapters/`

External service integrations following a consistent client/processor pattern:

**Architecture:**
```
adapters/
├── base.py                 # SharedRepositoryManifest dataclass
├── sec/                    # SEC EDGAR adapter (active, self-contained)
│   ├── manifest.py         # SEC_MANIFEST (plans, rates, endpoints, credits)
│   ├── client/             # EDGAR API, EFTS, Arelle, Downloader
│   ├── processors/         # XBRL processing, ingestion, metadata
│   │   ├── metadata.py     # SECMetadataLoader
│   │   ├── xbrl_graph.py   # XBRLGraphProcessor
│   │   ├── processing.py   # Single filing processing
│   │   ├── consolidation.py # Parquet consolidation
│   │   └── ingestion/      # DuckDB/LadybugDB ingestion
│   └── pipeline/           # Dagster orchestration (self-contained)
│       ├── __init__.py     # get_dagster_components() discovery
│       ├── configs.py      # Run configurations
│       ├── download.py     # sec_raw_filings asset
│       ├── process.py      # sec_processed_filings asset
│       ├── stage.py        # DuckDB staging assets
│       ├── materialize.py  # LadybugDB materialization assets
│       ├── vector_publish.py # LanceDB vector index S3 publishing
│       ├── text_index.py   # OpenSearch text indexing (textblocks, narratives, iXBRL)
│       ├── jobs.py         # Job definitions
│       └── sensors.py      # Sensors + schedule
├── quickbooks/             # QuickBooks adapter
│   ├── client/             # QB OAuth client
│   ├── dbt/                # dbt transforms (JSON → graph-shaped Parquet)
│   └── pipeline/           # Dagster extract/transform/load assets
```

**Adapter Pattern:**
Each adapter is self-contained with:
1. **Manifest** - `SharedRepositoryManifest` declaring identity, plans, rate limits, endpoints, and credit costs (shared repos only)
2. **Client** - API connection, authentication, rate limiting
3. **Processors** - Data transformation to graph-ready format
4. **Pipeline** - Dagster orchestration (assets, jobs, sensors, schedules) with `get_dagster_components()` discovery
5. **Models** (optional) - Service-specific data models

The manifest registry (`config/shared_repositories.py`) lazy-loads all manifests and provides the query API used by billing, middleware, and operations. Adding a new shared repository requires only a manifest file and one line in the registry. `dagster/definitions.py` collects adapter pipelines via `get_dagster_components()`.

**SEC Adapter Details:**
- **SECClient**: SEC EDGAR API with rate limiting and retry logic
- **XBRLGraphProcessor**: Transforms XBRL to DataFrames using Arelle
- **XBRLDuckDBGraphProcessor**: Orchestrates S3 → DuckDB → LadybugDB flow
- Creates nodes: Entity, Report, Fact, Element, Period, Unit, Taxonomy
- Outputs year-partitioned Parquet files to S3

**Extending with custom sources:**

The supported route for connecting your own data sources is an **integration** — a program in its own repository writing through the public API ([Building Custom Integrations](Building-Custom-Integrations), [`robosystems-integration-template`](https://github.com/RoboFinSystems/robosystems-integration-template)). The in-core adapter registry is maintained exclusively by the platform team; platform-operated deployments run an unmodified core.

For **self-hosted forks**, the adapter directory remains a merge boundary — in-core additions go in the `custom_*` namespace so `git pull upstream main` never conflicts:

```
adapters/
├── sec/                 # ← Upstream (don't modify)
├── quickbooks/          # ← Upstream (don't modify)
└── custom_*/            # ← Self-hosted-fork namespace (upstream never touches)
    └── custom_erp/
```

**See**: [Adapters Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/adapters/README.md) in codebase

### Middleware Components

**Location**: `/robosystems/middleware/`

**Graph Middleware** (`/middleware/graph/`):
- Graph Router: Intelligent cluster selection and routing
- Allocation Manager: Database allocation via DynamoDB registry
- Query Queue: Admission control with backpressure
- Repository Access: Shared repository subscription management

**Authentication & Authorization** (`/middleware/auth/`):
- JWT token validation and refresh
- API key authentication
- SSO integration (Google, GitHub)
- Permission enforcement

**Billing & Credits** (`/middleware/billing/`):
- Credit consumption tracking for AI operations
- Usage metering and analytics
- Subscription enforcement

**Rate Limiting** (`/middleware/rate_limits/`):
- Burst-focused rate limiting (1-minute windows)
- Tier-based multipliers
- Per-graph and per-user limits

**Robustness** (`/middleware/robustness/`):
- Circuit breakers for external services
- Retry policies with exponential backoff
- Graceful degradation patterns

**Observability** (`/middleware/otel/`):
- OpenTelemetry integration
- Distributed request tracing
- Performance metrics collection

**Real-time** (`/middleware/sse/`):
- Server-sent events for streaming responses
- Progress tracking for long-running operations

### Models

**Location**: `/robosystems/models/`

The models split across two databases with independent migration histories:

**Core Platform Models** (`/robosystems/models/core/`):
- SQLAlchemy models for the platform PostgreSQL database (`robosystems`)
- Users, orgs, permissions, graphs, billing, connections, and documents
- Foundation for Alembic migrations and multi-tenant access control
- Credit system and usage analytics models

**See**: [Core Models Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/core/README.md) in codebase

**Extensions OLTP Models** (`/robosystems/models/extensions/`):
- SQLAlchemy models for the extensions PostgreSQL database (`extensions`)
- RoboLedger and RoboInvestor transactional state with schema-per-graph-id tenancy
- Separate `DeclarativeBase` and migration history from the core models

**See**: [Extensions Models Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/extensions/README.md) in codebase

**API Models** (`/robosystems/models/api/`):
- Centralized Pydantic models for API request/response validation
- OpenAPI documentation generation and type safety
- Consistent structure across all API endpoints

**See**: [API Models Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/api/README.md) in codebase

## Data Storage

### Graph Databases (LadybugDB)

**Purpose**: Financial knowledge graphs with entity relationships and multi-dimensional facts

- **Primary Backend**: LadybugDB embedded graph database with columnar storage
- **Multi-Tenant**: Separate database per entity (`kg12345abc`) with memory/storage isolation
- **Shared Repositories**: Public datasets (SEC) served by dedicated read-only replica fleet
- **Subgraphs**: Isolated workspaces for teams, AI memory, and environments (see [Subgraphs](#subgraphs-workspaces))
- **Cypher Queries**: Full Cypher query language support for graph traversal and analytics

### DuckDB

**Purpose**: Staging database for bulk data ingestion

- **One database per graph**: `{DUCKDB_STAGING_PATH}/{graph_id}.duckdb`
- **Materialized tables**: Data loaded from S3 Parquet files (not views)
- **Validation layer**: SQL queries before graph ingestion
- **High performance**: Columnar storage, direct S3 access
- **Default path**: `./data/staging/` (configurable via `DUCKDB_STAGING_PATH`)

### LanceDB

**Purpose**: Vector similarity search on graph instances

- **Embedded Engine**: Runs alongside LadybugDB on EC2 graph instances (no separate cluster)
- **Per-Graph Indexes**: `{LANCE_INDEX_PATH}/{graph_id}/{table_name}/{table_name}.lance/`
- **IVF-PQ Partitions**: Optimized for large-scale ANN search (cosine distance, 384-dim vectors)
- **S3 Distribution**: Indexes exported as tar.gz archives, downloaded by replicas at boot
- **Default path**: `./data/lance/` (configurable via `LANCE_INDEX_PATH`)

### OpenSearch

**Purpose**: Unstructured content search with keyword and semantic capabilities, scoped by `graph_id`

- **BM25 Keyword Search**: Standard analyzer for natural language queries over document text
- **Semantic Search**: `knn_vector` field (384-dim HNSW, cosine similarity) for embedding-based retrieval
- **Multi-Tenant Isolation**: Every query filters by `graph_id` at the client level
- **Source Types**: SEC content (`xbrl_textblock`, `narrative_section`, `ixbrl_disclosure`), user-uploaded documents (`uploaded_doc`), synced connection documents (`connection_doc`), and agent memory (`memory`)
- **Externalized Content**: Full text stored in S3/CDN; OpenSearch holds metadata, snippets, and embeddings
- **Feature-Flag Gated**: `TEXT_SEARCH_ENABLED` controls availability; `SEMANTIC_SEARCH_ENABLED` controls hybrid KNN search; `tuning/indexing/ENABLE_EMBEDDINGS` controls embedding generation during indexing

### DynamoDB

**Purpose**: Service registry and metadata

- **Instance Registry**: Track graph database instances
- **Graph Registry**: Map graphs to instances
- **Volume Registry**: EBS volume management
- **Fast lookups**: Sub-millisecond access times

### PostgreSQL

**Purpose**: Primary relational databases — two of them, on the same RDS instance with independent migration histories

**Platform database (`robosystems`)** — IAM, billing, and metadata:
- **Identity & Access Management**: Users, orgs, permissions, graphs
- **Subscription Management**: Plans, billing, credits
- **File Registry**: Track uploaded files per table (GraphFile, GraphTable)
- **Schema Registry**: Graph schema definitions (GraphSchema)
- **Dagster Orchestration**: Job runs, schedules, sensors, and asset metadata

**Extensions database (`extensions`)** — per-graph OLTP for RoboLedger and RoboInvestor:
- Transactional state (ledger entries, schedules, reports, portfolio positions) written through the `/extensions/*/operations/*` command surface
- **Schema-per-graph-id tenancy**: each graph's data is isolated in its own PostgreSQL schema via `SET search_path`
- Materialized into the graph (LadybugDB) for analytical queries — see the Pattern C pipeline in the [Pipeline Guide](Pipeline-Guide)

### Valkey (Redis)

**Purpose**: Caching and coordination

- Authentication tokens and sessions (DB 0)
- Rate limiting counters (DB 1)
- Graph client routing cache (DB 2)
- Server-sent events pub/sub and operation state tracking (DB 3)
- Distributed locks (DB 4)
- Background worker task queue (DB 6)

Database allocations are managed in [valkey_registry.py](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/valkey_registry.py).

### AWS S3

**Purpose**: Document storage and data lake

S3 storage is organized into four canonical buckets with consistent naming (`robosystems-{purpose}-{env}`):

| Bucket | Environment Variable | Purpose |
|--------|---------------------|---------|
| `robosystems-shared-raw-{env}` | `SHARED_RAW_BUCKET` | Raw downloads from external sources (SEC filings, etc.) |
| `robosystems-shared-processed-{env}` | `SHARED_PROCESSED_BUCKET` | Processed parquet files for graph ingestion |
| `robosystems-user-{env}` | `USER_DATA_BUCKET` | User uploads, graph backups, staging files |
| `robosystems-public-data-{env}` | `PUBLIC_DATA_BUCKET` | CDN-served public content |

**Key Structure:**

```
# Shared data (SEC)
s3://robosystems-shared-raw-{env}/
  sec/{cik}/{accession}.zip

s3://robosystems-shared-processed-{env}/
  sec/processed/filed=2024-Q1/nodes/Entity/part_*.parquet

# User/graph data
s3://robosystems-user-{env}/
  user-staging/{user_id}/{graph_id}/{table}/*.parquet
  graph-backups/databases/{graph_id}/full/*.lbug.gz

# Shared repository databases (downloaded by replicas on boot)
s3://robosystems-user-{env}/
  shared-repositories/databases/sec.lbug
  shared-repositories/databases/sec.duckdb
  shared-repositories/databases/sec.Element.lance.tar.gz
```

**Configuration**: Path helpers are centralized in [`robosystems/config/storage/`](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/storage/README.md) with `shared.py` for external data sources and `graph.py` for customer graph storage

## Infrastructure

### AWS Services

**Compute**:
- **ECS Fargate**: API and Dagster (webserver, daemon, run workers) on ARM64/Graviton
- **EC2**: LadybugDB writer clusters (ARM64/Graviton auto-scaling groups with EBS persistence)
- **Lambda**: Infrastructure management (instance monitoring, secret rotation, volume lifecycle)

**Database & Cache**:
- **RDS PostgreSQL**: Primary database (auto-scaling storage, optional Aurora upgrade)
- **ElastiCache**: Valkey/Redis cache
- **DynamoDB**: Service registries (on-demand pricing)

**Search & Analytics**:
- **OpenSearch Service**: Unstructured content search with keyword and semantic capabilities (single-node, VPC-private, feature-flag gated)

**Storage**:
- **S3**: Data lake and file storage
- **EBS**: Persistent volumes for graph databases

**Networking**:
- **VPC**: Private subnets with NAT Gateway
- **ALB**: Application load balancing for API
- **VPC Endpoints**: Private AWS service access

**Security**:
- **WAF**: Web application firewall for API protection
- **Secrets Manager**: Encrypted credential storage
- **CloudTrail**: Audit logging
- **VPC Flow Logs**: Network monitoring

**Observability**:
- **CloudWatch**: Logs and metrics
- **Amazon Managed Prometheus**: Metrics collection
- **Amazon Managed Grafana**: Dashboards and visualization
- **AWS Cost & Usage Report**: Cost tracking 

## CI/CD & Deployment

### GitHub OIDC Authentication

RoboSystems uses GitHub OIDC federation for AWS authentication - no AWS credentials are stored in GitHub:

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  GitHub Action  │─────▶│  OIDC Token     │─────▶│     AWS STS     │
│  Workflow       │      │  (I am repo X)  │      │  (temp creds)   │
└─────────────────┘      └─────────────────┘      └─────────────────┘
                                                          │
                                                          ▼
                                                  ┌─────────────────┐
                                                  │  Deploy to AWS  │
                                                  │  (1hr session)  │
                                                  └─────────────────┘
```

**Bootstrap process** (`just bootstrap`):
1. Deploys OIDC federation CloudFormation stack
2. Sets GitHub variables (`AWS_ROLE_ARN`, `AWS_ACCOUNT_ID`, `AWS_REGION`)
3. Creates ECR repository for Docker images
4. Creates application secrets in AWS Secrets Manager

See **[Bootstrap Guide](Bootstrap-Guide)** for complete setup instructions.

### GitHub Actions Workflows

All deployments automated through GitHub Actions. Key workflows include:

- **`prod.yml` / `staging.yml`**: Environment deployment orchestrators
- **`test.yml`**: Automated test suite
- **`build.yml`**: Docker image building and ECR push
- **`deploy-*.yml`**: Individual stack deployment workflows

See [.github/workflows/](https://github.com/RoboFinSystems/robosystems/tree/main/.github/workflows) for all available workflows.

**Runner Configuration**:
- **GitHub-hosted** (default): Free for public repos, no setup required
- **Self-hosted** (optional): Forks can use their own org-level or repo-level runners by setting the `RUNNER_LABELS` repository variable

### CloudFormation Templates

All infrastructure is managed through CloudFormation templates in `/cloudformation/`. See **[CloudFormation README](https://github.com/RoboFinSystems/robosystems/blob/main/cloudformation/README.md)** for detailed template documentation including parameters, exports, and deployment order. For initial setup, see the **[Bootstrap Guide](Bootstrap-Guide)**.

#### Bootstrap

- **`bootstrap-oidc.yaml`**: GitHub OIDC federation for CI/CD authentication (deployed locally via `just bootstrap`)

#### Core Infrastructure

- **`vpc.yaml`**: VPC, subnets, NAT gateways, VPC endpoints, network configuration, and VPC Flow Logs
- **`cloudtrail.yaml`**: CloudTrail AWS Audit Logging for compliance purposes
- **`s3.yaml`**: S3 buckets for data storage, backups, and CloudFormation templates
- **`postgres.yaml`**: RDS PostgreSQL database with auto-scaling storage and automated backups
- **`valkey.yaml`**: ElastiCache Valkey for caching 

#### API & Workers

- **`api.yaml`**: ECS Fargate API service with auto-scaling, load balancing, and health checks
- **`waf.yaml`**: AWS Web Application Firewall for protecting the API from web exploits
- **`dagster.yaml`**: Dagster webserver, daemon, and run workers for pipeline orchestration

#### LadybugDB Infrastructure

- **`graph-infra.yaml`**: Base infrastructure (DynamoDB registries, security groups, IAM roles, SNS alerts)
- **`graph-volumes.yaml`**: EBS volume lifecycle management (auto-expansion, snapshots, retention)
- **`graph-ladybug.yaml`**: Auto-scaling EC2 writer clusters for LadybugDB (multi-tenant and dedicated tiers)
- **`graph-ladybug-replicas.yaml`**: Read replica fleet for shared repositories (download .lbug and .lance.tar.gz from S3 on boot)

#### Search

- **`opensearch.yaml`**: Amazon OpenSearch Service domain for unstructured content search (VPC-private, feature-flag gated)

#### Observability

- **`prometheus.yaml`**: Amazon Managed Prometheus for metrics collection
- **`grafana.yaml`**: Amazon Managed Grafana for visualization and dashboards

#### Support

- **`bastion.yaml`**: Bastion host for secure access and troubleshooting

### Environment Configuration

Environment variables are managed through:

- **Development**: `.env` file (auto-generated)
- **Production & Staging**: AWS Secrets Manager with hierarchical structure
- **GitHub Actions**: Repository secrets and variables

### Central Configuration

- **`.github/configs/graph.yml`**: Defines all tier specifications
  - Instance configuration (hardware specs, memory, performance settings)
  - Scaling configuration (min/max replicas, auto-scaling)
  - Deployment configuration (feature flags, enablement)

### Infrastructure Setup

See **[Bootstrap Guide](Bootstrap-Guide)** for complete setup instructions including:
- AWS OIDC federation (`just bootstrap`)
- GitHub variables and secrets (`just setup-gha`)
- AWS Secrets Manager configuration (`just setup-aws`)

## Frontend Applications

RoboSystems has multiple Next.js frontend applications that interface with the FastAPI backend:

| Application | Domain | Purpose |
|-------------|--------|---------|
| **robosystems-app** | robosystems.ai | Main platform dashboard, authentication, settings |
| **roboledger-app** | roboledger.ai | Accounting and financial management interface |
| **roboinvestor-app** | roboinvestor.ai | Investment analysis and portfolio tools |

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CloudFront CDN                          │
│              (SSL termination, caching, routing)                │
└─────────────────────────────────────────────────────────────────┘
                    │                           │
         ┌──────────┴──────────┐    ┌──────────┴──────────┐
         │   Static Assets     │    │   Dynamic Content   │
         │   (S3 Bucket)       │    │   (App Runner)      │
         │   /_next/static/*   │    │   /* (default)      │
         │   /images/*         │    │   /api/*            │
         └─────────────────────┘    └─────────────────────┘
                                              │
                                              ▼
                                    ┌─────────────────────┐
                                    │  RoboSystems API    │
                                    │  (FastAPI Backend)  │
                                    └─────────────────────┘
```

### Infrastructure

**AWS App Runner**:
- Serverless container hosting with automatic scaling
- No load balancer management required
- Health checks via `/api/utilities/health`

**CloudFront Distribution**:
- Global edge caching for static assets
- SSL/TLS termination with ACM certificates
- Origin routing: S3 for static files, App Runner for dynamic content
- www-to-apex redirect via CloudFront Function

**S3 Static Assets**:
- Next.js build artifacts (`/_next/static/*`)
- Public images and assets (`/images/*`)
- Optimized for high-throughput delivery

### Backend Integration

Frontend applications communicate with the RoboSystems API via:

1. **RoboSystems Client SDK** (`@robosystems/client`)
   - TypeScript/JavaScript client for API calls
   - Automatic authentication token management
   - Type-safe API responses

2. **Authentication Flow**
   - JWT tokens from RoboSystems API
   - Automatic token refresh

### Deployment

Frontends deploy via GitHub Actions:
- **Trigger**: Deploy from `main` branch
- **Build**: Docker image → ECR
- **Deploy**: CloudFormation → App Runner + CloudFront
- **Auth**: GitHub OIDC (no stored AWS credentials)

See individual app repositories for specific deployment workflows.

## External Integrations

### SEC EDGAR

**Purpose**: Public company financial data

- XBRL filing downloads via Dagster pipeline (quarterly partitions)
- Arelle-based XBRL parsing with fastembed enrichment
- Nightly automated incremental pipeline (download → process → stage → materialize → publish → replica refresh)
- LanceDB vector indexes for semantic XBRL element resolution via MCP tools
- OpenSearch text and semantic indexing of filing narratives, text blocks, and iXBRL disclosures
- Rate-limited API access with backoff

### QuickBooks API

**Purpose**: Accounting data synchronization

- OAuth 2.0 authentication with token management
- Full dbt transformation pipeline (JSON → graph-shaped Parquet)
- Dagster extract/transform/load assets
- Full rebuild and incremental sync modes

### Anthropic Claude (via AWS Bedrock)

**Purpose**: AI-powered financial operations

- AWS Bedrock for Claude model access (Sonnet 4.6/4.5)
- Unified operator system: stateless operators with automatic credit tracking per call
- CypherOperator for natural language graph queries, MappingOperator for autonomous CoA→rs-gaap taxonomy mapping
- MCP tools provide operators with graph queries, taxonomy operations, SEC structure discovery, and document search
- Dual execution: API (sync/SSE) for interactive queries, background worker for long-running tasks

## Key Design Principles

### Multi-Tenancy

- **Database Isolation**: Each entity gets dedicated graph database
- **Access Control**: Row-level security in PostgreSQL
- **Resource Limits**: Tier-based resource allocation

### Scalability

- **Horizontal Scaling**: Auto-scaling groups for writers
- **Vertical Scaling**: Tiered instance types (large → xlarge)
- **Caching**: Valkey for hot data
- **Pipeline Orchestration**: Dagster for data pipelines and batch operations
- **Real-time Operations**: Background worker with Valkey queue for SSE-enabled agent tasks

### Reliability

- **Circuit Breakers**: Prevent cascading failures
- **Retry Logic**: Exponential backoff with jitter
- **Health Checks**: Continuous monitoring
- **Graceful Degradation**: Fallback to read-only modes

### Performance

- **Connection Pooling**: Reuse database connections
- **Query Optimization**: Indexes and query planning
- **Streaming**: NDJSON for large results
- **Admission Control**: Prevent overload

### Security

- **Authentication**: JWT + API keys
- **Authorization**: Role-based access control
- **Encryption**: TLS in transit, at rest
- **Audit Logging**: All operations tracked

For the full posture — built-in controls, the optional compliance stacks (WAF, CloudTrail, VPC flow logs, detective baseline, audit retention) and their toggles, and the SOC 2 story for forks — see **[Security & Compliance](Security-and-Compliance)** (and [`SECURITY.md`](https://github.com/RoboFinSystems/robosystems/blob/main/SECURITY.md) for the control catalog).

## Related Documentation

**Wiki Guides:**
- **[Bootstrap Guide](Bootstrap-Guide)** - Set up a fresh fork with AWS OIDC authentication
- **[Data Pipeline Guide](Pipeline-Guide)** - Dagster data orchestration and custom integrations
- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - Load and query SEC financial filings
- **[Custom Graph Schema](Custom-Graph-Schema)** - Design and build custom graph databases

**Codebase Documentation:**
- **[Dagster Orchestration](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/dagster/README.md)** - Platform orchestration and adapter pipeline collection
- **[Adapters](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/adapters/README.md)** - External service integrations
- **[Graph API](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/README.md)** - Graph database abstraction layer
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)