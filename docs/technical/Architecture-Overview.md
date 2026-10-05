# Architecture Overview

How RoboSystems is built: the knowledge graph at the center, the operational and analytical planes around it, and the AWS infrastructure a fork deploys. RoboSystems is an AI-native financial intelligence platform for accounting, financial reporting, and investment management, and this page maps the services, code, storage, and infrastructure that carry it.

## Documentation Map

The wiki's sections, as the sidebar groups them, with a representative page or two from each:

- **Start Here** — orientation against the hosted platform ([Quick Start](Quick-Start), [Core Concepts](Core-Concepts)).
- **Build on the API** — the contracts and the core graph surface ([Operations Contract](Operations-Contract), [Graph Operations](Graph-Operations), [Querying the Analytical Graph](Querying-the-Analytical-Graph)).
- **Connect AI, Documents & Search** — the MCP server, the operators, and retrieval ([AI Operators & MCP](AI-Operators-and-MCP), [Search & AI Retrieval](Search-and-AI-Retrieval)).
- **Shared Repositories (SEC)** — the public datasets ([Shared Repositories](Shared-Repositories), [SEC Data Model & Query Rules](SEC-Data-Model-and-Query-Rules)).
- **RoboLedger & RoboInvestor** — the extensions ([Extensions Surface Overview](Extensions-Surface-Overview), [GraphQL Reads](GraphQL-Reads), [RoboLedger Operations](RoboLedger-Operations)).
- **Concepts in Depth** — the financial-content model ([Information Blocks](Information-Blocks), [Event-Driven Ledger](Event-Driven-Ledger), [Reporting & Rendering](Reporting-and-Rendering)).
- **Run It Yourself** — running and deploying your own stack ([Local Development](Local-Development), [Bootstrap Guide](Bootstrap-Guide), [Security & Compliance](Security-and-Compliance)).
- **Demos** — hands-on walkthroughs on a local stack ([RoboLedger Demos](RoboLedger-Demo-Walkthrough), [SEC XBRL Pipeline](SEC-XBRL-Pipeline), [Custom Graph Schema](Custom-Graph-Schema)).

### Three Retrieval Planes

AI Operators — and any MCP client — read three distinct data planes, each with a query tool and a companion: a schema tool on the two structured planes (`get-graphql-schema`, `get-graph-schema`), and a drill-down fetch (`get-document-section`) on the unstructured one:

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
│          (Engine Abstraction, DuckDB Staging)           │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│                      LadybugDB                          │
│         (Embedded Columnar Graph Database)              │
└─────────────────────────────────────────────────────────┘
```

## Application Layer

### FastAPI Backend

**Location**: `/main.py` (app factory), `/robosystems/routers/`

The FastAPI backend serves two distinct API surfaces:

**Core Platform API (REST, versioned):**

- **Versioned Endpoints**: Core platform routes under `/v1/` for API stability
- **Multi-Tenant Routing**: Database-scoped endpoints at `/v1/graphs/{graph_id}/*`
- **Authentication**: JWT tokens and API keys
- **OpenAPI Documentation**: the live spec at `/openapi.json`; the API root (`/`) serves the Swagger UI with try-it, and `/docs` redirects (301) to the rendered reference at [robosystems.ai/docs/api](https://robosystems.ai/docs/api)
- **Async Operations**: Non-blocking I/O for high throughput

**Extensions API (GraphQL reads + named command writes):**

RoboLedger and RoboInvestor are product extensions served under a unified `/extensions/*` surface that splits reads from writes by transport:

- **GraphQL Reads**: `POST /extensions/{graph_id}/graphql` — Strawberry GraphQL endpoint. The schema is composed dynamically: `InformationBlockQuery`, `TaxonomyBlockQuery`, and `LibraryQuery` are always-on regardless of product flags — they are cross-domain, and their visibility follows the session `search_path` derived from the URL's `graph_id`. `LedgerQuery` and `InvestorQuery` are gated by `ROBOLEDGER_ENABLED` / `ROBOINVESTOR_ENABLED`. A ledger-only deployment exposes ledger fields plus the always-on roots but not investor fields — introspection never reveals disabled domains. Types auto-derive from Pydantic response models; adding a field on the backend exposes it on the wire with zero GraphQL-layer boilerplate.
- **Named Command Operations**: `POST /extensions/{roboledger|roboinvestor}/{graph_id}/operations/{op_name}` — ledger commands (`close-period`, `share-report`, `create-mapping-association`, `create-information-block`, `evaluate-rules`, etc.) and roboinvestor commands. Writes are explicit command operations rather than resource CRUD, matching the multi-step domain operations they actually perform. Every response is wrapped in an `OperationEnvelope` with a ULID `operationId` and supports `Idempotency-Key` headers for safe retries. Long-running commands return `status: "pending"` with HTTP 202 and stream progress through `/v1/operations/{operationId}/stream`.
- **Analytical View Operations**: `POST /extensions/{domain}/{graph_id}/operations/{view_name}` — graph-backed analytical operations that query LadybugDB rather than the extensions OLTP database. Share the same envelope contract as command operations but are read-only and routed to the materialized graph. `build-fact-grid` (multi-dimensional pivot tables over the XBRL hypercube schema) and `financial-statement-analysis` read the graph; the pair `disclosures` (the map of a report's sections) and `information-block` (one section read whole) run xbrlkit over the report held whole rather than the graph; and `describe-filing`, `search-text` and `read-text` read one SEC filing's text whole from its public folder (shared repositories only). This is the seam for any analytical operation that needs columnar/graph query power beyond what a Postgres read can provide. Gated independently from the OLTP domains so deployments without the corresponding roboledger/roboinvestor tenants can still use them.

Feature flags (`ROBOLEDGER_ENABLED`, `ROBOINVESTOR_ENABLED`, `EXTENSIONS_GRAPHQL_ENABLED`, `FACT_GRID_ENABLED`) gate each surface at schema-construction time, not request time — disabled domains are simply absent from the schema rather than throwing runtime errors.

**See**: [GraphQL Extensions Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md) in codebase

**Graph Lifecycle Operations (CQRS command surface):**

Graph lifecycle and content writes use the same `OperationEnvelope` / idempotency / audit infrastructure as the extensions surface, exposed at `POST /v1/graphs/{graph_id}/operations/{op_name}`. They fall into three groups:

**Lifecycle** (`routers/graphs/operations.py`):

- **create-subgraph** — Initialize a subgraph with optional fork of parent data (sync without fork, async with fork)
- **delete-subgraph** — Remove a subgraph database with optional pre-delete backup
- **delete-graph** — Permanently destroy a user graph and cancel its subscription, immediately or at the billing-period boundary
- **update-graph-metadata** — Edit the graph's platform-level label (display name, description, tags)
- **create-backup** — Full-dump backup with tier-capped retention (async, returns pending envelope)
- **change-tier** — Change graph infrastructure tier with Stripe billing integration (async)
- **materialize** — Rebuild the graph from DuckDB staging tables or extensions OLTP data, direct or Dagster-orchestrated (async)

**Content** (`routers/graphs/content_ops.py`) — file staging and the document corpus:

- **create-file-upload** / **ingest-file** / **delete-file** — Presign an S3 upload, stage the uploaded file into DuckDB, remove it
- **index-document** / **delete-document** — Write a document to PostgreSQL and the OpenSearch index, or remove it

**Memory** (`routers/graphs/content_ops.py`) — the semantic memory store:

- **remember** / **update-memory** / **forget** — Store, edit, and delete a semantic memory (see [Semantic Memory](#semantic-memory))

There is no customer-facing restore operation. Backups are a *download* capability: `GET /v1/graphs/{g}/backups/{backup_id}/download` returns the archive, and restoring it into a running graph is an operator-run internal path. For entity graphs the extensions OLTP database is the source of truth, so the recovery path is `materialize`, not a graph restore.

Reads remain as REST GETs at their existing paths (`GET /v1/graphs/{g}/subgraphs`, `GET /v1/graphs/{g}/backups`, `GET /v1/graphs/{g}/health`, etc.). The shared dispatch infrastructure lives in `middleware/operations.py`; the extensions registrar (`middleware/extensions.py`) imports from it so both surfaces share idempotency, audit, and envelope semantics.

### Identity & Org Model

**Location**: `/robosystems/routers/orgs/`, `/robosystems/routers/graphs/members.py`, `/robosystems/models/core/org/`

Authorization has two nested boundaries: the **org** owns and pays, the **graph** is the unit of access.

**Orgs** (`/v1/orgs/*`) are the billing and ownership boundary. Every graph belongs to exactly one org (`Graph.org_id`), as does every subscription and invoice. Users join orgs through an `OrgUser` row carrying one of three roles — `owner`, `admin`, `member` — and orgs come in three types (`personal`, `team`, `enterprise`). A new user gets a personal org by default; a user who registers through an invitation link joins the issuing org at the invited role instead. `OrgLimits` caps provisioning per org (graph count and similar safety limits), so team members share one allowance rather than each getting their own.

**Graph membership** (`/v1/graphs/{graph_id}/members`) is a separate, finer grant with ordered roles `viewer < member < admin`. Org membership alone confers no graph access — a plain org member needs an explicit `GraphUser` row. The two layers compose in `GraphUser.get_effective_role`, which returns the stronger of the explicit grant and the implicit admin that org owners and admins hold on every graph their org owns. Grants attach to parent graphs only: subgraphs resolve to their parent and inherit its permissions. Shared repositories (SEC) have no member management at all — access there is subscription-based.

This resolution runs in the auth dependency before any handler executes, so every graph-scoped route across the core API, the extensions surface, and MCP enforces the same rule.

### MCP Server

**Server Location**: `/robosystems/routers/graphs/mcp/`
**Middleware Location**: `/robosystems/middleware/mcp/`

Model Context Protocol server for AI integration:

- **MCP Endpoints**: the JSON-RPC Streamable HTTP transport; the tool list is assembled per graph and returned by `tools/list`
- **Specialized Tools**: Cypher queries, schema introspection, fact grids, workspace management, subgraph knowledge-graph writes (write Cypher, add node/relationship tables), semantic memory (`recall` / `remember` / `update-memory` / `forget`), canonical-concept element resolution (SEC-manifest-gated), and document search (OpenSearch full-text + semantic)
- **Streaming Support**: SSE for memory-efficient large result processing
- **Query Validation**: Complexity scoring and timeout enforcement

**Client Integration**: MCP clients (Claude, ChatGPT, Cursor or any MCP client) connect to a graph's MCP Streamable HTTP endpoint (`POST /v1/graphs/{graph_id}/mcp`) with an `X-API-Key` header or OAuth, or sign in through the graph-agnostic OAuth routes (`/v1/mcp`, and `/v1/mcp/roboledger` for RoboLedger graphs) — see [AI Operators and MCP](AI-Operators-and-MCP). A legacy [stdio bridge](https://github.com/RoboFinSystems/robosystems-mcp-client) (`@robosystems/mcp` npm package) remains for stdio-only clients.

**See**: [MCP Middleware Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/mcp/README.md) in codebase

### Operator System

**Location**: `/robosystems/operations/operators/`

Unified operator architecture for autonomous financial operations:

- **Three-Layer Design**: Operator (domain logic), OperatorContext (services), Adapters (execution lifecycle)
- **Stateless Operators**: Declare capabilities via `OperatorSpec`, receive services via `OperatorContext` — no graph_id or user in constructor
- **Automatic Credit Tracking**: `TrackedAIClient` wraps every Bedrock call with token counting and credit consumption
- **Worker Execution**: every run executes on the background worker (Valkey queue + SSE progress); the request answers `202` with the operation's links, or with `?mode=sync` waits up to 50 seconds and answers `200` with the result
- **Protocol-Based Services**: `ToolAccess`, `ProgressReporter`, `CreditConsumer` — swap implementations per context
- **MCP Tool Integration**: Operators access graph queries, taxonomy operations, and document search via MCP tools

**See**: [Operator README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/operators/README.md) in codebase

### Semantic Memory

**Kernel**: `/robosystems/operations/memory/`
**Storage**: `/robosystems/graph_api/core/lance/memory_store.py`

Per-graph semantic memory gives an Operator state that survives a session — a durable place to record what it learned about a book, rather than re-deriving it on every conversation.

The subsystem splits along the usual three lines:

- **Writes** go through the content-operation envelope — `remember`, `update-memory`, `forget` at `POST /v1/graphs/{g}/operations/{op}` — so memory writes carry the same idempotency and audit trail as any other write.
- **Reads** split in two. `POST /v1/graphs/{g}/memory/recall` is the ranked semantic read; `GET /v1/graphs/{g}/memory` and `GET /v1/graphs/{g}/memory/{memory_id}` are the deterministic governance reads that let a human see and audit what an operator has stored.
- **MCP tools** (`recall` / `remember` / `update-memory` / `forget`) mirror the same four verbs for AI clients.

All three transports are thin adapters over `MemoryService`, which imports no FastAPI or MCP. The kernel computes embeddings locally (fastembed, 384-dim — the same embedding service document search uses, so no credits are consumed) and delegates persistence to a writer-routed graph client.

Storage is a single LanceDB `memory` table per graph, living on the graph API instance at `{LANCE_INDEX_PATH}/{graph_id}/memory/`. It is mutated row by row rather than rebuilt, and it is deliberately **not** part of the tar.gz replica-sync pipeline — every memory operation routes to the writer/master instance.

This is distinct from a **knowledge subgraph** (see [Subgraphs](#subgraphs-workspaces)), which is a structural knowledge graph of Concept/Observation/Session nodes queried in Cypher. Semantic memory is the vector store: unstructured recollections retrieved by similarity.

The surface is gated by `SEMANTIC_MEMORY_ENABLED` (the master switch over REST, operations, and recall) with `MCP_SEMANTIC_MEMORY_ENABLED` as an additional sub-gate on tool registration. Both are off by default.

### Background Worker

**Location**: `/robosystems/worker/`

Always-on ECS service that runs every operator task, period close, and other background operations:

- **Valkey Queue**: BRPOP consumer loop on DB 6 with graceful shutdown
- **Task Registry**: `@register_task` decorator for handler discovery
- **SSE Progress**: Real-time streaming via OperationManager (Valkey DB 3)
- **Dagster Reporting**: Fire-and-forget AssetMaterialization events for observability
- **Tenant Isolation**: Connection pool disposal between tasks prevents cross-tenant leaks

**See**: [Worker README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/worker/README.md) in codebase

### Dagster Orchestration

**Location**: `/robosystems/dagster/`

Dagster is the orchestration system for scheduled tasks, event-driven triggers, and data pipelines. The background worker (Valkey queue) handles user-initiated operator tasks with real-time progress streaming.

**Architecture:**

`dagster/` handles platform orchestration (billing, infrastructure, provisioning, graph ops). Adapter-specific pipelines live inside their adapter packages and expose a `get_dagster_components()` function. `definitions.py` collects everything.

```
dagster/
├── definitions.py         # Collector: platform + adapter pipelines
├── reporting.py           # AssetMaterialization reporting from outside Dagster jobs
├── resources/             # Shared Dagster resources (database, graph, storage)
├── assets/
│   ├── graphs.py          # User graph operation assets
│   └── shared_repositories/  # All shared repos:
│       ├── publish.py         # S3 (replica boot) + R2 (subscriber download) publish
│       ├── replicas.py        # Rolling replica-fleet refresh after a publish
│       ├── master.py          # Wake / sleep the shared master around a run
│       ├── master_parking.py  # The parking logic itself (boto3 + registry)
│       └── huggingface.py     # Copy the R2 snapshot to the public dataset
├── jobs/                  # Platform jobs (billing, invoice billing, infrastructure,
│                          # graph lifecycle, migration, backups, extensions,
│                          # documents, notifications, storage reclaim, audit
│                          # retention, shared repositories)
└── sensors/
    ├── graph_lifecycle.py               # Expired subscriptions → suspend/deprovision
    ├── invoice_billing.py               # Invoice-billed subscription renewals
    ├── materialization.py               # Stale graphs → rematerialization jobs
    ├── run_failure.py                   # Failed run → CloudWatch RunFailure metric
    ├── scheduled_obligation_promoter.py # Period-boundary obligation promotion
    ├── usage_monitor.py                 # Storage usage against tier limits
    └── worker_reaper.py                 # Reap stale worker inflight tasks

adapters/sec/pipeline/         # SEC pipeline (assets, jobs, sensors, schedules)
adapters/quickbooks/pipeline/  # QuickBooks extract → dbt transform → load
adapters/mercury/pipeline/     # Mercury bank feed (off unless CONNECTION_MERCURY_ENABLED)
adapters/plaid/pipeline/       # Plaid bank feed (off unless CONNECTION_PLAID_ENABLED)
adapters/custom_*/pipeline/    # Self-hosted-fork adapter namespace
```

Each adapter's pipeline is collected only when its flag is on (`SEC_PIPELINE_ENABLED`, and the `CONNECTION_*_ENABLED` flags for the connection adapters).

**What Dagster Handles:**

| Category | Description |
|----------|-------------|
| **Billing** | Credit allocation, storage billing, usage collection |
| **Infrastructure** | Auth cleanup, health checks, instance monitoring |
| **Graph Operations** | Database creation, backups, materialization |
| **Data Pipelines** | Adapter pipelines (SEC XBRL: download, process, stage, materialize, publish; QuickBooks sync; bank feeds when enabled) |
| **Event Triggers** | Graph lifecycle transitions, stale-graph rematerialization, repository sync, adapter sensors |

**Key Features:**
- **Pipeline Orchestration**: Scheduled and event-driven data pipelines
- **Asset Dependencies**: Declarative data lineage with automatic orchestration
- **Quarter Partitioning**: The SEC pipeline processes filings by calendar quarter
- **Observability**: Dagster's web UI for runs, step logs, and asset lineage
- **Scheduling**: Cron-based schedules for billing, cleanup, and pipelines
- **Sensors**: Event-driven job triggers (e.g., expired subscription → suspend, then deprovision; stale graph → rematerialize)
- **Local Development**: on a local stack, `just sec-load` drives the SEC pipeline's Dagster jobs inside the local containers (see [Local Development](Local-Development#sec-filings))

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
- **Vector Search**: Native in-graph HNSW indexes built at materialization and queried from Cypher via `CALL QUERY_VECTOR_INDEX`
- **Streaming Results**: NDJSON support for memory-efficient large query responses
- **Connection Pooling**: Efficient resource management per database
- **Admission Control**: CPU/memory backpressure to prevent overload

**Enterprise Features:**
- **Multi-Tenant Isolation**: Separate databases per customer with memory and storage isolation
- **Subgraph Support**: Isolated workspaces within a parent graph for AI memory, testing, and team collaboration (see [Subgraphs](#subgraphs-workspaces))
- **Automated Backups**: EBS snapshots with configurable retention
- **Auto-Scaling**: EC2 writer clusters scale based on demand

**Pluggable Architecture:**
Engines sit behind a common interface (`graph_api/interfaces/engine.py`), so alternative implementations are possible. LadybugDB is the only one today.

### Graph API

**Location**: `/robosystems/graph_api/`

FastAPI microservice providing the unified interface to the graph engine:

- **HTTP REST Interface**: Port 8001 (default)
- **Engine Abstraction**: Consistent API behind the engine interface
- **Multi-Database Management**: Multiple databases per instance
- **Vector Index Management**: Build, inspect, and delete vector indexes (LadybugDB HNSW; searched in Cypher)
- **Semantic Memory**: Per-graph LanceDB memory table with row-level CRUD
- **Connection Pooling**: Efficient resource management
- **Streaming Support**: NDJSON for large query results
- **Admission Control**: CPU/memory backpressure

**See**: [Graph API Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/README.md) in codebase

### Client Factory System

**Location**: `/robosystems/graph_api/client/`

The client factory layer provides intelligent routing between application code and graph database infrastructure:

- **Engine-Agnostic**: Talks HTTP to the Graph API, never to the engine directly
- **Automatic Discovery**: Finds database instances via DynamoDB registry
- **Valkey Caching**: Caches instance locations to reduce lookups
- **Circuit Breakers**: Prevents cascading failures with automatic recovery
- **Connection Reuse**: HTTP/1.1 keep-alive connection pooling (httpx)
- **Retry Logic**: Exponential backoff with jitter for transient errors

**See**: [Client Factory Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/client/README.md) in codebase

### Engine Interface

**Interface**: `/robosystems/graph_api/interfaces/engine.py`
**Implementation**: `/robosystems/graph_api/core/ladybug/`

`GraphEngineInterface` is the contract every graph engine implements — query execution, batched transactions, health check, close. `GRAPH_BACKEND_TYPE` selects the implementation; `ladybug` is the only supported value today, and startup validation rejects anything else.

The LadybugDB implementation splits along the concurrency boundary the engine imposes:

- **Engine** (`engine.py`): a single owned connection to one database file, for isolated work
- **Pool** (`pool.py`): the path for anything serving concurrent requests — one shared `lbug.Database` per file, since two Database objects over the same file do not see each other's committed writes
- **Manager** (`manager.py`): lifecycle of the `.lbug` files on a node — create, delete, inspect, apply schema, and the blue-green swap that promotes a `-wip` database to active
- **Service** (`service.py`): the query path the routers expose, with validation, timeouts, streaming, health, and metrics

### DuckDB Staging System

**Location**: `/robosystems/graph_api/core/duckdb/`

High-performance data ingestion pipeline bridging raw data to LadybugDB:

- **Staging Tables**: DuckDB materialized tables for data validation and transformation
- **File-Based Workflow**: Users upload Parquet → DuckDB validates → LadybugDB ingests
- **Native Integration**: Direct DuckDB → LadybugDB materialization via database extensions (no intermediate files)
- **Schema-Driven**: Tables auto-created from graph schema DDL
- **S3 Integration**: Direct S3 file access via httpfs extension
- **SQL Preview**: Query staged data with full SQL before graph materialization

### Vector Search

**Location**: `/robosystems/graph_api/routers/databases/vector_search.py`, `/robosystems/graph_api/core/lance/`

Vector search runs on the graph API instances themselves — no separate vector cluster. There are two distinct paths, and they should not be confused.

**In-graph HNSW (the live path).** Vector indexes are built on materialized LadybugDB tables and queried *from Cypher* via `CALL QUERY_VECTOR_INDEX`, not through a separate search endpoint. The materialization path builds them; a query joins similarity against the rest of the graph in one statement, which is the point — a vector hit is only useful in a financial graph if you can traverse from it. The Graph API's vector routes manage the index (build, metadata, delete); the search itself lives in the query language.

**LanceDB (the memory path).** The embedded LanceDB store on each instance backs [Semantic Memory](#semantic-memory) — a per-graph `memory` table under `{LANCE_INDEX_PATH}/{graph_id}/memory/`, mutated row by row and routed to the writer instance. Embeddings are 384-dim, computed locally with BAAI/bge-small-en-v1.5 via fastembed.

Alongside it sits `LanceManager`, a batch IVF-PQ index builder with a DuckDB-staging-to-tar.gz lifecycle. It is **dormant, not dead**: only its `delete` path runs today (tearing down a graph's lance directory when the database is dropped). It is retained as the IVF-PQ foundation for a planned `lance` vector-store subgraph, which — unlike LadybugDB — would expose vector search over the Graph API routes rather than through Cypher.

**What element resolution actually uses.** The MCP `resolve-element` tool maps natural-language financial concepts to XBRL element names using neither of the above: it matches against a small in-memory set of curated canonical-concept embeddings (`adapters/sec/enrichment`) with a text-label fallback. There is no per-element vector index — the SEC corpus carries millions of elements dominated by single-filing filer extensions, and embedding search across all of them returns low-signal noise. The tool is manifest-gated on `has_semantic_enrichment`, which only the SEC repository sets.

### Infrastructure Design

**Cluster-Based Deployment:**
- **Writer Clusters**: EC2 auto-scaling groups for graph writes
- **Multi-Tenant Isolation**: Each entity gets dedicated database
- **Shared Repositories**: Public databases (SEC) accessible to all users
- **DynamoDB Registry**: Track database allocation across instances
- **API-First**: All access through REST APIs, no direct connections

**Tier System:**

The platform offers multiple tiers with different isolation levels and capabilities:


| Tier | Display Name | Description |
|------|--------------|-------------|
| `ladybug-standard` | Standard | Cost-efficient entry tier with subgraph support |
| `ladybug-large` | Large | Enhanced performance for growing teams |
| `ladybug-xlarge` | XLarge | Maximum performance and scale |
| `ladybug-shared` | — | Platform-managed fleet for public repositories (SEC) |

Every customer tier is **dedicated** — one database per instance. Instance types, RAM, vCPU,
subgraph caps, storage limits, backup retention and API rate multipliers are deliberately **not
duplicated here**, because they are served live from the same configuration that provisions the
infrastructure:

- **`GET /v1/offering`** — machine-readable, computed from the authoritative config
- **[robosystems.ai/pricing](https://robosystems.ai/pricing)** — the public pricing page
- **[`.github/configs/graph.yml`](https://github.com/RoboFinSystems/robosystems/blob/main/.github/configs/graph.yml)** — the source itself, used by the GHA deployment layer and imported by the application at runtime

### Subgraphs (Workspaces)

Subgraphs provide isolated database environments within a parent graph, available on all tiers:

**Use Cases:**
- **AI Memory**: Persistent agent memory with built-in Concept/Observation/Session schema that compounds across sessions
- **Data Workspaces**: Fork parent data, build mappings or transformations, then publish back
- **Development & Testing**: Experiment with schema changes or pipelines without affecting production
- **Team Collaboration**: Give teams isolated workspaces that share the parent's infrastructure

**Subgraph Types** (`subgraph_type` on `create-subgraph`):
- **`static`** (default): inherits the parent's base schema and schema extensions
- **`knowledge`**: the knowledge schema only — Concept, Observation, and Session nodes plus their relationship types, no base schema (Entity/Period); built for agent-constructed knowledge graphs (`memory` resolves to the same extension)
- **`empty`**: a bare database with no base schema and no extensions; define your own tables

**How Subgraphs Work:**
- Created on the same EC2 instance as the parent graph (no additional infrastructure cost)
- Share the parent's credit pool and subscription limits
- Static subgraphs inherit the parent's schema; knowledge subgraphs use the knowledge extension schema; empty subgraphs start with none
- Optional `fork_parent` copies all parent data into the subgraph at creation
- Schema can be dynamically extended via MCP tools (add node tables, relationship tables)
- Fully isolated data - queries cannot cross subgraph boundaries

**Naming & Identification:**
- Subgraph names must be alphanumeric, 1-20 characters (no hyphens or underscores)
- Full ID format: `{parent_graph_id}_{subgraph_name}` (e.g., `kg123abc_dev`)
- Each subgraph is its own MCP connector URL; `list-subgraphs` returns each one's `connector_url`

**Limits by Tier:** the maximum number of subgraphs is set per tier. The live values are in **`GET /v1/offering`** and on the [pricing page](https://robosystems.ai/pricing), both computed from the configuration below.

For current tier specifications including instance types, memory allocation, subgraph limits, and pricing, see:
- **[Graph Configuration](https://github.com/RoboFinSystems/robosystems/blob/main/.github/configs/graph.yml)** - Infrastructure specifications
- **[Billing Configuration](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/billing/)** - Subscription plans, AI costs, and storage pricing
- **[Shared Repository Registry](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/shared_repositories.py)** - Repository plans, rate limits, and credit costs (defined in adapter manifests)

## Data Processing Layer

### Operations Layer

**Location**: `/robosystems/operations/`

Business workflow orchestration and service layer. Two categories of code live here: platform services (unchanged for years) and extension domain logic (a CQRS-shaped subtree introduced with the GraphQL Extensions surface).

**Platform services** (`operations/graph/`, `operations/extensions/`, `operations/providers/`, `operations/search/`, `operations/memory/`, top-level modules):

- **Entity Graph Service**: Entity-specific graph creation workflows with curated schemas
- **Generic Graph Service**: Custom schema graph creation with user-defined node/relationship types
- **Table Service**: Schema-driven DuckDB staging table management and file ingestion
- **Data Ingestion**: High-performance bulk data loading using COPY operations
- **Credit Service**: AI operator usage tracking with token-based consumption
- **Search Service**: OpenSearch-backed full-text and semantic document search
- **Memory Service**: Transport-independent semantic memory kernel over the per-graph LanceDB store
- **Materialization**: OLTP → OLAP rebuild of a graph from the extensions database
- **Connection Service**: Provider-agnostic connection management with encrypted credential storage. Providers register behind their flags: `quickbooks` and `external` (an outside integration claiming a source namespace) on by default; `mercury` and `plaid` bank feeds off by default (`CONNECTION_MERCURY_ENABLED`, `CONNECTION_PLAID_ENABLED`)

**Extension domain kernels** (`operations/roboledger/`, `operations/roboinvestor/`, `operations/information_block/`):

Each product extension keeps its business logic in a CQRS-style subtree with reads and commands as separate pure-function modules:

```
operations/roboledger/
├── reads/       # Pure functions: session + args → Pydantic response
│   └── accounts.py, closing_book.py, entity.py, fiscal_calendar.py,
│       journal_entries.py, reconciliations.py, reports.py, trial_balance.py, ...
├── commands/    # Pure functions: session + request → Pydantic response
│   └── fiscal_calendar.py, journal_entries.py, reconciliations.py,
│       reconciling_items.py, reports.py, schedules.py, taxonomies.py, ...
├── fiscal_calendar/   # Domain services: fiscal calendar, period close, QB write-back
├── reconciliations/   # Ledger vs. independent balance: engine, resolvers,
│                      # balance observations, reconciliation blocks
├── reports/           # fact_grid, calc_dag, guard_rails, statement sets
├── schedules/         # ScheduleService
├── tasks/             # Worker task for period close
├── views/             # Analytical views: fact grid and statement analysis
│                      # (graph), information blocks (xbrlkit), filing text
├── entry_status.py    # Which entry statuses count as in the books
└── fact_set.py        # The one construction path for FactSet rows

operations/roboinvestor/
├── reads/       # portfolios, portfolio_block, securities, positions, holdings
└── commands/    # portfolio_block (portfolios and positions as one envelope), securities

operations/information_block/  # Cross-domain; not nested under roboledger
├── types.py        # BlockTypeRegistryEntry — the code-owned per-block-type descriptor
├── registry.py     # REGISTRY dict (frozen at import); binds each type to its handlers
├── commands.py     # Generic create/update/delete dispatch by block_type
├── reads.py        # get/list_information_blocks — envelope reads
├── envelope.py     # Shared atom → Lite projection helpers
├── rules/          # Rule evaluation engine (evaluate_rules_for_structure)
│
│                   # ── Block-type handlers ──
├── statement.py    # balance_sheet / income_statement / cash_flow_statement /
│                   # equity_statement / comprehensive_income — one envelope builder
├── disclosure.py   # regulatory_disclosure notes (compositional mode)
├── text_block.py   # Narrative text-block disclosures bound from a Document
├── schedule.py     # Schedule handler (declarative construction mode)
├── rollforward.py  # Attribution blocks over ledger LineItems (declarative mode)
├── metric.py       # Metric handler (derivative mode) — standing metric time series
├── metrics.py      # compute-metrics / assert-metrics write paths
├── chart.py        # Chart View projection over a rendering
├── reconciliation.py # Reconciliation envelope — one column per period's comparison
│
│                   # ── FP&A forecasting arm ──
├── forecast.py               # Authored scenario container: levers, line assertions, growth
├── forecast_compute.py       # Walks the driver cascade into forward FactSets
├── forecast_articulation.py  # Balance-sheet roll, schedule projection, derived cash flow
└── forecast_history.py       # Back-solves realized lever rates from closed months
```

The **forecasting arm** (`forecast*.py`) is the FP&A operating-plan engine: a scenario is authored as an Information Block, and the compute path projects it forward month by month into the same FactSet shape the historical statements use — so a plan and an actual render through one renderer.

**Single source of truth.** `reads/*.py` and `commands/*.py` are the only place domain logic lives. Three different transports call into them:

- **GraphQL resolvers** (`graphql/resolvers/*`) delegate reads to `operations/{domain}/reads/*`
- **Named command operation routers** (`routers/extensions/roboledger/operations/`, a package split by area, and `routers/extensions/roboinvestor/operations.py`) delegate writes to `operations/{domain}/commands/*`
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
│   ├── enrichment.py       # Canonical-concept matching for elements and structures
│   ├── client/             # EDGAR (xbrlkit's EdgarClient), downloader, 8-K
│   │                       # discovery over EFTS, xbrlkit's Arelle loader
│   ├── arelle/bundles/     # Taxonomy cache bundle extracted at image build
│   ├── processors/         # XBRL processing, ingestion, metadata
│   │   ├── xbrl_graph.py   # XBRLGraphProcessor — one filing to graph parquet on xbrlkit
│   │   ├── processing.py   # Single filing processing
│   │   ├── metadata.py     # SECMetadataLoader
│   │   ├── classify.py     # Association classification (RollUp, RollForward, ...)
│   │   ├── textblock.py    # Text-block externalization to the CDN
│   │   ├── current_reports.py # Which 8-Ks to keep, and which of their files
│   │   ├── consolidation.py # Parquet consolidation
│   │   └── ingestion/      # DuckDB staging + LadybugDB materialization
│   ├── knowledge/          # Statement classification + knowledge artifacts
│   ├── taxonomy/           # Canonical financial concepts for enrichment
│   ├── mcp/                # Element and report resolvers for the SEC views
│   └── pipeline/           # Dagster orchestration (self-contained)
│       ├── __init__.py     # get_dagster_components() discovery
│       ├── configs.py      # Run configurations + quarter partitions
│       ├── download.py     # sec_raw_filings asset
│       ├── process.py      # sec_processed_filings asset
│       ├── stage.py        # DuckDB staging assets
│       ├── materialize.py  # LadybugDB materialization assets
│       ├── artifact.py     # Precomputed Parquet artifacts from DuckDB staging
│       ├── catalog.py      # Per-filer catalog on the public CDN
│       ├── documents.py    # 8-K earnings releases + pre-iXBRL primary documents
│       ├── text_index.py   # OpenSearch text indexing (narrative sections, iXBRL disclosures)
│       ├── s3_publish.py   # Publish the .lbug repository database to S3
│       ├── duckdb_s3_publish.py # Publish the .duckdb staging database to S3
│       ├── r2_publish.py   # Publish the .lbug database to Cloudflare R2
│       ├── hf_publish.py   # Copy the R2 snapshot to Hugging Face (manual only)
│       ├── dataset_card.py # Snapshot statistics + the dataset README
│       ├── jobs.py         # Job definitions
│       └── sensors.py      # Sensors + schedules
├── quickbooks/             # QuickBooks adapter
│   ├── client/             # QB OAuth client
│   ├── dbt/                # dbt-duckdb transforms over the extracted Parquet
│   ├── pipeline/           # Dagster extract → transform → load (to extensions PostgreSQL)
│   └── reports.py          # Parsers for QB reports read outside the sync
├── bank_feed/              # What every bank feed shares: capture into the inbox
├── mercury/                # Mercury bank feed (client + pipeline; flag-gated, off by default)
└── plaid/                  # Plaid bank feed (client + pipeline; flag-gated, off by default)
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
- **EDGAR client**: xbrlkit's `EdgarClient`, bound to the platform's User-Agent, rate limits, and retry policy
- **XBRLGraphProcessor**: Loads, parses, and projects one filing on xbrlkit (Arelle underneath), then adds text-block externalization, semantic enrichment, and association classification
- **XBRLDuckDBGraphProcessor**: Orchestrates S3 → DuckDB → LadybugDB flow
- Creates nodes: Entity, Report, Fact, Element, Period, Unit, Taxonomy
- Outputs quarter-partitioned Parquet files to S3

**Extending with custom sources:**

The supported route for connecting your own data sources is an **integration** — a program in its own repository writing through the public API ([Building Custom Integrations](Building-Custom-Integrations), [`robosystems-integration-template`](https://github.com/RoboFinSystems/robosystems-integration-template)). The in-core adapter registry is maintained exclusively by the platform team; platform-operated deployments run an unmodified core.

For **self-hosted forks**, the adapter directory remains a merge boundary — in-core additions go in the `custom_*` namespace so `git pull upstream main` never conflicts:

```
adapters/
├── sec/                 # ← Upstream (don't modify)
├── quickbooks/          # ← Upstream (don't modify)
├── bank_feed/, mercury/, plaid/  # ← Upstream (don't modify)
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
- Cross-app SSO (shared session across robosystems / roboledger / roboinvestor)
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

**Purpose**: Semantic memory storage on graph instances

- **Embedded Engine**: Runs alongside LadybugDB on EC2 graph instances (no separate cluster)
- **Per-Graph Memory Table**: `{LANCE_INDEX_PATH}/{graph_id}/memory/memory.lance/`
- **Incremental CRUD**: Rows are added, patched, and deleted in place — never rebuilt in bulk
- **Writer-Routed**: Not part of the replica sync; all memory operations go to the writer/master instance
- **384-dim Vectors**: Embeddings from BAAI/bge-small-en-v1.5 via fastembed
- **Default path**: `./data/lance/` (configurable via `LANCE_INDEX_PATH`)

The same directory tree also hosts the dormant IVF-PQ index builder described under [Vector Search](#vector-search); graph-query vector search itself is native to LadybugDB, not LanceDB.

### OpenSearch

**Purpose**: Unstructured content search with keyword and semantic capabilities, scoped by `graph_id`

- **BM25 Keyword Search**: Standard analyzer for natural language queries over document text
- **Semantic Search**: `knn_vector` field (384-dim HNSW, cosine similarity) for embedding-based retrieval
- **Multi-Tenant Isolation**: Every query filters by `graph_id` at the client level
- **Source Types**: SEC content (`narrative_section` for a filing's Item sections, `ixbrl_disclosure` for its iXBRL text-block disclosures) and user documents (`uploaded_doc`). Semantic memory is *not* here — it lives in LanceDB on the graph instance.
- **Externalized Content**: Full text stored in S3/CDN; OpenSearch holds metadata, snippets, and embeddings
- **Feature-Flag Gated**: `SEMANTIC_SEARCH_ENABLED` (on by default) gates the search service entirely — there is no separate text or embedding toggle, and embedding generation during indexing is unconditional

### DynamoDB

**Purpose**: Service registry and metadata

- **Instance Registry**: Track graph database instances
- **Graph Registry**: Map graphs to instances
- **Volume Registry**: EBS volume management
- **Fast lookups**: Sub-millisecond access times

### PostgreSQL

**Purpose**: Primary relational databases — three of them, on the same RDS instance, each with its own migration history

**Platform database (`robosystems`)** — IAM, billing, and metadata:
- **Identity & Access Management**: Users, orgs, permissions, graphs
- **Subscription Management**: Plans, billing, credits
- **File Registry**: Track uploaded files per table (GraphFile, GraphTable)
- **Schema Registry**: Graph schema definitions (GraphSchema)

**Extensions database (`extensions`)** — per-graph OLTP for RoboLedger and RoboInvestor:
- Transactional state (ledger entries, schedules, reports, portfolio positions) written through the `/extensions/*/operations/*` command surface
- **Schema-per-graph-id tenancy**: each graph's data is isolated in its own PostgreSQL schema via `SET search_path`
- Materialized into the graph (LadybugDB) for analytical queries — see the Pattern C pipeline in the [Pipeline Guide](Pipeline-Guide)

**Dagster database (`dagster`)** — orchestration state:
- Job runs, schedules, sensor cursors, event logs, and asset metadata
- Owned and migrated by Dagster itself (`DAGSTER_POSTGRES_DB`); it has no Alembic history. The two application histories are `migrations/platform/` and `migrations/extensions/`, and running either never touches the others.

### Valkey (Redis)

**Purpose**: Caching and coordination

- Authentication tokens and sessions (DB 0)
- Rate limiting counters (DB 1)
- Graph client routing cache (DB 2)
- Server-sent events pub/sub and operation state tracking (DB 3)
- Distributed locks (DB 4)
- MCP tool result cache — schema, info (DB 5)
- Background worker task queue (DB 6)
- Operation idempotency envelope cache (DB 7)

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
```

**Configuration**: Path helpers are centralized in [`robosystems/config/storage/`](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/storage/README.md) with `shared.py` for external data sources and `graph.py` for customer graph storage

## Infrastructure

### AWS Services

**Compute**:
- **ECS Fargate**: API and Dagster (webserver, daemon, run workers) on ARM64/Graviton
- **EC2**: LadybugDB writer clusters (ARM64/Graviton auto-scaling groups with EBS persistence)
- **Lambda**: Infrastructure management (instance monitoring, secret rotation, volume lifecycle)

**Database & Cache**:
- **RDS PostgreSQL**: Primary database (auto-scaling storage, optional Multi-AZ)
- **ElastiCache**: Valkey/Redis cache
- **DynamoDB**: Service registries (on-demand pricing)

**Search & Analytics**:
- **OpenSearch Service**: Unstructured content search with keyword and semantic capabilities (single-node by default — the template's `InstanceCount` parameter adds data nodes for a Multi-AZ domain; VPC-private, feature-flag gated)

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
- **`worker.yaml`**: ECS Fargate background task worker (the Valkey-queue consumer)

#### LadybugDB Infrastructure

- **`graph-infra.yaml`**: Base infrastructure (DynamoDB registries, security groups, IAM roles, SNS alerts)
- **`graph-volumes.yaml`**: EBS volume lifecycle management (auto-expansion, snapshots, retention)
- **`graph-ladybug.yaml`**: EC2 writer clusters for LadybugDB (all customer tiers are dedicated — one database per instance)
- **`graph-ladybug-replicas.yaml`**: Read replica fleet for shared repositories (download the `.lbug` database from S3 on boot)

#### Search

- **`opensearch.yaml`**: Amazon OpenSearch Service domain for unstructured content search (VPC-private, feature-flag gated)

#### Security & Compliance

- **`security.yaml`**: Account-global detective controls baseline
- **`audit.yaml`**: Long-retention forwarding of security-audit logs to S3

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
         │   (S3 Bucket)       │    │ (App Runner / ECS)  │
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

**AWS App Runner or Amazon ECS Express Mode**:
- Serverless container hosting with automatic scaling, chosen per deployment by the `app_compute` input (`APP_COMPUTE` variables)
- `apprunner` is the default; an AWS account that has never used App Runner, which is closed to new customers, deploys with `ecs-express` instead
- No load balancer management required
- Health checks via `/api/utilities/health`

**CloudFront Distribution**:
- Global edge caching for static assets
- SSL/TLS termination with ACM certificates
- Origin routing: S3 for static files, the app service for dynamic content
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
- **Trigger**: Manual workflow dispatch (`prod.yml` / `staging.yml`)
- **Build**: Docker image → ECR
- **Deploy**: CloudFormation → App Runner or ECS Express Mode, plus CloudFront
- **Auth**: GitHub OIDC (no stored AWS credentials)

See individual app repositories for specific deployment workflows.

## External Integrations

### SEC EDGAR

**Purpose**: Public company financial data

- XBRL filing downloads via Dagster pipeline (quarterly partitions)
- XBRL parsing on xbrlkit (Arelle underneath) with fastembed enrichment
- Nightly incremental chain on weekdays (download → process → wake the parked shared master → stage → materialize → publish → replica refresh); the master sleeps again once everything is published
- Same-day pickups through the day that refresh the per-filer catalog only, without waking the master or touching the graph
- 8-K earnings releases and their exhibits captured every half hour during EDGAR's business hours, kept with each filing's documents and read whole through the filing-text views
- Schedules ship stopped; a deployment turns them on in the Dagster UI
- Curated canonical-concept resolution mapping natural language to XBRL element names via the `resolve-element` MCP tool
- OpenSearch text and semantic indexing of filing narrative sections and iXBRL text-block disclosures
- Rate-limited API access with backoff

### QuickBooks API

**Purpose**: Accounting data synchronization

- OAuth 2.0 authentication with token management
- Dagster pipeline: extract from the QuickBooks API to Parquet → dbt-duckdb transform → load into the extensions PostgreSQL database, which then materializes into the graph (Pattern C in the [Pipeline Guide](Pipeline-Guide))
- Full rebuild and incremental sync modes

### AI Models (via AWS Bedrock)

**Purpose**: AI-powered financial operations

- AWS Bedrock (the Converse API) for model access: Claude Sonnet 5.5 by default (the balanced profile), with Claude Opus 5.5 (quality) and OpenAI GPT-5.6 Luna (economy) as the other profiles; the `author` operator runs on the quality profile
- Self-hosted stacks can point operators at any OpenAI-compatible server instead (see [Self-Hosted AI Models](Self-Hosted-AI-Models))
- Unified operator system: stateless operators with automatic credit tracking per call
- Three registered operators: `analyst` (`AnalystOperator`, answers a question from the graph's read-only tools), `author` (`AuthorOperator`, makes a requested change through writes that are reversible or wait on a person's step — metric and forecast blocks, counterparties, memories, draft reports, inbox classifications), and `mapping` (`MappingOperator`, autonomous CoA→rs-gaap taxonomy mapping)
- MCP tools provide operators with graph queries, taxonomy operations, SEC structure discovery, and document search
- Every operator run executes on the background worker, with progress over SSE; `?mode=sync` waits up to 50 seconds for the answer

## Key Design Principles

### Multi-Tenancy

- **Database Isolation**: Each entity gets a dedicated graph database on a dedicated instance
- **Data Isolation**: Each graph's data is structurally separate — its own LadybugDB database, and
  its own PostgreSQL schema in the extensions database, with no shared tenant tables to filter. The
  platform binds each session to the schema of the `graph_id` it has already authorized
  (`search_path`)
- **Access Control**: Two nested boundaries — the org owns and pays, the graph is the unit of
  access. Org roles (`owner` / `admin` / `member`) compose with per-graph roles
  (`viewer` < `member` < `admin`) into one effective role, resolved before any handler runs
  (see [Identity & Org Model](#identity--org-model))
- **Resource Limits**: Tier-based resource allocation, with per-org provisioning caps

### Scalability

- **Horizontal Scaling**: Auto-scaling groups for writers
- **Vertical Scaling**: Tiered instance types (large → xlarge)
- **Caching**: Valkey for hot data
- **Pipeline Orchestration**: Dagster for data pipelines and batch operations
- **Real-time Operations**: Background worker with Valkey queue for SSE-enabled operator tasks

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