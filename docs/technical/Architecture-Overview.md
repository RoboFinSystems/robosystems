# Architecture Overview

RoboSystems is built on a modern, scalable architecture designed for enterprise financial data processing and knowledge graph management.

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
│          (SEC, QuickBooks, Plaid Integrations)          │
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

The FastAPI backend provides a RESTful API with:

- **Versioned Endpoints**: All routes under `/v1/` for API stability
- **Multi-Tenant Routing**: Database-scoped endpoints at `/v1/graphs/{graph_id}/*`
- **Authentication**: JWT tokens and API keys
- **OpenAPI Documentation**: Auto-generated at `/docs`
- **Async Operations**: Non-blocking I/O for high throughput

### MCP Server

**Server Location**: `/robosystems/routers/graphs/mcp/`
**Middleware Location**: `/robosystems/middleware/mcp/`

Model Context Protocol server for AI integration:

- **MCP Endpoints**: FastAPI router exposing MCP-compliant tool endpoints
- **Specialized Tools**: Cypher queries, schema introspection, fact grids, workspace management
- **Streaming Support**: SSE for memory-efficient large result processing
- **Query Validation**: Complexity scoring and timeout enforcement

**Client Integration**: External MCP client (`@robosystems/mcp` npm package) connects Claude Desktop to the server.

**See**: [MCP Middleware Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/mcp/README.md) in codebase

### Agent Interface

**Location**: `/robosystems/routers/graphs/agent/`

AI agent execution system for natural language graph interactions:

- **Text-to-Cypher**: Convert natural language queries to Cypher
- **Automatic Strategy Selection**: Choose optimal execution approach based on query intent
- **Streaming Responses**: SSE support for real-time progress monitoring
- **Background Processing**: Queue integration for extended analysis operations

### Dagster Orchestration

**Location**: `/robosystems/dagster/`

Dagster is the **sole orchestration system** for all scheduled tasks, event-driven triggers, and data pipelines. There is no separate task queue - everything runs through Dagster.

**Architecture:**
```
dagster/
├── assets/           # Software-Defined Assets (data pipelines)
│   ├── graphs.py     # User graph operation assets
│   └── sec/          # SEC XBRL pipeline (modular)
│       ├── configs.py     # Configuration classes
│       ├── download.py    # sec_raw_filings asset
│       ├── process.py     # sec_processed_filings asset
│       ├── stage.py       # DuckDB staging assets
│       ├── materialize.py # LadybugDB materialization assets
│       ├── entity_update.py  # Entity incremental update
│       └── backup.py      # SEC backup asset
├── jobs/             # Job definitions (all system tasks)
│   ├── billing.py    # Credit allocation, storage billing
│   ├── infrastructure.py  # Auth cleanup, health checks, instance monitoring
│   ├── graph.py      # Graph operations (create, backup, restore)
│   ├── provisioning.py    # Subscription provisioning
│   ├── sec.py        # SEC jobs and schedules
│   └── shared_repository.py # Shared repository operations
├── sensors/          # Event-driven triggers
│   ├── provisioning.py    # Watch for pending subscriptions
│   └── sec.py        # SEC processing sensor
├── resources/        # Dagster resources (DB, S3, Graph)
└── definitions.py    # Dagster definitions registry
```

**What Dagster Handles:**

| Category | Description |
|----------|-------------|
| **Billing** | Credit allocation, storage billing, usage collection |
| **Infrastructure** | Auth cleanup, health checks, instance monitoring |
| **Graph Operations** | Database creation, backup/restore, materialization |
| **Data Pipelines** | SEC XBRL pipeline (download, process, stage, materialize) |
| **Event Triggers** | Subscription provisioning, repository sync, SEC processing |

**Key Features:**
- **Single System**: All orchestration in one place - no separate task queues
- **Asset Dependencies**: Declarative data lineage with automatic orchestration
- **Year Partitioning**: Process historical data by year
- **Observability**: Built-in UI at `localhost:3003` for monitoring
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

### Infrastructure Design

**Cluster-Based Deployment:**
- **Writer Clusters**: EC2 auto-scaling groups for graph writes
- **Multi-Tenant Isolation**: Each entity gets dedicated database
- **Shared Repositories**: Common databases (SEC, industry, economic data)
- **DynamoDB Registry**: Track database allocation across instances
- **API-First**: All access through REST APIs, no direct connections

**Tier System:**

The platform offers multiple tiers with different isolation levels and capabilities:


| Tier | Display Name | Isolation | Subgraphs | Description |
|------|--------------|-----------|-----------|-------------|
| `ladybug-standard` | LadybugDB Standard | Multi-tenant | 0 | Cost-optimized entry tier, single database |
| `ladybug-large` | LadybugDB Large | Dedicated r7g.large | 10 max | Dedicated instance for team collaboration |
| `ladybug-xlarge` | LadybugDB XLarge | Dedicated r7g.xlarge | 25 max | Maximum performance and scale |
| `ladybug-shared` | — | Platform | — | Public repositories (SEC, industry, economic) |

*Note: Neo4j backend tiers exist in the codebase but are experimental and disabled by default.*

### Subgraphs (Workspaces)

Subgraphs provide isolated database environments within a parent graph, available on Large and XLarge tiers:

**Use Cases:**
- **AI Memory Layer**: Create graphs for storing and querying AI generated memories 
- **Development**: Test schema changes or new data pipelines without affecting production
- **Team Collaboration**: Give teams isolated workspaces that share the parent's infrastructure
- **Staging**: Validate data before promoting to the main graph

**How Subgraphs Work:**
- Created on the same EC2 instance as the parent graph (no additional infrastructure cost)
- Share the parent's credit pool and subscription limits
- Inherit schema from parent, with optional extensions or custom schemas
- Fully isolated data - queries cannot cross subgraph boundaries

**Naming & Identification:**
- Subgraph names must be alphanumeric, 1-20 characters (no hyphens or underscores)
- Full ID format: `{parent_graph_id}_{subgraph_name}` (e.g., `kg123abc_dev`)
- Switch between parent and subgraphs via MCP tools

**Limits by Tier:**
| Tier | Max Subgraphs | Total Databases |
|------|---------------|-----------------|
| Standard | 0 | 1 (parent only) |
| Large | 10 | 11 (1 parent + 10 subgraphs) |
| XLarge | 25 | 26 (1 parent + 25 subgraphs) |

For current tier specifications including instance types, memory allocation, subgraph limits, and pricing, see:
- **[Graph Configuration](https://github.com/RoboFinSystems/robosystems/blob/main/.github/configs/graph.yml)** - Infrastructure specifications
- **[Billing Configuration](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/billing/)** - Subscription plans, AI costs, and pricing

## Data Processing Layer

### Operations Layer

**Location**: `/robosystems/operations/`

Business workflow orchestration and service layer:

- **Entity Graph Service**: Entity-specific graph creation workflows with curated schemas
- **Generic Graph Service**: Custom schema graph creation with user-defined node/relationship types
- **Table Service**: Schema-driven DuckDB staging table management and file ingestion
- **Data Ingestion**: High-performance bulk data loading using COPY operations
- **Credit Service**: AI agent usage tracking with token-based consumption

**See**: [Operations Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md) in codebase

### Adapters

**Location**: `/robosystems/adapters/`

External service integrations following a consistent client/processor pattern:

**Architecture:**
```
adapters/
├── sec/                    # SEC EDGAR adapter (active)
│   ├── client/
│   │   ├── edgar.py        # SECClient - API client
│   │   ├── arelle.py       # ArelleClient - XBRL parsing
│   │   ├── downloader.py   # Bulk file downloads
│   │   └── efts.py         # EFTS filing discovery
│   ├── metadata.py         # SECMetadataLoader
│   └── processors/
│       ├── constants.py    # Shared constants (SHARED_NODE_TABLES)
│       ├── xbrl_graph.py   # XBRLGraphProcessor
│       ├── processing.py   # Single filing processing
│       ├── consolidation.py # Parquet consolidation
│       ├── parquet.py      # Parquet file output
│       ├── schema.py       # Schema adapter
│       └── ingestion/      # DuckDB/LadybugDB ingestion
│           ├── staging.py      # DuckDBStager
│           ├── materializer.py # LadybugMaterializer
│           ├── direct_copy.py  # LadybugDirectCopier
│           └── processor.py    # XBRLDuckDBGraphProcessor
├── quickbooks/             # QuickBooks adapter (stubbed)
│   ├── client/api.py       # QBClient - OAuth client
│   └── processors/         # Transaction processing (stubbed)
└── plaid/                  # Plaid adapter (stubbed)
    ├── client/api.py       # PlaidClient
    └── processors/         # Transaction sync (stubbed)
```

**Adapter Pattern:**
Each adapter consists of:
1. **Client** - API connection, authentication, rate limiting
2. **Processors** - Data transformation to graph-ready format
3. **Models** (optional) - Service-specific data models

**SEC Adapter Details:**
- **SECClient**: SEC EDGAR API with rate limiting and retry logic
- **XBRLGraphProcessor**: Transforms XBRL to DataFrames using Arelle
- **XBRLDuckDBGraphProcessor**: Orchestrates S3 → DuckDB → LadybugDB flow
- Creates nodes: Entity, Report, Fact, Element, Period, Unit, Taxonomy
- Outputs year-partitioned Parquet files to S3

**Fork-Friendly Extensibility:**

The adapter directory structure is designed as a **merge boundary** for forks. Add custom data sources in the `custom_*` namespace:

```
adapters/
├── sec/                 # ← Upstream (don't modify)
├── quickbooks/          # ← Upstream (don't modify)
├── plaid/               # ← Upstream (don't modify)
└── custom_*/            # ← Your namespace (upstream never touches)
    └── custom_erp/      #    Your custom integrations
```

This enables `git pull upstream main` without merge conflicts on your custom adapters.

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

**IAM Models** (`/robosystems/models/iam/`):
- SQLAlchemy models for PostgreSQL database interactions
- Foundation for Alembic migrations and schema management
- Multi-tenant architecture with user management and access control
- Credit system and usage analytics models

**See**: [IAM Models Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/iam/README.md) in codebase

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
- **Shared Repositories**: SEC, industry, economic data in dedicated read-optimized clusters
- **Subgraphs**: Isolated workspaces for teams, AI memory, and environments (see [Subgraphs](#subgraphs-workspaces))
- **Cypher Queries**: Full Cypher query language support for graph traversal and analytics

### DuckDB

**Purpose**: Staging database for bulk data ingestion

- **One database per graph**: `{DUCKDB_STAGING_PATH}/{graph_id}.duckdb`
- **Materialized tables**: Data loaded from S3 Parquet files (not views)
- **Validation layer**: SQL queries before graph ingestion
- **High performance**: Columnar storage, direct S3 access
- **Default path**: `./data/staging/` (configurable via `DUCKDB_STAGING_PATH`)

### DynamoDB

**Purpose**: Service registry and metadata

- **Instance Registry**: Track graph database instances
- **Graph Registry**: Map graphs to instances
- **Volume Registry**: EBS volume management
- **Fast lookups**: Sub-millisecond access times

### PostgreSQL

**Purpose**: Primary relational database

- **Identity & Access Management**: Users, orgs, permissions, graphs
- **Subscription Management**: Plans, billing, credits
- **File Registry**: Track uploaded files per table (GraphFile, GraphTable)
- **Schema Registry**: Graph schema definitions (GraphSchema)
- **Dagster Orchestration**: Job runs, schedules, sensors, and asset metadata

### Valkey (Redis)

**Purpose**: Caching and coordination

- Authentication tokens and sessions
- Server-sent events pub/sub
- Distributed locks
- Pipeline tracking
- Credit balance caching
- Rate limiting counters
- Graph client caching

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
# Shared data (SEC, FRED, etc.)
s3://robosystems-shared-raw-{env}/
  sec/year=2024/{cik}/{accession}.zip

s3://robosystems-shared-processed-{env}/
  sec/year=2024/nodes/Entity/*.parquet

# User/graph data
s3://robosystems-user-{env}/
  user-staging/{user_id}/{graph_id}/{table}/*.parquet
  graph-backups/databases/{graph_id}/full/*.lbug.gz
  graph-databases/{env}/{graph_id}/*.tar.gz
```

**Configuration**: Path helpers are centralized in [`robosystems/config/storage/`](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/storage/README.md) with `shared.py` for external data sources and `graph.py` for customer graph storage

## Infrastructure

### AWS Services

**Compute**:
- **ECS Fargate**: API and Dagster (webserver, daemon, run workers) on ARM64/Graviton
- **EC2**: LadybugDB writer clusters (ARM64/Graviton auto-scaling groups with EBS persistence)
- **Lambda**: Infrastructure management (instance monitoring, secret rotation, volume lifecycle)

**Database & Cache**:
- **RDS (Aurora) PostgreSQL**: Primary database (auto-scaling storage)
- **ElastiCache**: Valkey/Redis cache
- **DynamoDB**: Service registries (on-demand pricing)

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
- **`graph-ladybug-replicas.yaml`**: Read replica fleet for shared repositories (SEC, industry, economic)

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

**Purpose**: Financial filing data source

- XBRL filing downloads via Dagster pipeline
- Year-partitioned processing (2019-2025)
- Arelle-based XBRL parsing and transformation
- Rate-limited API access with backoff

### QuickBooks API

**Purpose**: Accounting data synchronization

- OAuth-based authentication
- Real-time account sync
- Transaction processing
- Trial balance generation

### Plaid API

**Purpose**: Banking and financial institution connections

- OAuth-based Link integration
- Transaction synchronization via Dagster
- Account balance tracking
- Multi-institution support

### Anthropic Claude (via AWS Bedrock)

**Purpose**: AI-powered financial analysis

- AWS Bedrock as the interface to Claude models
- Natural language queries
- GraphRAG-enabled responses
- Token-based billing
- Multi-agent architecture

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
- **Real-time Operations**: FastAPI background tasks for SSE-enabled interactions

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

## Related Documentation

**Wiki Guides:**
- **[Bootstrap Guide](Bootstrap-Guide)** - Set up a fresh fork with AWS OIDC authentication
- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - Load and query SEC financial filings
- **[Accounting Demo](Accounting-Demo)** - Interactive demo of graph-based accounting
- **[Custom Graph Schema](Custom-Graph-Schema)** - Design and build custom graph databases

**Codebase Documentation:**
- **[Dagster Pipelines](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/dagster/README.md)** - Pipeline orchestration and assets
- **[Adapters](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/adapters/README.md)** - External service integrations
- **[Graph API](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/README.md)** - Graph database abstraction layer
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)