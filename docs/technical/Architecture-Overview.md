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
│               Graph Database Backends                   │
│               (Ladybug Primary Backend)                 │
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

**Location**: `/robosystems/middleware/mcp/`

Model Context Protocol server for AI integration via the FastAPI Backend:

- **Claude Integration**: Native support for Claude Desktop
- **Natural Language Queries**: Convert user intent to Cypher
- **Financial Analysis**: Specialized tools for financial data exploration
- **Tool Registry**: Discover available graph operations

### Dagster Orchestration

**Location**: `/robosystems/dagster/`

Dagster is the **sole orchestration system** for all scheduled tasks, event-driven triggers, and data pipelines. There is no separate task queue - everything runs through Dagster.

**Architecture:**
```
dagster/
├── assets/           # Software-Defined Assets (data pipelines)
│   ├── sec.py        # SEC XBRL pipeline (5 assets)
│   ├── quickbooks.py # QuickBooks pipeline
│   └── plaid.py      # Plaid banking pipeline
├── jobs/             # Job definitions (all system tasks)
│   ├── sec.py        # SEC jobs and schedules
│   ├── billing.py    # Credit allocation, storage billing
│   ├── infrastructure.py  # Auth cleanup, health checks
│   ├── graph.py      # Graph operations (create, backup, restore)
│   └── provisioning.py    # Subscription provisioning
├── sensors/          # Event-driven triggers
│   └── provisioning.py    # Watch for pending subscriptions
├── partitions/       # Partition definitions
│   └── sec.py        # Year-based partitions (2019-2025)
└── definitions.py    # Dagster definitions registry
```

**What Dagster Handles:**

| Category | Jobs | Description |
|----------|------|-------------|
| **Billing** | 4 jobs, 4 schedules | Credit allocation, storage billing, usage collection |
| **Infrastructure** | 7 jobs, 7 schedules | Auth cleanup, health checks, instance monitoring |
| **Graph Operations** | 7 jobs | Database creation, backup/restore, materialization |
| **Data Pipelines** | SEC, QuickBooks, Plaid | Software-Defined Assets with dependencies |
| **Event Triggers** | 2 sensors | Subscription provisioning, repository sync |

**SEC Pipeline Assets:**
1. `sec_companies_list` → Fetch company list from SEC EDGAR
2. `sec_raw_filings` → Download XBRL ZIPs (year-partitioned)
3. `sec_processed_filings` → Process to Parquet via adapters
4. `sec_duckdb_staging` → Create DuckDB staging tables
5. `sec_graph_materialized` → Materialize to LadybugDB

**Key Features:**
- **Single System**: All orchestration in one place - no separate task queues
- **Asset Dependencies**: Declarative data lineage with automatic orchestration
- **Year Partitioning**: Process historical data by year (2019-2025)
- **Observability**: Built-in UI at `localhost:3003` for monitoring
- **Scheduling**: Cron-based schedules for billing, cleanup, and pipelines
- **Sensors**: Event-driven job triggers (e.g., new subscription → provision graph)
- **Local Development**: `just sec-load NVDA 2025` triggers pipeline via Docker CLI

**Infrastructure (AWS):**
- **Fargate**: Daemon (singleton) + Webserver (1-3 replicas) - always on, lightweight
- **EC2 Run Workers**: r7g.large with 500GB EBS - scale 0-5 based on job queue
- **Scale to Zero**: No EC2 costs when no jobs running

## Graph Database System

### Pluggable Backend Architecture

RoboSystems supports multiple graph database backends:

**Active Backends:**
- **Ladybug**: Primary backend - high-performance embedded graph database with columnar storage
  - Optimized for analytical workloads
  - Native DuckDB integration for bulk ingestion

**Optional Backends:**
- **Neo4j Community**: Client-server architecture with advanced query features
- **Neo4j Enterprise**: Clustering and advanced security features

The system is currently **Ladybug-first** with Neo4j available as opt-in tiers.

### Graph API

**Location**: `/robosystems/graph_api/`

FastAPI microservice providing unified interface for all backends:

- **HTTP REST Interface**: Port 8001 (default) or 8002 (optional Neo4j instances)
- **Backend Abstraction**: Consistent API regardless of backend
- **Multi-Database Management**: Multiple databases per instance
- **Connection Pooling**: Efficient resource management
- **Streaming Support**: NDJSON for large query results
- **Admission Control**: CPU/memory backpressure

**See**: [Graph API Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/README.md) in codebase

### Backend Factory System

**Location**: `/robosystems/graph_api/backends/`

The backend factory provides pluggable graph database backends with a unified interface:

- **Backend Abstraction**: Common interface for Ladybug and Neo4j implementations
- **Factory Pattern**: Dynamic backend selection based on configuration (`GRAPH_BACKEND_TYPE`)
- **Backend-Specific Optimizations**: Each backend implements optimal patterns for their architecture
- **Connection Management**: Backend-appropriate connection pooling and lifecycle management
- **Query Translation**: Cypher query execution with backend-specific optimizations
- **Ingestion Strategies**: Parquet-optimized bulk data loading for each backend type

**See**: [Backend Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/backends/README.md) in codebase

### Client Factory System

**Location**: `/robosystems/graph_api/client/`

The client factory layer provides intelligent routing between application code and graph database infrastructure:

- **Backend-Agnostic**: Works seamlessly with both Ladybug and Neo4j backends
- **Automatic Discovery**: Finds database instances via DynamoDB registry
- **Redis Caching**: Caches instance locations to reduce lookups
- **Circuit Breakers**: Prevents cascading failures with automatic recovery
- **Connection Reuse**: HTTP/2 connection pooling for efficiency
- **Retry Logic**: Exponential backoff with jitter for transient errors

**See**: [Client Factory Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/client/README.md) in codebase

### DuckDB Staging System

**Location**: `/robosystems/graph_api/core/duckdb_*.py`

High-performance data ingestion via intermediate staging:

- **Staging Tables**: DuckDB materialized tables for data preparation
- **File-Based Workflow**: Users upload Parquet → DuckDB validates → Graph ingests
- **Direct Integration**: Native DuckDB → Ladybug via extensions
- **Schema-Driven**: Tables auto-created from graph schema DDL
- **S3 Integration**: Direct S3 file access via httpfs extension

### Infrastructure Design

**Cluster-Based Deployment:**
- **Writer Clusters**: EC2 auto-scaling groups for graph writes
- **Multi-Tenant Isolation**: Each entity gets dedicated database
- **Shared Repositories**: Common databases (SEC, industry, economic data)
- **DynamoDB Registry**: Track database allocation across instances
- **API-First**: All access through REST APIs, no direct connections

**Production Tier System (Ladybug):**

- **ladybug-standard**: Multi-tenant, cost-optimized entry
  - Instance: r7g.large (16GB RAM, 2 vCPUs)
  - Density: 10 databases per instance
  - Memory: 2GB per database (oversubscribed for cost efficiency)
  - Subgraphs: Not supported (single database only)
  - Storage: 500GB per database

- **ladybug-large**: Dedicated instance with subgraph support
  - Instance: r7g.large (16GB RAM, 2 vCPUs)
  - Density: 1 customer per instance (11 total databases with subgraphs)
  - Subgraphs: Up to 10 subgraphs (1 parent + 10 children)
  - Storage: 2TB total across all databases

- **ladybug-xlarge**: Maximum performance and scale
  - Instance: r7g.xlarge (32GB RAM, 4 vCPUs)
  - Density: 1 customer per instance (26 total databases with subgraphs)
  - Subgraphs: Up to 25 subgraphs (1 parent + 25 children)
  - Storage: 5TB total across all databases

- **ladybug-shared**: Public data repositories
  - Instance: r7g.large
  - Purpose: SEC filings, industry data, economic indicators
  - Read-only for all users

**Staging Environment:**
- ladybug-standard: r7g.medium (8GB RAM)
- ladybug-large: r7g.medium (8GB RAM)
- ladybug-xlarge: r7g.large (16GB RAM)
- ladybug-shared: r7g.medium (8GB RAM)

**Optional Neo4j Tiers (Disabled by Default):**
- neo4j-community-large: r7g.large (production) / r7g.medium (staging)
- neo4j-enterprise-xlarge: r7g.xlarge (production) / r7g.large (staging)

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
├── sec/                    # SEC EDGAR adapter
│   ├── client/
│   │   └── edgar.py        # SECClient - API client
│   ├── processors/
│   │   ├── graph.py        # XBRLGraphProcessor
│   │   └── duckdb_graph_ingestion.py  # XBRLDuckDBGraphProcessor
│   └── arelle/             # Arelle XBRL integration
├── quickbooks/             # QuickBooks adapter
│   ├── client/
│   │   └── api.py          # QBClient - OAuth client
│   └── processors/
│       └── transactions.py # Transaction processing
└── plaid/                  # Plaid banking adapter
    ├── client/
    │   └── api.py          # PlaidClient
    └── processors/
        └── transactions.py # PlaidTransactionsProcessor
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
- Graph Router: Intelligent cluster selection
- Allocation Manager: Database allocation via DynamoDB
- Query Queue: Admission control and long polling

**Security & Robustness**:
- Authentication & authorization
- Rate limiting (burst-focused, 1-minute windows)
- Credit tracking and billing
- Circuit breakers and retry policies

**Observability**:
- OpenTelemetry integration
- Request tracing
- Performance metrics

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

### Graph Databases

**Purpose**: Financial knowledge graphs with relationships

- **Ladybug**: Primary embedded graph database backend
- **Neo4j**: Optional managed graph database (disabled by default)
- **Multi-tenant**: Separate database per entity (`kg12345abc`)
- **Shared repositories**: SEC, industry, economic data in dedicated clusters

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

- **Identity & Access Management**: Users, permissions, graphs
- **Subscription Management**: Plans, billing, credits
- **File Registry**: Track uploaded files per table (GraphFile, GraphTable)
- **Schema Registry**: Graph schema definitions (GraphSchema)
- **Audit Logs**: Security and compliance tracking

### Valkey (Redis)

**Purpose**: Caching

**Database Allocation**:
- DB 2: Authentication cache
- DB 3: Server-sent events
- DB 4: Distributed locks
- DB 5: Pipeline tracking
- DB 6: Credits cache
- DB 7: Rate limiting
- DB 8: Ladybug client caching

### AWS S3

**Purpose**: Document storage and data lake

- **User uploads**: Parquet files for graph ingestion
- **Backups**: Database snapshots and archives
- **SEC data**: XBRL filing storage
- **Graph exports**: Database dumps and migrations

## Infrastructure

### AWS Services

**Compute**:
- **ECS Fargate**: API and Dagster webserver/daemon (ARM64/Graviton)
- **EC2**: Graph database writers and Dagster run workers (ARM64/Graviton auto-scaling groups)
- **Lambda**: Management functions (monitoring, rotation, volume management)

**Database & Cache**:
- **RDS PostgreSQL**: Primary database (auto-scaling storage)
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

### GitHub Actions Workflows

All deployments automated through GitHub Actions:

- **`prod.yml`**: Production deployment orchestrator
- **`staging.yml`**: Staging environment deployment
- **`test.yml`**: Automated test suite
- **`build.yml`**: Docker image building and ECR push

**Runner Configuration**:
- **GitHub-hosted** (default): Free for public repos, no setup required
- **Self-hosted** (optional): Forks can use their own org-level or repo-level runners by setting the `RUNNER_LABELS` repository variable

### CloudFormation Templates

All infrastructure is managed through CloudFormation templates in `/cloudformation/`:

#### Core Infrastructure

- **`vpc.yaml`**: VPC, subnets, NAT gateways, VPC endpoints, network configuration, and VPC Flow Logs
- **`cloudtrail.yaml`**: CloudTrail AWS Audit Logging for compliance purposes
- **`s3.yaml`**: S3 buckets for data storage, backups, and CloudFormation templates
- **`postgres.yaml`**: RDS PostgreSQL database with auto-scaling storage and automated backups
- **`valkey.yaml`**: ElastiCache Valkey for caching 

#### API & Workers

- **`api.yaml`**: ECS Fargate API service with auto-scaling, load balancing, and health checks
- **`waf.yaml`**: AWS Web Application Firewall for protecting the API from web exploits
- **`dagster.yaml`**: Dagster webserver and daemon for pipeline orchestration
- **`dagster-run-worker.yaml`**: EC2 auto-scaling group for Dagster run workers

#### Graph Database Infrastructure

- **`graph-infra.yaml`**: Base infrastructure for graph database clusters (security groups, roles, registries)
- **`graph-volumes.yaml`**: EBS volume management and snapshot automation for graph databases
- **`graph-ladybug.yaml`**: Auto-scaling EC2 writer clusters with tiered instance types (LadybugDB backend)
- **`graph-neo4j.yaml`**: Auto-scaling EC2 writer clusters with tiered instance types (Neo4j backend)

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

```bash
just setup-gha     # Interactive GitHub secrets/variables setup
just setup-aws     # Interactive AWS secrets manager setup
```

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

### Anthropic Claude

**Purpose**: AI-powered financial analysis

- Natural language queries
- GraphRAG-enabled responses
- Token-based billing
- Multi-agent architecture

## Key Design Principles

### Multi-Tenancy

- **Database Isolation**: Each entity gets dedicated graph database
- **Access Control**: Row-level security in PostgreSQL
- **Resource Limits**: Per-tier memory, storage, query limits

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
- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - Load and query SEC financial filings
- **[Accounting Demo](Accounting-Demo)** - Interactive demo of graph-based accounting
- **[Custom Graph Schema](Custom-Graph-Schema)** - Design and build custom graph databases

**Codebase Documentation:**
- **[Dagster Pipelines](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/dagster/README.md)** - Pipeline orchestration and assets
- **[Adapters](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/adapters/README.md)** - External service integrations
- **[Graph API](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/README.md)** - Graph database abstraction layer
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration

**API Documentation:**
- Local: [http://localhost:8000/docs](http://localhost:8000/docs)
- Production: [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs)
- Dagster UI: [http://localhost:3003](http://localhost:3003) (local development)

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Discussions](https://github.com/RoboFinSystems/robosystems/discussions)
- [Projects](https://github.com/RoboFinSystems/robosystems/projects)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)