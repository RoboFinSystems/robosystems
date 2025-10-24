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
                           ↓
┌─────────────────────────────────────────────────────────┐
│                Operations & Middleware                  │
│           (Graph Router, Credits, Query Queue)          │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│                  Graph API Layer                        │
│          (Backend Abstraction, DuckDB Staging)          │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│               Graph Database Backends                   │
│                  (Kuzu Primary Backend)                 │
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

Model Context Protocol server for AI integration:

- **Claude Integration**: Native support for Claude Desktop
- **Natural Language Queries**: Convert user intent to Cypher
- **Financial Analysis**: Specialized tools for financial data exploration
- **Tool Registry**: Discover available graph operations

### Celery Workers

**Location**: `/robosystems/tasks/`, `/robosystems/celery.py`

Asynchronous task processing with priority queues:

- **SEC Data Pipeline**: Background processing of XBRL filings
- **QuickBooks Sync**: Periodic accounting data synchronization
- **Bulk Operations**: Large-scale data imports and transformations
- **Priority Queues**: High-priority tasks processed first

## Graph Database System

### Pluggable Backend Architecture

RoboSystems supports multiple graph database backends:

**Active Backends:**
- **Kuzu**: Primary backend - high-performance embedded graph database with columnar storage
  - All production tiers use Kuzu
  - Optimized for analytical workloads
  - Native DuckDB integration for bulk ingestion

**Optional Backends (Disabled by Default):**
- **Neo4j Community**: Client-server architecture with advanced query features
- **Neo4j Enterprise**: Clustering and advanced security features

The system is currently **Kuzu-first** with Neo4j available as opt-in tiers.

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

- **Backend Abstraction**: Common interface for Kuzu and Neo4j implementations
- **Factory Pattern**: Dynamic backend selection based on configuration (`GRAPH_BACKEND_TYPE`)
- **Backend-Specific Optimizations**: Each backend implements optimal patterns for their architecture
- **Connection Management**: Backend-appropriate connection pooling and lifecycle management
- **Query Translation**: Cypher query execution with backend-specific optimizations
- **Ingestion Strategies**: Parquet-optimized bulk data loading for each backend type

**See**: [Backend Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/backends/README.md) in codebase

### Client Factory System

**Location**: `/robosystems/graph_api/client/`

The client factory layer provides intelligent routing between application code and graph database infrastructure:

- **Backend-Agnostic**: Works seamlessly with both Kuzu and Neo4j backends
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
- **Direct Integration**: Native DuckDB → Kuzu via extensions
- **Schema-Driven**: Tables auto-created from graph schema DDL
- **S3 Integration**: Direct S3 file access via httpfs extension

### Infrastructure Design

**Cluster-Based Deployment:**
- **Writer Clusters**: EC2 auto-scaling groups for graph writes
- **Multi-Tenant Isolation**: Each entity gets dedicated database
- **Shared Repositories**: Common databases (SEC, industry, economic data)
- **DynamoDB Registry**: Track database allocation across instances
- **API-First**: All access through REST APIs, no direct connections

**Production Tier System (Kuzu):**

- **kuzu-standard**: Multi-tenant, cost-optimized entry
  - Instance: r7g.large (16GB RAM, 2 vCPUs)
  - Density: 10 databases per instance
  - Memory: 2GB per database (oversubscribed for cost efficiency)
  - Subgraphs: Not supported (single database only)
  - Storage: 500GB per database

- **kuzu-large**: Dedicated instance with subgraph support
  - Instance: r7g.large (16GB RAM, 2 vCPUs)
  - Density: 1 customer per instance (11 total databases with subgraphs)
  - Subgraphs: Up to 10 subgraphs (1 parent + 10 children)
  - Storage: 2TB total across all databases

- **kuzu-xlarge**: Maximum performance and scale
  - Instance: r7g.xlarge (32GB RAM, 4 vCPUs)
  - Density: 1 customer per instance (26 total databases with subgraphs)
  - Subgraphs: Up to 25 subgraphs (1 parent + 25 children)
  - Storage: 5TB total across all databases

- **kuzu-shared**: Public data repositories
  - Instance: r7g.large
  - Purpose: SEC filings, industry data, economic indicators
  - Read-only for all users

**Staging Environment:**
- kuzu-standard: r7g.medium (8GB RAM)
- kuzu-large: r7g.medium (8GB RAM)
- kuzu-shared: r7g.medium (8GB RAM)

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

### Processors

**Location**: `/robosystems/processors/`

Data transformation pipeline and business logic:

**XBRL Processing Pipeline** (`/processors/xbrl/`):
- **`DataFrameManager`**: DataFrame validation, cleaning, and transformation
- **`XBRLDuckDBGraphProcessor`**: DuckDB staging to graph database ingestion
- **`ParquetWriter`**: Optimized Parquet file generation for graph ingestion
- **`TextBlockExternalizer`**: Large text extraction and externalization

**Financial Processing**:
- **`TrialBalanceProcessor`**: Financial calculations and accounting operations
- **`ScheduleProcessor`**: Financial schedule generation and formatting
- **`QBTransactionsProcessor`**: QuickBooks data processing and normalization

**See**: [Processors Documentation](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/processors/README.md) in codebase

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

- **Kuzu**: Primary embedded graph database backend
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

**Purpose**: Message broker and caching

**Database Allocation**:
- DB 0: Celery broker
- DB 1: Celery results
- DB 2: Authentication cache
- DB 3: Server-sent events
- DB 4: Distributed locks
- DB 5: Pipeline tracking
- DB 6: Credits cache
- DB 7: Rate limiting
- DB 8: Kuzu client caching

### AWS S3

**Purpose**: Document storage and data lake

- **User uploads**: Parquet files for graph ingestion
- **Backups**: Database snapshots and archives
- **SEC data**: XBRL filing storage
- **Graph exports**: Database dumps and migrations

## Infrastructure

### AWS Services

**Compute**:
- **ECS Fargate**: API and worker containers (ARM64/Graviton)
- **EC2**: Graph database writer instances (ARM64/Graviton auto-scaling groups)
- **Lambda**: Management functions (monitoring, rotation, volume management)

**Database & Cache**:
- **RDS PostgreSQL**: Primary database (db.t4g.micro, auto-scaling storage)
- **ElastiCache**: Valkey/Redis (cache.t4g.micro)
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

## CI/CD & Deployment

### GitHub Actions Workflows

All deployments automated through GitHub Actions with self-hosted runners for infrastructure operations:

- **`prod.yml`**: Production deployment orchestrator
- **`staging.yml`**: Staging environment deployment
- **`bootstrap.yml`**: One-time infrastructure initialization
- **`run-migrations.yml`**: Database migrations on RDS instances

**Runner Configuration**:
- Self-hosted runner for infrastructure operations
- GitHub-hosted for Docker builds
- Automated deployments: `staging` branch → staging, `main` → production

### CloudFormation Templates

All infrastructure is managed through CloudFormation templates in `/cloudformation/`:

#### Core Infrastructure

- **`vpc.yaml`**: VPC, subnets, NAT gateways, VPC endpoints, network configuration, and VPC Flow Logs
- **`cloudtrail.yaml`**: CloudTrail AWS Audit Logging for compliance purposes
- **`s3.yaml`**: S3 buckets for data storage, backups, and CloudFormation templates
- **`postgres.yaml`**: RDS PostgreSQL database with auto-scaling storage and automated backups
- **`valkey.yaml`**: ElastiCache Valkey for caching and message broker

#### API & Workers

- **`api.yaml`**: ECS Fargate API service with auto-scaling, load balancing, and health checks
- **`waf.yaml`**: AWS Web Application Firewall for protecting the API from web exploits
- **`worker.yaml`**: ECS Fargate Celery workers with priority queues and spot capacity
- **`worker-monitor.yaml`**: Lambda function for monitoring worker health and queue depths
- **`beat.yaml`**: Celery beat scheduler for periodic tasks and cron jobs

#### Graph Database Infrastructure

- **`graph-infra.yaml`**: Base infrastructure for graph database clusters (security groups, roles, registries)
- **`graph-volumes.yaml`**: EBS volume management and snapshot automation for graph databases
- **`kuzu-writers.yaml`**: Auto-scaling EC2 writer clusters with tiered instance types (Kuzu backend)
- **`neo4j-writers.yaml`**: Auto-scaling EC2 writer clusters with tiered instance types (Neo4j backend)

#### Observability

- **`prometheus.yaml`**: Amazon Managed Prometheus for metrics collection
- **`grafana.yaml`**: Amazon Managed Grafana for visualization and dashboards

#### CI/CD & Support

- **`gha-runner.yaml`**: Self-hosted GitHub Actions runner on EC2 spot instances
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
just bootstrap     # Bootstrap infrastructure
```

## External Integrations

### SEC EDGAR

**Purpose**: Financial filing data source

- XBRL filing downloads
- Metadata extraction
- Bulk processing with parallel workers
- Rate-limited API access

### QuickBooks API

**Purpose**: Accounting data synchronization

- OAuth-based authentication
- Real-time account sync
- Transaction processing
- Trial balance generation

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
- **Async Processing**: Celery for background jobs

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

- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - Financial data loading
- **Component READMEs**: Detailed docs in codebase at `/robosystems/*/README.md`
- **API Reference**: http://localhost:8000/docs (local) or https://api.robosystems.ai/docs (production)

## Support

- **GitHub Issues**: [robosystems/issues](https://github.com/RoboFinSystems/robosystems/issues)
- **Discussions**: [robosystems/discussions](https://github.com/RoboFinSystems/robosystems/discussions)
- **Wiki**: [robosystems/wiki](https://github.com/RoboFinSystems/robosystems/wiki)
