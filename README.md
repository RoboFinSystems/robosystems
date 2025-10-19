# RoboSystems

RoboSystems is an enterprise-grade financial knowledge graph platform that transforms complex financial data into actionable intelligence through graph-based analytics and AI-powered insights.

- **Graph-Based Financial Intelligence**: Leverages graph database technology (Kuzu or Neo4j) to model complex financial relationships, enabling deep analysis of relationships between accounting, financial reporting, portfolio management, and public XBRL data
- **GraphRAG Architecture**: Knowledge graph-based retrieval-augmented generation for LLM-powered financial analysis over enterprise financial and operating data
- **Model Context Protocol (MCP)**: Standardized server and [client](https://www.npmjs.com/package/@robosystems/mcp) for LLM integration with natural language querying
- **Multi-Source Data Integration**: Seamlessly integrates QuickBooks accounting data, SEC XBRL filings (10-K, 10-Q), and custom financial datasets into a unified knowledge graph
- **Enterprise-Ready Infrastructure**: Multi-tenant architecture with tiered scaling (Standard/Enterprise/Premium), production-grade query management, and credit-based billing for sustainable operations
- **Developer-First API**: RESTful API with comprehensive endpoints for graph operations, data ingestion, and analysis - designed for integration with financial applications like RoboLedger and RoboInvestor

## Core Features

RoboSystems bridges the gap between raw financial data and actionable business intelligence by creating interconnected knowledge graphs that reveal hidden relationships, patterns, and insights that traditional databases miss. It's the backbone for next-generation financial applications that need to understand not just numbers, but the relationships and context behind them.

- **Multi-Tenant Graph Databases**: Create isolated graph database instances (Kuzu or Neo4j) with cluster-based scaling
- **AI Agent Interface**: Natural language financial analysis through Claude powered agents via Model Context Protocol (MCP)
- **Entity Graph Creation**: Curated enterprise financial data schemas for defined use cases with RoboLedger, RoboInvestor and more
- **Generic Graph Creation**: Custom schema definitions with custom node/relationship types
- **Subgraph Creation**: Create subgraphs with custom schemas for AI memory layers, version and access control
- **Shared Repositories**: XBRL SEC filings (10-K, 10-Q) knowledge graph for context mining with MCP
- **QuickBooks Integration**: Complete accounting synchronization with trial balance and report creation
- **Credit-Based Billing**: AI operations via in-house agents consume credits in proportion to token use (Anthropic/OpenAI)
- **Query Queue System**: Production-ready query management with admission control and load shedding

## Quick Start

### Docker Development Environment

```bash
# Install uv (Python package and version manager)
curl -LsSf https://astral.sh/uv/install.sh | sh
# Or on macOS with Homebrew: brew install uv

# Install just (command runner)
uv tool install rust-just

# Start all services (includes automatic migrations and seeds)
just start
```

This initializes the `.env` file and starts the complete RoboSystems stack with:

- Graph database (Kuzu by default, Neo4j optional)
- PostgreSQL with automatic migrations
- Valkey message broker
- All development services

### Local Development Environment

For local development with docker:

```bash
# Setup Python environment (uv automatically handles Python versions)
just init
```

## Development Commands

### Testing

```bash
just test                   # Default test suite
just test-cov               # Tests with coverage
just test-all               # Tests with code quality
```

### Code Quality

```bash
just lint                   # Lint and format code
just format                 # Code formatting only
just typecheck              # Type checking codebase
just cf-lint api            # CloudFormation linting
just cf-validate worker     # CloudFormation AWS validation
```

### Log Monitoring

```bash
just logs worker 200                 # View worker logs
just logs-grep api "pipeline" 500    # Search API logs
just logs-follow worker              # CloudWatch log search
```

### Prerequisites

#### System Requirements

- Docker & Docker Compose
- 8GB RAM minimum
- 20GB free disk space

#### Required Tools

- `uv` for Python package and version management
- `rust-just` for project command runner (installed via uv)

#### Deployment Requirements

- Fork this repo
- GHA secrets & variables initialized
- AWS account with credentials & secrets initialized

## Architecture

### Application Layer

- **FastAPI Backend** (`main.py`) with async REST API and versioned endpoints (`/v1/`)
- **Multi-Database Routing**: Database-scoped endpoints (`/v1/graphs/{graph_id}/`) for multi-tenant operations
- **MCP Server**: Model Context Protocol Server for AI-powered financial analytics
- **Celery Workers** with priority queues for asynchronous processing

### Graph Database System

RoboSystems supports **pluggable graph database backends** to provide flexibility and choice for different deployment scenarios:

#### Supported Backends

- **Kuzu** (Default): High-performance embedded graph database, ideal for Standard tier deployments
- **Neo4j**: Client-server architecture for Enterprise/Premium tiers with advanced features

#### Graph API System (`/robosystems/graph_api/`)

The **Graph API** is a FastAPI microservice that provides a unified interface regardless of backend:

- **Backend Abstraction**: Consistent API whether using Kuzu or Neo4j
- **HTTP REST Interface**: High-performance API for all graph operations (port 8001 for Kuzu, 8002 for Neo4j)
- **Multi-Database Management**: Handles multiple databases per instance (backend-dependent)
- **Connection Pooling**: Efficient resource management with backend-optimized pooling
- **Async Ingestion**: Queue-based data loading with S3 integration
- **Streaming Support**: NDJSON streaming for large query results
- **Admission Control**: CPU/memory-based backpressure to prevent overload

#### Infrastructure Design

- **Cluster-Based Infrastructure**: Tiered instances (Standard/Enterprise/Premium) for different workload requirements
- **Multi-Tenant Isolation**: Each entity gets a dedicated database (`kg12345abc`) with complete data isolation
- **Shared Repositories**: Common databases for SEC filings, industry benchmarks, and economic indicators
- **API-First Design**: All database access through REST APIs with no direct database connections
- **Schema-Driven Operations**: All graph operations derive from curated schemas (RoboLedger, RoboInvestor, and more)

#### Backend Factory System (`/robosystems/graph_api/backends/`)

The backend factory provides pluggable graph database backends with a unified interface:

- **Backend Abstraction**: Common interface for Kuzu and Neo4j implementations
- **Factory Pattern**: Dynamic backend selection based on configuration (`GRAPH_BACKEND_TYPE`)
- **Backend-Specific Optimizations**: Each backend implements optimal patterns for their architecture
- **Connection Management**: Backend-appropriate connection pooling and lifecycle management
- **Query Translation**: Cypher query execution with backend-specific optimizations
- **Ingestion Strategies**: Optimized bulk loading for each backend type

#### Client Factory System (`/robosystems/graph_api/client/`)

The client factory layer provides intelligent routing between application code and graph database infrastructure:

- **Backend-Agnostic**: Works seamlessly with both Kuzu and Neo4j backends
- **Automatic Discovery**: Finds database instances via DynamoDB registry (Kuzu) or direct connection (Neo4j)
- **Redis Caching**: Caches instance locations to reduce lookups
- **Circuit Breakers**: Prevents cascading failures with automatic recovery
- **Connection Reuse**: HTTP/2 connection pooling for efficiency
- **Retry Logic**: Exponential backoff with jitter for transient errors

### Key Components

#### Middleware Layer (`/robosystems/middleware/graph/`)

- **GraphRouter**: Intelligent cluster selection and routing logic
- **AllocationManager**: DynamoDB-based database allocation across writer instances
- **MultiTenantUtils**: Database name resolution, validation, and routing utilities
- **QueryQueue**: Advanced query queue with admission control and long polling

#### Operations Layer (`/robosystems/operations/`)

- **Entity Graph Service**: Entity-specific graph creation workflows
- **Generic Graph Service**: Generic graph creation with custom schemas
- **Data Ingestion**: High-performance bulk data loading using COPY operations
- **Credit Service**: AI agent usage with token-based consumption

#### Data Processors (`/robosystems/processors/`)

- **XBRL Graph Processor**: XBRL filing processing and graph transformation
- **QB Transactions Processor**: QuickBooks data processing and normalization
- **Trial Balance Processor**: Financial calculations and accounting operations
- **Schedule Processor**: Financial schedule generation

#### Database Models (`/robosystems/models/`)

**IAM Models** (`/robosystems/models/iam/`):

- SQLAlchemy models for PostgreSQL database interactions
- Foundation for Alembic migrations and schema management
- Multi-tenant architecture with user management and access control
- Credit system and usage analytics models

**API Models** (`/robosystems/models/api/`):

- Centralized Pydantic models for API request/response validation
- OpenAPI documentation generation and type safety
- Consistent structure across all API endpoints

### Data Layer

- **Graph Database**: Pluggable backend (Kuzu or Neo4j) for financial knowledge graphs with cluster-based scaling
- **DynamoDB**: Database allocation registry, instance and volume management
- **PostgreSQL**: Primary relational database for identity and access management
- **Valkey**: Message broker and caching (separate DBs for queues, cache, progress tracking)
- **AWS S3**: Document storage and database synchronization

### Infrastructure

- **VPC**: AWS VPC with NAT Gateway, CloudTrail, and VPC Flow Logs
- **API**: ECS Fargate ARM64/Graviton with auto-scaling and WAF
- **Workers**: ECS Fargate ARM64/Graviton with auto-scaling
- **Graph Database Writers**: EC2 Graviton instances with DynamoDB registry and management lambdas
- **Database & Cache**: RDS PostgreSQL + ElastiCache Valkey instances
- **Observability**: Amazon Managed Prometheus + Grafana with AWS SSO
- **Self-Hosted CI/CD**: GitHub Actions runner on dedicated infrastructure

### External Integrations

- **SEC EDGAR**: XBRL filing processing with staging parallel processing
- **QuickBooks API**: OAuth-based accounting synchronization
- **Anthropic Claude**: AI analysis agents system powered by Claude

## AI

### Model Context Protocol (MCP)

- **Financial Analysis**: Natural language queries across entity and benchmark data
- **Cross-Database Queries**: Compare entity data against SEC public data
- **Credit Tracking**: AI operations consume credits based on actual token usage
- **Handler Pool**: Managed MCP handler instances with resource limits

### Agent System Capabilities

- **Multi-Agent Architecture**: Intelligent routing to specialized agents based on query context
- **Dynamic Agent Selection**: Automatic selection of the most appropriate agent for each task
- **Parallel Query Processing**: Batch processing of multiple queries simultaneously
- **Context-Aware Responses**: GraphRAG-enabled agents with native graph database integration
- **Extensible Framework**: Support for custom agents with specific domain expertise

### Credit System

- **AI-Focused**: Credits consumed only by AI operations (Anthropic/OpenAI API calls)
- **Token-Based Billing**: Actual token usage determines credit consumption

## CI/CD

### GitHub Actions Workflows

#### Primary Deployment Workflows

- **`prod.yml`**: Production deployment orchestrator

  - Triggered on release tags (e.g., `v1.0.0`)
  - Full stack deployment with health checks and rollback capability
  - Run database migrations separately with dedicated workflow

- **`staging.yml`**: Staging environment deployment

  - Triggered on manual dispatch on branches or release tags
  - Used for integration testing before production releases
  - Identical infrastructure to production at reduced scale

- **`bootstrap.yml`**: One-time infrastructure initialization

  - Creates base AWS resources (VPC, networking)
  - Sets up GitHub Actions runner infrastructure
  - Run once per AWS account setup

- **`run-migrations.yml`**: Database migrations on RDS instances
  - Connect to bastion host via AWS SSM
  - Run migrations script with deployed environment container

#### Infrastructure Features

- **Self-Hosted Runner**: Dedicated ARM64 EC2 spot instance for CI/CD

  - Cost-optimized CI/CD - up to 90% savings compared to GitHub Hosted
  - Pre-configured with Python, NodeJS, AWS CLI, Docker, and build tools

- **Multi-Architecture Builds**:

  - Native ARM64 builds for Graviton instances
  - AMD64 compatibility for development environments
  - Parallel builds with layer caching

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
- **`beat.yaml`**: Celery beat scheduler for periodic tasks and cron jobs
- **`worker-monitor.yaml`**: Lambda function for monitoring worker health and queue depths

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

### Infrastructure Setup

```bash
just setup-gha     # Interactive GitHub secrets/variables setup
just setup-aws     # Interactive AWS secrets manager setup
just bootstrap     # Bootstrap infrastructure
```

## Client Libraries

RoboSystems provides comprehensive client libraries for building applications on top of the platform:

### MCP (Model Context Protocol) Client

AI integration client for connecting Claude and other LLMs to RoboSystems.

```bash
npx -y @robosystems/mcp
```

- **Features**: Claude Desktop integration, natural language queries, graph traversal tools, financial analysis
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

## Component Documentation

Each major system component has detailed documentation:

### Core Services

- **`/robosystems/operations/README.md`**: Business workflow orchestration and service layer patterns
- **`/robosystems/processors/README.md`**: Data transformation and processing components
- **`/robosystems/schemas/README.md`**: Graph schema definitions and management
- **`/robosystems/models/iam/README.md`**: Database models, SQLAlchemy integration, and Alembic migrations
- **`/robosystems/models/api/README.md`**: Centralized Pydantic models for API validation
- **`/robosystems/config/README.md`**: Configuration management and environment handling

### Graph Database System

- **`/robosystems/graph_api/README.md`**: Complete Graph API documentation (supports Kuzu and Neo4j backends)
- **`/robosystems/graph_api/backends/README.md`**: Backend abstraction layer and implementation details
- **`/robosystems/graph_api/client/README.md`**: Client factory system for intelligent routing

### Middleware Components

- **`/robosystems/middleware/auth/README.md`**: Authentication and authorization
- **`/robosystems/middleware/credits/README.md`**: AI token-based credit system
- **`/robosystems/middleware/graph/README.md`**: Graph database middleware and routing layer
- **`/robosystems/middleware/mcp/README.md`**: MCP tools, client factory, pooling
- **`/robosystems/middleware/otel/README.md`**: OpenTelemetry observability
- **`/robosystems/middleware/robustness/README.md`**: Circuit breakers and retry policies

### Security & Compliance

- **`/SECURITY.md`**: Security features
- **`/COMPLIANCE.md`**: SOC 2 compliance features

### Development & Testing

- **`/CLAUDE.md`**: Curated Claude memory to improve usefulness
- **`/tests/README.md`**: Testing framework and test organization

## API Reference

- [API reference](https://api.robosystems.ai)
- [API documentation](https://api.robosystems.ai/docs)
- [OpenAPI specification](https://api.robosystems.ai/openapi.json)

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Discussions](https://github.com/RoboFinSystems/robosystems/discussions)
- [Projects](https://github.com/RoboFinSystems/robosystems/projects)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

MIT © 2025 RFS LLC
