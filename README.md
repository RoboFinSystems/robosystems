# RoboSystems

RoboSystems is an enterprise-grade financial knowledge graph platform that transforms complex financial data into actionable intelligence through graph-based analytics and AI-powered insights.

- **Graph-Based Financial Intelligence**: Leverages Kuzu graph database technology to model complex financial relationships, enabling deep analysis of relationships between accounting, financial reporting, investment management, and public XBRL data
- **Multi-Source Data Integration**: Seamlessly integrates QuickBooks accounting data, SEC XBRL filings (10-K, 10-Q), and custom financial datasets into a unified knowledge graph
- **AI-Powered Analysis**: Natural language financial analysis through Claude AI integration via Model Context Protocol (MCP), enabling sophisticated queries and insights
- **Enterprise-Ready Infrastructure**: Multi-tenant architecture with tiered scaling (Standard/Enterprise/Premium), production-grade query management, and credit-based billing for sustainable operations
- **Developer-First API**: RESTful API with comprehensive endpoints for graph operations, data ingestion, and analysis - designed for integration with financial applications like RoboLedger and RoboInvestor

## Core Features

RoboSystems bridges the gap between raw financial data and actionable business intelligence by creating interconnected knowledge graphs that reveal hidden relationships, patterns, and insights that traditional databases miss. It's the backbone for next-generation financial applications that need to understand not just numbers, but the relationships and context behind them.

- **Multi-Tenant Graph Databases**: Create isolated Kuzu database instances with cluster-based scaling
- **AI Agent Interface**: Natural language financial analysis through Claude AI via Model Context Protocol (MCP)
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

- Kuzu graph database
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
- **MCP Integration**: Model Context Protocol for AI-powered financial analytics
- **Celery Workers** with priority queues for asynchronous processing

### Kuzu Graph Database System

**Kuzu** is a high-performance embedded graph database that powers RoboSystems' financial knowledge graph platform. This system provides multi-tenant graph databases with enterprise-grade scaling and reliability.

- **Cluster-Based Infrastructure**: Tiered instances (Standard/Enterprise/Premium) for different workload requirements
- **Multi-Tenant Isolation**: Each entity gets a dedicated database (`kg12345abc`) with complete data isolation
- **Shared Repositories**: Common databases for SEC filings, industry benchmarks, and economic indicators
- **API-First Design**: All database access through REST APIs with no direct database connections
- **Schema-Driven Operations**: All graph operations derive from curated schemas (RoboLedger, RoboInvestor, and more)

#### Kuzu API System (`/robosystems/kuzu_api/`)

The **Kuzu API** is a FastAPI microservice that runs alongside Kuzu databases on instances, providing:

- **HTTP REST Interface**: High-performance API for all graph operations (port 8001)
- **Multi-Database Management**: Handles up to 10 databases per instance (Standard tier)
- **Connection Pooling**: Efficient resource management with max 3 connections per database
- **Async Ingestion**: Queue-based data loading with S3 integration
- **Streaming Support**: NDJSON streaming for large query results
- **Admission Control**: CPU/memory-based backpressure to prevent overload

#### Client-Factory System (`/robosystems/kuzu_api/client/`)

The client-factory layer provides intelligent routing between application code and Kuzu infrastructure:

- **Automatic Discovery**: Finds database instances via DynamoDB registry
- **Redis Caching**: Caches instance locations to reduce lookups
- **Circuit Breakers**: Prevents cascading failures with automatic recovery
- **Connection Reuse**: HTTP/2 connection pooling for efficiency
- **Retry Logic**: Exponential backoff with jitter for transient errors

#### Infrastructure Tiers

| Tier           | Instance Type | Databases/Instance | Memory/Graph | Use Case                |
| -------------- | ------------- | ------------------ | ------------ | ----------------------- |
| **Standard**   | r7g.xlarge    | 10                 | 2GB          | Shared resources        |
| **Enterprise** | r7g.large     | 1                  | 14GB         | Isolated resources      |
| **Premium**    | r7g.xlarge    | 1                  | 28GB         | Maximum performance     |
| **Shared**     | r7g.xlarge    | N/A                | Pooled       | SEC/public repositories |

### Key Components

#### Middleware Layer (`/robosystems/middleware/graph/`)

- **GraphRouter**: Intelligent cluster selection and routing logic
- **AllocationManager**: DynamoDB-based database allocation across writer instances
- **MultiTenantUtils**: Database name resolution, validation, and routing utilities
- **QueryQueue**: Advanced query queue with admission control and long polling

#### Operations Layer (`/robosystems/operations/`)

- **EntityGraphService**: Entity-specific graph creation workflows
- **GenericGraphService**: Generic graph creation with custom schemas
- **Data Ingestion**: High-performance bulk data loading using COPY operations
- **CreditService**: AI agent usage with token-based consumption

#### Data Processors (`/robosystems/processors/`)

- **XBRLGraphProcessor**: XBRL filing processing and graph transformation
- **QBTransactionsProcessor**: QuickBooks data processing and normalization
- **TrialBalanceProcessor**: Financial calculations and accounting operations
- **ScheduleProcessor**: Financial schedule generation
- **SchemaProcessor**: DataFrame schema compatibility validation

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

- **Kuzu Graph Database**: Financial knowledge graph with cluster-based scaling
- **DynamoDB**: Kuzu database allocation registry, instance and volume management
- **PostgreSQL**: Primary relational database for identity and access management
- **Valkey**: Message broker and caching (separate DBs for queues, cache, progress tracking)
- **AWS S3**: Document storage and database synchronization

### Infrastructure

- **VPC**: AWS VPC with NAT Gateway, CloudTrail, and VPC Flow Logs
- **API**: ECS Fargate ARM64/Graviton with auto-scaling and WAF
- **Workers**: ECS Fargate ARM64/Graviton with auto-scaling
- **Kuzu Writers**: EC2 Graviton instances with DynamoDB registry and management lambdas
- **Kuzu Readers**: EC2 Graviton instances with load balancing for shared repositories
- **Database & Cache**: AWS RDS PostgreSQL + AWS ElastiCache Valkey instances
- **Observability**: Amazon Managed Prometheus + Grafana with AWS SSO
- **Self-Hosted CI/CD**: GitHub Actions runner on dedicated infrastructure

### External Integrations

- **SEC EDGAR**: XBRL filing processing with staging parallel processing
- **QuickBooks API**: OAuth-based accounting synchronization
- **Anthropic Claude**: AI analysis via Model Context Protocol

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
- **Context-Aware Responses**: GraphRAG-enabled agents with native kuzu graph database integration
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
  - Runs database migrations automatically

- **`staging.yml`**: Staging environment deployment

  - Triggered on manual dispatch on branches or release tags
  - Used for integration testing before production releases
  - Identical infrastructure to production at reduced scale

- **`bootstrap.yml`**: One-time infrastructure initialization
  - Creates base AWS resources (VPC, networking)
  - Sets up GitHub Actions runner infrastructure
  - Configures secrets and IAM roles
  - Run once per AWS account setup

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
- **`valkey.yaml`**: ElastiCache Valkey (Redis fork) for caching and message broker

#### API & Workers

- **`api.yaml`**: ECS Fargate API service with auto-scaling, load balancing, and health checks
- **`waf.yaml`**: AWS Web Application Firewall for protecting the API from web exploits
- **`worker.yaml`**: ECS Fargate Celery workers with priority queues and spot capacity
- **`beat.yaml`**: Celery beat scheduler for periodic tasks and cron jobs
- **`worker-monitor.yaml`**: Lambda function for monitoring worker health and queue depths

#### Kuzu Graph Database

- **`kuzu-infra.yaml`**: Base infrastructure for Kuzu clusters (security groups, roles, registries)
- **`kuzu-volumes.yaml`**: EBS volume management and snapshot automation
- **`kuzu-writers.yaml`**: Auto-scaling EC2 writer clusters with tiered instance types
- **`kuzu-shared-replicas.yaml`**: ECS Fargate read replicas for shared repositories (SEC)

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
just setup-gha                     # Interactive GitHub secrets/variables setup
just setup-aws                     # Interactive AWS secrets manager setup
gh workflow run bootstrap.yml      # Bootstrap infrastructure
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

### Kuzu Graph Database System

- **`/robosystems/kuzu_api/README.md`**: Complete Kuzu API documentation
- **`/robosystems/kuzu_api/client/README.md`**: Client-factory system for intelligent routing

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

### Testing & Development

- **`/tests/README.md`**: Testing framework and test organization
- **`/.github/CONTRIBUTING.md`**: Open source contribution guide

## API Reference

- API reference: [https://api.robosystems.ai](https://api.robosystems.ai)
- API documentation: [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs)
- OpenAPI specification: [https://api.robosystems.ai/openapi.json](https://api.robosystems.ai/openapi.json)

## Support

- Issues: [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- Discussions: [Discussions](https://github.com/RoboFinSystems/robosystems/discussions)
- Projects: [Projects](https://github.com/RoboFinSystems/robosystems/projects)
- Wiki: [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

MIT © 2025 RFS LLC
