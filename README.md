# RoboSystems

RoboSystems is an enterprise-grade financial knowledge graph platform that transforms complex financial data into actionable intelligence through graph-based analytics and AI-powered insights.

- **Graph-Based Financial Intelligence**: Leverages graph database technology to model complex financial relationships
- **GraphRAG Architecture**: Knowledge graph-based retrieval-augmented generation for LLM-powered financial analysis
- **Model Context Protocol (MCP)**: Standardized server and [client](https://www.npmjs.com/package/@robosystems/mcp) for LLM integration
- **Multi-Source Data Integration**: SEC XBRL filings, QuickBooks accounting data, and custom financial datasets
- **Enterprise-Ready Infrastructure**: Multi-tenant architecture with tiered scaling and production-grade query management
- **Developer-First API**: RESTful API designed for integration with financial applications

## Core Features

- **Multi-Tenant Graph Databases**: Isolated graph databases with tiered cluster-based scaling
- **AI Agent Interface**: Natural language financial analysis via Model Context Protocol (MCP)
- **Entity & Generic Graphs**: Curated schemas for RoboLedger/RoboInvestor, plus custom schema support
- **Shared Repositories**: SEC XBRL filings knowledge graph for context mining
- **QuickBooks Integration**: Complete accounting synchronization with trial balance creation
- **DuckDB Staging System**: High-performance data validation and bulk ingestion pipeline
- **Credit-Based Billing**: AI operations consume credits based on token usage
- **Query Queue System**: Production-ready query management with admission control

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

### Local Development

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
just logs-follow worker              # Tail worker logs
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

RoboSystems is built on a modern, scalable architecture with:

**Application Layer:**

- FastAPI REST API with versioned endpoints (`/v1/`)
- MCP Server for AI-powered financial analytics
- Celery workers with priority queues

**Graph Database System:**

- Pluggable backends (Kuzu by default, Neo4j optional)
- Multi-tenant isolation with dedicated databases per entity
- DuckDB staging system for high-performance data ingestion
- Tiered infrastructure from multi-tenant to dedicated instances

**Data Layer:**

- PostgreSQL for IAM and graph metadata
- Valkey for caching and message broker
- AWS S3 for data lake storage and static assets
- DynamoDB for instance/graph/volume registry

**Infrastructure:**

- ECS Fargate for API/Workers (ARM64/Graviton)
- EC2 auto-scaling groups for graph database writers
- RDS PostgreSQL + ElastiCache Valkey
- CloudFormation-managed infrastructure

**For detailed architecture documentation, see the [Architecture Overview](https://github.com/RoboFinSystems/robosystems/wiki/Architecture-Overview) in the Wiki.**

## AI

### Model Context Protocol (MCP)

- **Financial Analysis**: Natural language queries across entity and benchmark data
- **Cross-Database Queries**: Compare entity data against SEC public data
- **Credit Tracking**: AI operations consume credits based on actual token usage
- **Handler Pool**: Managed MCP handler instances with resource limits

### Agent System

- Multi-agent architecture with intelligent routing
- Dynamic agent selection based on query context
- Parallel query processing with GraphRAG-enabled responses
- Extensible framework for custom domain expertise

### Credit System

- **AI-Focused**: Credits consumed only by AI operations (Anthropic/OpenAI API calls)
- **Token-Based Billing**: Actual token usage determines credit consumption

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
- **[Architecture Overview](https://github.com/RoboFinSystems/robosystems/wiki/Architecture-Overview)** - System design and components
- **[SEC XBRL Pipeline](https://github.com/RoboFinSystems/robosystems/wiki/SEC-XBRL-Pipeline)** - Working with SEC financial data
- **[Accounting Demo](https://github.com/RoboFinSystems/robosystems/wiki/Accounting-Demo)** - Complete guide to graph-based accounting workflows

### Developer Documentation (Codebase)

**Core Services:**

- **[Operations](/robosystems/operations/README.md)** - Business workflow orchestration
- **[Processors](/robosystems/processors/README.md)** - Data transformation pipeline
- **[Schemas](/robosystems/schemas/README.md)** - Graph schema definitions
- **[IAM Models](/robosystems/models/iam/README.md)** - Database models and migrations
- **[API Models](/robosystems/models/api/README.md)** - API request/response models
- **[Configuration](/robosystems/config/README.md)** - Configuration management

**Graph Database System:**

- **[Graph API](/robosystems/graph_api/README.md)** - Graph API overview
- **[Backends](/robosystems/graph_api/backends/README.md)** - Backend abstraction layer
- **[Client Factory](/robosystems/graph_api/client/README.md)** - Client factory system
- **[Core Services](/robosystems/graph_api/core/README.md)** - Core services layer

**Middleware Components:**

- **[Authentication](/robosystems/middleware/auth/README.md)** - Authentication and authorization
- **[Credits](/robosystems/middleware/credits/README.md)** - AI credit system
- **[Graph Routing](/robosystems/middleware/graph/README.md)** - Graph routing layer
- **[MCP](/robosystems/middleware/mcp/README.md)** - MCP tools and pooling
- **[Observability](/robosystems/middleware/otel/README.md)** - OpenTelemetry observability
- **[Robustness](/robosystems/middleware/robustness/README.md)** - Circuit breakers and retry policies

**Security & Testing:**

- **[Security](/SECURITY.md)** - Security features
- **[Compliance](/COMPLIANCE.md)** - SOC 2 compliance
- **[Development Guide](/CLAUDE.md)** - Development assistant memory
- **[Testing](/tests/README.md)** - Testing framework

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
