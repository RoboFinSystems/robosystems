# CLAUDE.md - RoboSystems Development Guide

## Critical Rules

**All Python commands MUST use `uv run`:**

```bash
uv run python script.py    # NOT: python script.py
uv run pytest              # NOT: pytest
uv run ruff check          # NOT: ruff check
```

**Always use Docker profile `robosystems`** - never individual service profiles:

```bash
just start                 # Uses robosystems profile by default
just start robosystems     # Explicit form (same result)
                           # NOT: just start api
```

**Never use `os.getenv()` directly** - use centralized config:

```python
from robosystems.config import env
database_url = env.DATABASE_URL  # NOT: os.getenv("DATABASE_URL")
```

**Never create migrations manually** - always autogenerate:

```bash
just migrate-create "description"  # NOT: manual alembic revision
```

## Quick Reference

### Daily Development

```bash
just start                 # Start full Docker stack
just restart               # Quick restart (Python code changes only)
just rebuild               # Full rebuild (dependency/Dockerfile changes)
just test                  # Run tests (excludes slow/integration)
just logs container=api              # View API logs
just logs container=dagster-daemon   # View Dagster daemon logs
```

### Code Quality

```bash
just lint fix              # Fix linting issues
just format                # Format code
just typecheck             # Type checking
just test-all              # Full suite with all checks
```

### Database Operations

```bash
just migrate-create "msg"  # Create migration (autogenerate)
just migrate-up            # Apply migrations
just migrate-down          # Rollback one migration
just migrate-current       # Show current revision
```

### Graph Database

```bash
just graph-health                              # Health check
just graph-query GRAPH_ID "CYPHER_QUERY"       # Execute query
just graph-info GRAPH_ID                       # Database info
just lbug-query GRAPH_ID "CYPHER_QUERY"        # Direct LadybugDB query (bypass API)
```

### SEC Data (Local Development)

```bash
just sec-load NVDA 2025    # Load company filings
just sec-health            # SEC database health
just sec-reset             # Reset SEC database
```

### Demo Scripts

```bash
just demo-user             # Create/reuse demo user credentials
just demo-accounting       # Run accounting demo
just demo-custom-graph     # Run custom graph demo
just demo-sec NVDA 2025    # Run SEC demo
```

## Code Standards

- **Python 3.13** with uv package management
- **Ruff** formatting (88-char lines, double quotes)
- **basedpyright** for type checking
- **Self-documenting code**: Prefer clear names over comments; add comments only for non-obvious logic
- **Emojis**: Only in interactive scripts (`/examples/`), never in production code or logs

## Common Patterns

### API Endpoint Pattern

```python
from robosystems.middleware.auth import get_current_user
from robosystems.models.api import ResponseModel

@router.get("/endpoint")
async def endpoint(
    request: Request,
    user: User = Depends(get_current_user)
) -> ResponseModel:
    pass
```

### Service Layer Pattern

```python
from robosystems.operations.graph import CreditService, EntityGraphService

# Business logic in operations, not routers
credit_service = CreditService(user_id, graph_id)
if await credit_service.has_sufficient_credits("operation"):
    result = await entity_service.execute(...)
    await credit_service.consume_credits("operation")
```

### Database Migrations

1. Update SQLAlchemy models in `/robosystems/models/iam/`
2. Generate migration: `just migrate-create "description"`
3. Review and run: `just migrate-up`

## Architecture Overview

```
robosystems/
├── routers/           # API endpoints (thin layer, calls operations)
├── operations/        # Business logic orchestration
│   ├── graph/         # Graph services (credit, entity, subscription)
│   ├── lbug/          # LadybugDB operations (backup, ingest)
│   ├── agents/        # AI agent operations
│   ├── providers/     # Provider registry and implementations
│   └── views/         # Data view operations
├── middleware/        # Cross-cutting concerns
│   ├── auth/          # Authentication (JWT, API keys, SSO)
│   ├── billing/       # Credit consumption tracking
│   ├── graph/         # Graph routing and multi-tenancy
│   ├── rate_limits/   # Burst protection
│   ├── sse/           # Server-Sent Events
│   └── ...            # mcp, otel, robustness
├── dagster/           # Dagster orchestration (jobs, sensors, assets, resources)
├── adapters/          # External service integrations (SEC, QuickBooks)
├── admin/             # Admin CLI and utilities
├── security/          # Security controls (audit, auth protection, encryption)
├── models/
│   ├── api/           # Pydantic request/response models
│   ├── billing/       # Billing models
│   └── iam/           # SQLAlchemy database models
├── config/            # Centralized configuration (see config/README.md)
├── schemas/           # Graph schema definitions
└── graph_api/         # Graph API microservice
```

### Key Architectural Patterns

1. **Operations orchestrate, adapters integrate**: Operations coordinate business logic; adapters handle external service integration and data transformation
2. **Multi-tenant by design**: All graph operations are scoped to `graph_id`
3. **Credit-based AI billing**: Only AI operations (Anthropic/OpenAI) consume credits; database operations are free
4. **Pluggable graph backend**: LadybugDB (default) or Neo4j via `GRAPH_BACKEND_TYPE`

## Testing

### Test Commands

```bash
just test                  # Unit tests (fast, no external deps)
just test routers          # Run tests at /tests/routers
just test-cov              # Coverage report
just test-all              # Full suite with all checks
```

### Bash Timeouts

Always use `timeout: 600000` (10 minutes) on Bash tool calls for `just test-all`, `just test`, and other test commands. The default 2-minute Bash timeout is too short for the full suite. CI has a 10-minute limit for the test step.

### Test Markers

```python
@pytest.mark.unit          # Fast, isolated
@pytest.mark.integration   # May use databases
@pytest.mark.slow          # Long-running
@pytest.mark.security      # Security-focused
```

### Long-Running Tests

For tests exceeding the default pytest timeout, use the `@pytest.mark.timeout` decorator:

```python
@pytest.mark.timeout(300)  # 5 minutes
@pytest.mark.slow
def test_long_running_operation():
    pass
```

Or configure in `pytest.ini` for specific test paths.

## Environment Configuration

### Dual .env Pattern

- **`.env`**: Container hostnames for Docker services (e.g., `postgres:5432`)
  - Used by: Docker Compose, containers communicating with each other
- **`.env.local`**: Localhost URLs for host commands (e.g., `localhost:5432`)
  - Used by: Justfile recipes, local scripts, migrations run on host

Both are auto-created from `.example` templates by `just start` or `just init`.

**When to edit which:**
- Adding secrets/credentials → Update both files
- Changing service ports → Update both files
- Local overrides only → Update `.env.local` only

### Key Environment Variables

```bash
# Core
ENVIRONMENT=dev|staging|prod
DATABASE_URL=postgresql://...
VALKEY_URL=redis://...

# Graph API
GRAPH_API_URL=http://localhost:8001
GRAPH_BACKEND_TYPE=ladybug|neo4j_community
LBUG_DATABASE_PATH=/data/lbug-dbs

# Feature Flags
RATE_LIMIT_ENABLED=true
BILLING_ENABLED=true
```

## Configuration System

All configuration is centralized in `/robosystems/config/`. See `config/README.md` for full details.

| Module                  | Purpose                                        |
| ----------------------- | ---------------------------------------------- |
| `env.py`                | Environment variables with validation          |
| `shared_repositories.py`| Shared repository registry and manifests       |
| `billing/`              | Subscription plans and pricing                 |
| `graph_tier.py`         | Graph tier config from `.github/configs/graph.yml` |
| `rate_limits.py`        | Burst-focused rate limiting (1-minute windows) |
| `credits.py`            | AI operation credit costs                      |
| `agents.py`             | Claude model configuration (Bedrock)           |
| `validation.py`         | Startup configuration checks                   |
| `valkey_registry.py`    | Valkey database allocation (never hardcode DB numbers) |
| `storage/`              | S3 path helpers (shared data, graph storage)   |

## Graph API

### Backends

- **LadybugDB** (default): Embedded columnar graph database
- **Neo4j Community**: Client-server with Bolt protocol

### Key Endpoints

```http
POST /databases                           # Create database
POST /databases/{graph_id}/query          # Execute Cypher
POST /databases/{graph_id}/tables         # Create staging table
POST /databases/{graph_id}/tables/query   # Query staging (SQL)
POST /databases/{graph_id}/tables/{name}/materialize  # Materialize to graph
GET  /health                              # Health check
```

### LadybugDB Limitations

- Sequential ingestion (one file at a time per database)
- Connection pool size configurable (default 3, production config 10)
- Single writer per database at a time

### Subgraphs

- **Tiers**: ladybug-standard (3 max), ladybug-large (10 max), ladybug-xlarge (25 max)
- **Naming**: Alphanumeric only, 1-20 chars (no hyphens/underscores)
- **ID Format**: `{parent_graph_id}_{subgraph_name}` (e.g., `kg123_dev`)
- **Features**: Shared credit pool, shared permissions, isolated data

## Troubleshooting

### Docker Issues

```bash
just restart               # Code changes not picked up
just rebuild               # Dependency changes not working
just logs container=api              # Check API logs
just logs-grep container=worker pattern=ERROR  # Search worker logs
```

### Database Issues

```bash
docker ps | grep postgres  # Check PostgreSQL running
just migrate-current       # Verify migration status
just migrate-up            # Apply pending migrations
```

### Graph Database Issues

```bash
just graph-health          # Check Graph API
just graph-info GRAPH_ID   # Database info
```

### Cache Issues

```bash
just admin dev cache info               # View all cache databases
just admin dev cache info auth          # View specific database
just admin dev cache keys auth --pattern "apikey:*"  # List matching keys
just admin dev cache flush auth         # Flush single database
```

## Admin CLI

```bash
just admin dev --help                    # List all command groups
just admin dev stats                     # Subscription & revenue stats
just admin dev subscriptions list        # List all subscriptions
just admin dev invoices list             # List invoices
just admin dev cache info                # View cache databases
```

Command groups: `subscriptions`, `invoices`, `credits`, `graphs`, `users`, `orgs`, `cache`, `instances`, `migrations`. Use `--help` on any group for options.

## CI/CD

- **GitHub-hosted runners** (default): Used for tests, builds, and deployments
- **Self-hosted runners** (optional): Set `RUNNER_LABELS` repo variable to use self-hosted runners
- **Deployments**: Manual workflow dispatch via `staging.yml` and `prod.yml`
- **Infrastructure Config**: `.github/configs/graph.yml`

## AWS Infrastructure

| Component   | Service           | Notes                          |
| ----------- | ----------------- | ------------------------------ |
| API/Workers | ECS Fargate ARM64 | Auto-scaling, Spot-preferred   |
| PostgreSQL  | RDS               | Burstable T4g instance         |
| LadybugDB   | EC2 ARM64         | R7g instances (tier-dependent) |
| Cache       | ElastiCache       | Valkey-compatible              |

Instance sizes and capacity are managed via GitHub Actions variables (`just gha-list`) and vary between staging and production.

## GitHub Actions Variables

```bash
just gha-list                          # List all variables
just gha-list SHARED_REPLICAS          # Filter by pattern (case-insensitive)
just gha-get SHARED_REPLICAS_INSTANCE_WARMUP_PROD  # Get single variable
just gha-set SHARED_REPLICAS_INSTANCE_WARMUP_PROD 900  # Set variable
just gha-delete SOME_OLD_VARIABLE      # Delete variable
```

Variables follow the naming convention `COMPONENT_SETTING_ENVIRONMENT` (e.g., `SHARED_REPLICAS_INSTANCE_WARMUP_PROD`). Run `just setup-gha` to initialize all variables with defaults.

## SSM Parameters

```bash
just ssm-list prod features            # List feature flags
just ssm-list prod tuning              # List tuning parameters
just ssm-get prod features/mcp-memory  # Get single parameter
just ssm-set prod features/mcp-memory true   # Set parameter
just ssm-delete prod features/old-flag       # Delete parameter
```

Parameters are stored at `/robosystems/{env}/{category}/{name}` in SSM Parameter Store. Unlike GitHub variables (which require redeployment), SSM parameters take effect immediately at runtime.

## Secret Management

- **AWS Secrets Manager Base**: `robosystems/{staging|prod}`
- **Components**: `robosystems/{staging|prod}/{postgres|valkey|admin|graph-api}`
- Monthly rotation via GitHub Actions (`secrets-rotation.yml`) using Lambda functions
- Never commit secrets to code

## Key READMEs

Before working in a directory, read its README:

- `/robosystems/config/README.md` - Configuration patterns
- `/robosystems/config/storage/README.md` - S3 storage paths
- `/robosystems/graph_api/README.md` - Graph API details
- `/robosystems/middleware/auth/README.md` - Authentication system
- `/robosystems/middleware/graph/README.md` - Graph routing
- `/robosystems/operations/README.md` - Business logic patterns
- `/robosystems/dagster/README.md` - Dagster orchestration patterns
- `/robosystems/models/api/README.md` - API models
- `/robosystems/models/iam/README.md` - Database models
- `/tests/README.md` - Testing guide
- `/examples/README.md` - Demo scripts
