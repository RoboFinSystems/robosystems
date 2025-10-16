# CLAUDE.md

## Quick Start

**CRITICAL**: All Python scripts and packages must be run through `uv run` to use the proper virtual environment context.

**NEVER run bare `python`, `pip`, `pytest`, or any Python tools directly** - they must be scoped to the uv virtual environment:

```bash
# CORRECT
uv run python script.py
uv run pytest
just cf-lint template
uv run ruff check

# WRONG - DO NOT USE
python script.py
pytest
cfn-lint template.yaml
ruff check
```

### Essential Commands

```bash
# Quick start - full Docker setup
just start

# Development workflow
just venv && just install          # Setup environment
just compose-up robosystems        # Start services (use 'robosystems' not 'api' or 'worker')
just restart                       # IMPORTANT: Run after ANY code changes
just rebuild                       # For larger changes (packages, env vars)

# Testing
just test                          # Run tests
just test-all                      # Run all tests with linting
just lint && just format          # Code quality

# For long-running tests that may exceed default timeout:
# Use the timeout parameter with Bash tool: timeout=300000 (5 minutes) or timeout=600000 (10 minutes)
```

## Development Environment

### Docker Infrastructure

**IMPORTANT**: Always use the `robosystems` Docker profile for development, not individual `api` or `worker` profiles. The full stack is required for proper functionality due to interdependencies between services.

```bash
just start robosystems    # Starts full stack
just restart robosystems  # Restarts full stack
```

### Core Services

- **FastAPI API** (`api` container) - Port 8000
- **Celery Worker** (`worker` container) - Async task processing
- **PostgreSQL** (`pg` container) - Port 5432
- **Valkey/Redis** (`valkey` container) - Port 6379
- **Kuzu API** (`kuzu-api` container) - Port 8001

### Environment Variables

Key environment variables are centrally managed in `/robosystems/config/env.py`. Always use the centralized configuration:

```python
from robosystems.config import env

# Type-safe access with defaults
database_url = env.DATABASE_URL
debug_mode = env.DEBUG
```

See `.env.example` for the complete list of available configuration options.

## Architecture Overview

### Application Structure

```
robosystems/
├── routers/           # API endpoints organized by domain
├── middleware/        # Cross-cutting concerns
├── models/           # Data models (API and IAM)
├── operations/       # Business logic and workflows
├── tasks/            # Celery async tasks
├── processors/       # Data transformation pipeline
├── adapters/         # External service integrations
├── config/           # Centralized configuration
├── security/         # Security implementations
├── graph_api/         # Graph database API service
└── scripts/          # Utility and admin scripts
```

### Key Components

1. **FastAPI Backend** (`main.py`, `routers/`)

   - RESTful API with automatic OpenAPI documentation
   - Multi-tenant with graph-scoped endpoints: `/v1/graphs/{graph_id}/*`
   - Authentication via JWT and API keys

2. **Kuzu Graph Database System**

   - **Multi-Tenant**: Separate database per entity
   - **Tiered Infrastructure**: Standard, Enterprise, Premium
   - **Shared Repositories**: SEC, industry, economic data
   - **Cluster-Based**: Writer clusters (EC2), reader clusters (ECS Fargate)

3. **Configuration System** (`config/`)

   - **Billing Plans**: Subscription tiers with credit allocations
   - **Rate Limiting**: Burst protection with 1-minute windows
   - **Credit System**: AI operation billing (all database ops are FREE)
   - **Environment Validation**: Startup configuration checks

4. **Middleware Layers**
   - **Graph Routing**: Intelligent cluster selection
   - **Rate Limiting**: Tier-based burst protection
   - **Credits**: AI operation billing and tracking
   - **Security**: Audit logging and authentication
   - **Observability**: OpenTelemetry integration

## Configuration Management

### Centralized Configuration

All configuration is managed in `/robosystems/config/`:

```bash
config/
├── env.py               # Environment variables with validation
├── billing.py           # Subscription plans and pricing
├── rate_limits.py       # Burst-focused rate limiting
├── credits.py           # AI operation credit costs
├── validation.py        # Startup validation
└── valkey_registry.py   # Redis database allocation
```

### Subscription Tiers

```python
# Standard: $49.99/month, 10K AI credits, 5 graphs
# Enterprise: $199.99/month, 50K AI credits, 25 graphs
# Premium: $499.99/month, 200K AI credits, 100 graphs
```

### Credit System (Simplified - AI Operations Only)

- **AI Operations**: Only Anthropic/OpenAI API calls consume credits (token-based billing)
- **FREE Operations**: All database operations (queries, imports, backups, etc.)
- **Monthly Allocations**: Based on subscription tier
- **Storage**: Separate optional billing mechanism (10 credits/GB/day)

### Rate Limiting

Burst-focused protection with 1-minute windows:

```python
# Standard tier examples
GRAPH_READ: 500/min      # 30k/hour possible
GRAPH_WRITE: 100/min     # 6k/hour possible
GRAPH_QUERY: 60/min      # 3.6k/hour possible
AI: 10/min               # 600/hour possible
```

## Database Systems

### PostgreSQL (Primary Database)

```bash
# Database migrations (always use autogenerate)
just migrate-create "description"
just migrate-up
just migrate-down

# Database management
just db-create-test-user           # Creates test user with JWT/API key
just db-list-users                 # List all users
```

### Kuzu Graph Database

**Infrastructure Tiers:**

**Production:**

- **Standard**: r7g.large, 10 databases/instance, 2GB per database
- **Enterprise**: r7g.large, 1 database/instance (isolated), 14GB total
- **Premium**: r7g.xlarge, 1 database/instance (max performance), 28GB total
- **Shared**: r7g.large, multiple repositories (SEC, etc.), shared memory pool

**Staging:**

- **Standard**: r7g.medium, 10 databases/instance, ~700MB per database
- **Shared**: r7g.medium, multiple repositories, shared memory pool

```bash
# Kuzu operations
just kuzu-query graph_id "MATCH (e:Entity) RETURN e"
just kuzu-health                   # Health check
just kuzu-info                     # Database info

# SEC shared database
just sec-load NVDA 2024      # Load company data
just sec-health              # SEC database health
```

### Valkey/Redis Configuration

**IMPORTANT**: Always use the centralized Valkey registry:

```python
from robosystems.config.valkey_registry import ValkeyDatabase, ValkeyURLBuilder

# Use enum for database allocation
redis_url = ValkeyURLBuilder.build_url(env.VALKEY_URL, ValkeyDatabase.AUTH_CACHE)
```

Database allocation:

- 0: Celery broker
- 1: Celery results
- 2: Authentication cache
- 3: Server-sent events
- 4: Distributed locks
- 5: Pipeline tracking
- 6: Credits cache
- 7: Rate limiting
- 8: Kuzu client caching

## Infrastructure & Deployment

### GitHub Actions

**Self-hosted runner**: Deployments, tests, infrastructure
**GitHub-hosted**: Docker builds only

### AWS Infrastructure

- **API/Workers**: ECS Fargate ARM64 (1-2 tasks, 99% Spot)
- **PostgreSQL**: RDS (db.t4g.micro, 20-100GB auto-scaling)
- **Kuzu**: EC2 ARM64 (r7g.medium/large/xlarge, auto-updated AMI)
- **Valkey**: ElastiCache (cache.t4g.micro)

### Configuration Management

**Central Config**: `.github/configs/kuzu.yml` defines all tier specifications

- Instance configuration (hardware specs, memory, performance settings)
- Scaling configuration (min/max replicas, auto-scaling)
- Deployment configuration (feature flags, enablement)

### Deployment Flow

- `staging` branch → staging environment
- `main` branch → production environment
- All deployments through GitHub Actions workflows

## Testing & Code Quality

### Testing Framework

```bash
# Test commands
just test                          # Unit tests
just test-all                      # Full test suite with linting
just test-cov                      # Coverage report

# Test markers
@pytest.mark.unit                  # Unit tests
@pytest.mark.integration           # Integration tests
@pytest.mark.kuzu_integration      # Kuzu-specific integration
@pytest.mark.celery                # Celery task tests
```

### Code Quality

```bash
# Code quality tools
just lint                          # Ruff linting
just format                        # Ruff formatting
just typecheck                     # Pyright type checking
just cf-lint template              # CloudFormation linting
just cf-validate template          # CloudFormation validation
```

**Standards:**

- Python 3.12.10, uv package management
- Ruff formatting (88-char, double quotes)
- Type hints with basedpyright
- **NO COMMENTS** unless explicitly requested

## Common Patterns

### Working with README Files

**IMPORTANT**: Before working in any major directory, always read the README.md file:

- `/robosystems/middleware/graph/README.md` - Graph database middleware
- `/robosystems/operations/README.md` - Business logic operations
- `/robosystems/config/README.md` - Configuration patterns
- `/robosystems/tasks/README.md` - Celery task patterns

### API Development

```python
# API endpoint pattern
from robosystems.middleware.auth import require_auth
from robosystems.models.api import SomeModel

@router.get("/endpoint")
@require_auth
async def endpoint(request: Request) -> SomeModel:
    # Implementation
    pass
```

### Configuration Access

```python
# ALWAYS use centralized config
from robosystems.config import env

# NEVER use os.getenv() directly
database_url = env.DATABASE_URL  # ✓ Correct
database_url = os.getenv("DATABASE_URL")  # ✗ Wrong
```

### Database Migrations

1. **Update SQLAlchemy models** in `/robosystems/models/iam/`
2. **Generate migration**: `just migrate-create "description"`
3. **Review and run**: `just migrate-up`

**NEVER manually create migrations** - always use autogenerate.

## Troubleshooting

### Common Issues

1. **Environment Variables**

   - Verify names match between CloudFormation and app code
   - Check .env file exists and is properly formatted
   - Use env.py validation functions

2. **Docker Issues**

   - Always use `robosystems` profile, not individual services
   - Run `just restart` after code changes
   - Use `just rebuild` for package/environment changes

3. **Database Connection**

   - Ensure PostgreSQL container is running
   - Check DATABASE_URL format
   - Verify migration status with `just migrate-current`

4. **Kuzu Database**

   - Check Kuzu API health with `just kuzu-health`
   - Verify cluster status in CloudFormation
   - Check allocation manager for database assignments

5. **Celery Tasks**
   - Monitor worker logs with `just logs worker`
   - Check Valkey queue status
   - Use DLQ management commands: `just dlq-stats`

### Development Debugging

```bash
# Logs and monitoring
just logs api                      # API logs
just logs-grep worker ERROR        # Search worker logs
just log-search "ERROR" prod "" 1  # CloudWatch search

# Database debugging
just db-info                       # Database status
just kuzu-info                     # Kuzu cluster info
just credit-admin-health           # Credit system health
```

### Secret Management

AWS Secrets Manager Base: `robosystems/{staging|prod}`
Components: `robosystems/{staging|prod}/{postgres|s3|kuzu}`

Never commit secrets to code. Use environment variables or AWS Secrets Manager.

---

**Support**: For questions about this setup, refer to individual README files in component directories, or check the GitHub Actions workflows for deployment examples.
