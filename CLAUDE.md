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

## API Endpoints (local testing)

Core platform APIs are mounted under `/v1` (graphs, billing, auth, etc.).
Extensions (roboledger, roboinvestor) live under `/extensions` and are
**graph-scoped at the URL level**, with three sub-surfaces:

- **Typed reads** → `POST /extensions/{graph_id}/graphql` (Strawberry + GraphiQL in dev)
- **Command writes** → `POST /extensions/{roboledger|roboinvestor}/{graph_id}/operations/{operation_name}`
- **Analytical view operations** → `POST /extensions/{domain}/{graph_id}/operations/{view_name}` — graph-backed operations that query LadybugDB rather than the extensions OLTP database. Read-only, same envelope contract as command writes. `build-fact-grid` is the first one (pivot tables over the XBRL hypercube); gated independently of the OLTP domain flags so deployments without the corresponding tenants can still mount them

All three surfaces take `graph_id` as a URL path parameter — auth + per-graph
access are validated by FastAPI dependencies before the handler runs.
GraphQL queries do NOT take a `graphId` argument; the URL is the scope.

Per-domain feature flags: `ROBOLEDGER_ENABLED` and `ROBOINVESTOR_ENABLED`
gate the corresponding GraphQL resolvers and operation routers. The
schema is built dynamically per flag combo, so a ledger-only deployment
exposes only ledger fields (no `INVESTOR_NOT_INITIALIZED` runtime errors).

**Graph lifecycle writes** follow the same CQRS pattern at
`POST /v1/graphs/{graph_id}/operations/{op_name}`:

| Operation | Path |
| --------- | ---- |
| Create subgraph | `POST /v1/graphs/{g}/operations/create-subgraph` |
| Delete subgraph | `POST /v1/graphs/{g}/operations/delete-subgraph` |
| Create backup | `POST /v1/graphs/{g}/operations/create-backup` |
|  Change tier | `POST /v1/graphs/{g}/operations/change-tier` |
| Materialize | `POST /v1/graphs/{g}/operations/materialize` |
| Update metadata | `POST /v1/graphs/{g}/operations/update-graph-metadata` |

All graph operation responses are `OperationEnvelope` and support `Idempotency-Key`. Reads (list subgraphs, list backups, health, etc.) remain REST GETs at their existing paths.

Common mistakes:

| ❌ Wrong                              | ✅ Correct                                                                | Purpose              |
| ------------------------------------- | ------------------------------------------------------------------------- | -------------------- |
| `GET /health`, `GET /v1/health`       | `GET /v1/status`                                                           | API health check     |
| `GET /v1/ledger/{g}/entity`           | GraphQL `POST /extensions/{g}/graphql` body `{ entity { … } }`             | Ledger read          |
| `PUT /v1/ledger/{g}/entity`           | `POST /extensions/roboledger/{g}/operations/update-entity`                 | Ledger write         |
| `POST /v1/graphs/{g}/views`           | `POST /extensions/roboledger/{g}/operations/build-fact-grid`               | Fact grid query      |
| `{ entity(graphId: "kg_x") }`         | `{ entity { … } }` (graph_id comes from URL)                                | GraphQL query        |
| `GET /graphs/...`                     | `GET /v1/graphs/{graph_id}/...`                                            | Graph endpoints      |
| `POST /v1/graphs/{g}/subgraphs`       | `POST /v1/graphs/{g}/operations/create-subgraph`                           | Create subgraph      |
| `DELETE /v1/graphs/{g}/subgraphs/{n}` | `POST /v1/graphs/{g}/operations/delete-subgraph`                           | Delete subgraph      |
| `POST /v1/graphs/{g}/backups`         | `POST /v1/graphs/{g}/operations/create-backup`                             | Create backup        |
| `POST /v1/graphs/{g}/materialize`     | `POST /v1/graphs/{g}/operations/materialize`                               | Materialize graph    |

- **Root `/`** serves the Swagger UI (HTML); don't use it for health checks.
- **`/openapi.json`** is the live OpenAPI spec — useful when SDK generation drifts from the server.
- **All authenticated endpoints** take `X-API-Key` for local testing (not `Authorization: Bearer`). Read the key from `.local/config.json` after running `just demo-user`.
- **Frontend-facing auth** (JWT/Bearer) is a frontend concern; backend testing with `curl` should use the API key.

Example:

```bash
# Health check
curl http://localhost:8000/v1/status

# GraphQL read (fiscal calendar) — graph_id is in the URL, not the query
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ fiscalCalendar { closedThrough closeTarget } }"}'

# Extensions operation write (close a period)
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/close-period" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"period": "2026-03", "allow_stale_sync": false}'

# Graph operation write (materialize)
curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/operations/materialize" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json"
```

## Quick Reference

### Daily Development

```bash
just start                 # Start full Docker stack
just upgrade               # Fetch latest images (or rebuild) and recreate what changed
just restart               # Quick restart (Python code changes only)
just rebuild               # Full rebuild (dependency/Dockerfile changes)
just test                  # Run tests (excludes slow/integration)
just logs api              # View API logs
just logs dagster-daemon   # View Dagster daemon logs
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
just demo-user                            # Create/reuse demo user credentials
just demo-custom-graph                    # Run custom graph demo
just demo-sec --ticker NVDA --year 2025   # Run SEC demo
just demo-roboledger                      # Full RoboLedger demo (synthetic data)
just demo-roboledger --skeleton           # Skeleton: user + empty roboledger graph for manual QB sandbox connect
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

RoboSystems has **two separate databases** with independent migration histories:

- **Platform DB** (`robosystems`) — users, orgs, graphs, billing, connections, documents. Models in `/robosystems/models/core/`.
- **Extensions DB** (`extensions`) — per-graph OLTP for RoboLedger and RoboInvestor with schema-per-graph-id tenancy. Models in `/robosystems/models/extensions/`.

Every migration command takes an optional `db` argument that defaults to `platform`:

```bash
# Platform (default)
just migrate-create "description"      # autogenerate
just migrate-up                        # apply
just migrate-down                      # rollback one
just migrate-current                   # show revision

# Extensions — pass "extensions" as the second argument
just migrate-create "description" extensions
just migrate-up extensions
just migrate-down extensions
just migrate-current extensions
```

Workflow for any change:

1. Update the SQLAlchemy model in `/robosystems/models/core/` or `/robosystems/models/extensions/`
2. Generate the migration against the correct database — `just migrate-create "msg"` for platform, `just migrate-create "msg" extensions` for extensions
3. Review the generated file (autogenerate misses enum changes, CHECK constraints, some index changes — fix those by hand)
4. Apply with `just migrate-up` or `just migrate-up extensions`

## Architecture Overview

```
robosystems/
├── routers/                      # API endpoints (thin layer, calls operations)
│   ├── extensions/               # Extensions command + analytical view surface
│   │   ├── roboledger/           # operations.py + views.py (build-fact-grid)
│   │   │                         # /extensions/roboledger/{g}/operations/*
│   │   └── roboinvestor/         # /extensions/roboinvestor/{g}/operations/*
│   └── …                         # Core platform routers (graphs, billing, auth, …)
├── graphql/                      # Strawberry GraphQL served at /extensions/{graph_id}/graphql
│   ├── types/                    # Strawberry types (wrap Pydantic response models)
│   ├── resolvers/                # Per-domain resolver classes (ledger, investor)
│   ├── context.py                # get_context / require_user
│   ├── auth.py                   # check_graph_access
│   └── schema.py                 # Query root (composes resolvers per ROBOLEDGER/ROBOINVESTOR flags)
├── operations/                   # Business logic kernel — single source of truth
│   ├── roboledger/
│   │   ├── reads/                # OLTP reads (PostgreSQL extensions DB)
│   │   ├── commands/             # OLTP writes (PostgreSQL extensions DB)
│   │   ├── views/                # Graph reads (LadybugDB XBRL hypercube)
│   │   ├── fiscal_calendar/      # FiscalCalendarService, PeriodCloseService
│   │   ├── reports/              # fact_grid, guard_rails
│   │   └── schedules/            # ScheduleService
│   ├── roboinvestor/
│   │   ├── reads/                # portfolios, securities, positions, holdings
│   │   └── commands/             # portfolios, securities, positions
│   ├── graph/                    # Graph services (credit, entity, subscription)
│   ├── extensions/               # OLTP→OLAP materialization (materialize.py, loader.py, staleness.py)
│   ├── operators/                # AI Operator operations (Claude/MCP executors — distinct from REA Agent counterparty model)
│   └── providers/                # Provider registry and implementations
├── middleware/                   # Cross-cutting concerns
│   ├── auth/                     # Authentication (JWT, API keys, SSO)
│   ├── billing/                  # Credit consumption tracking
│   ├── graph/                    # Graph routing and multi-tenancy
│   ├── rate_limits/              # Burst protection
│   ├── sse/                      # Server-Sent Events
│   ├── extensions.py             # OperationEnvelope, IdempotencyCache, execute_operation, audit
│   └── …                         # mcp, otel, robustness
├── dagster/                      # Dagster orchestration (jobs, sensors, assets, resources)
├── adapters/                     # External service integrations (SEC, QuickBooks)
├── admin/                        # Admin CLI and utilities
├── security/                     # Security controls (audit, auth protection, encryption)
├── models/
│   ├── api/                      # Pydantic request/response models
│   │   └── extensions/           # RoboLedger + RoboInvestor API models
│   ├── core/                     # Platform SQLAlchemy models (users, orgs, graphs, billing, connections, documents)
│   └── extensions/               # Extensions OLTP SQLAlchemy models (roboledger, roboinvestor); schema-per-graph tenancy
├── config/                       # Centralized configuration (see config/README.md)
├── schemas/                      # Graph schema definitions
└── graph_api/                    # Graph API microservice
```

### Key Architectural Patterns

1. **Operations orchestrate, adapters integrate**: Operations coordinate business logic; adapters handle external service integration and data transformation
2. **Operations kernel as single source of truth**: `operations/roboledger/{reads,commands,views}/` and `operations/roboinvestor/{reads,commands}/` hold domain logic as pure functions (session-in, Pydantic-out, domain exceptions). GraphQL resolvers, REST command operation routers, analytical view handlers, MCP tools, and AI Operators all delegate to the same functions. Adding business logic in a router, resolver, or MCP tool handler is a mistake — route it through the ops layer.
3. **Multi-tenant by design**: Core platform operations scoped to `graph_id`; extensions OLTP uses schema-per-graph-id PostgreSQL tenancy with `SET search_path` isolation
4. **Two-database split**: Platform (`robosystems`) for IAM/billing/metadata; extensions (`extensions`) for per-graph OLTP. Different `DeclarativeBase` classes, independent migration histories, same shared RDS instance
5. **Credit-based AI billing**: Only AI operations (Anthropic/OpenAI) consume credits; database operations are free
6. **Graph backend**: LadybugDB (`GRAPH_BACKEND_TYPE=ladybug`)
7. **Two `Agent` concepts, disambiguated**: `Agent` (REA party — customer/vendor/employee counterparty) lives in `models/extensions/roboledger/agent.py` and is the canonical ontology term. The AI executor layer (Claude/MCP-driven, used to be called "AI agents") is named **Operator** throughout the codebase: `routers/graphs/operator/`, `operations/operators/`, classes like `CypherOperator` / `MappingOperator`, endpoint `/v1/graphs/{g}/operator`. Marketing-facing copy can still say "AI agent" / "AI assistant" — that's decoupled from internal naming.

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
GRAPH_BACKEND_TYPE=ladybug
LBUG_DATABASE_PATH=/data/lbug-dbs

# Feature Flags
RATE_LIMIT_ENABLED=true
BILLING_ENABLED=true

# Extensions — RoboLedger & RoboInvestor product surfaces
ROBOLEDGER_ENABLED=true            # gates roboledger ops + GraphQL ledger fields
ROBOINVESTOR_ENABLED=true          # gates roboinvestor ops + GraphQL investor fields
EXTENSIONS_GRAPHQL_ENABLED=true    # kill switch for the GraphQL endpoint
EXTENSIONS_DATABASE_URL=postgresql://...  # extensions OLTP database
# EXTENSIONS_ENABLED is a derived property (ROBOLEDGER_ENABLED OR ROBOINVESTOR_ENABLED)
# — not a separate env var. Legacy LEDGER_ENABLED / INVESTOR_ENABLED names
# have been retired; only ROBOLEDGER_ENABLED / ROBOINVESTOR_ENABLED are read.
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
| `operators.py`          | Claude model configuration (Bedrock) — AI Operator config |
| `validation.py`         | Startup configuration checks                   |
| `valkey_registry.py`    | Valkey database allocation (never hardcode DB numbers) |
| `storage/`              | S3 path helpers (shared data, graph storage)   |

## Graph API

### Backend

- **LadybugDB**: Embedded columnar graph database

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
- Connection pool is capped at 3 connections per database, everywhere. The per-tier
  `connection_pool_size` in `.github/configs/graph.yml` and `LBUG_CONNECTION_POOL_SIZE` are surfaced
  by `env.get_lbug_tier_config()` but read by nothing — tuning either is currently a no-op.
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
just logs api              # Check API logs
just logs-grep worker ERROR  # Search worker logs
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
- **Release notes**: `tag-release.yml` auto-generates the changelog from changes since the last tag (via the Claude API). For milestone releases, commit curated notes to `.github/release-notes/v<version>.md` _before_ dispatching `create-release.yml` — when that file exists at the tagged ref it replaces the generated changelog (and the stats section is skipped)
- **Infrastructure Config**: `.github/configs/graph.yml`

## AWS Infrastructure

| Component   | Service           | Notes                          |
| ----------- | ----------------- | ------------------------------ |
| API/Workers | ECS Fargate ARM64 | Auto-scaling, Spot-preferred   |
| PostgreSQL  | RDS               | Burstable T4g instance         |
| LadybugDB   | EC2 ARM64         | R7g instances (tier-dependent) |
| Cache       | ElastiCache       | Valkey-compatible              |
| Search      | OpenSearch         | Full-text + semantic (KNN)     |

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
just ssm-list prod features                          # List feature flags
just ssm-list prod tuning                            # List tuning parameters
just ssm-get prod features/MCP_SUBGRAPH_OPS_ENABLED  # Get single parameter
just ssm-set prod features/MCP_SUBGRAPH_OPS_ENABLED true  # Set parameter
just ssm-delete prod features/OLD_FLAG               # Delete parameter
```

Parameters are stored at `/robosystems/{env}/{category}/{NAME}` in SSM Parameter Store. The `{NAME}` segment is **UPPER_SNAKE_CASE**, identical to the env var name (e.g., `RATE_LIMIT_ENABLED`, not `rate-limit-enabled`). Unlike GitHub variables (which require redeployment), SSM parameters take effect immediately at runtime.

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
- `/robosystems/graphql/README.md` - Strawberry GraphQL extensions surface, Pydantic auto-derivation, resolver patterns
- `/robosystems/middleware/auth/README.md` - Authentication system
- `/robosystems/middleware/graph/README.md` - Graph routing
- `/robosystems/operations/README.md` - Business logic patterns
- `/robosystems/dagster/README.md` - Dagster orchestration patterns
- `/robosystems/models/api/README.md` - Pydantic request/response models
- `/robosystems/models/core/README.md` - Platform SQLAlchemy models (users, orgs, graphs, billing, connections, documents)
- `/robosystems/models/extensions/README.md` - Extensions OLTP SQLAlchemy models (roboledger, roboinvestor) with schema-per-graph tenancy
- `/robosystems/schemas/README.md` - Graph schema definitions, extension naming conventions, URL/flag topology
- `/tests/README.md` - Testing guide
- `/examples/README.md` - Demo scripts
