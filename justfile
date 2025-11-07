# =============================================================================
# ROBOSYSTEMS JUSTFILE DEVELOPMENT & DEPLOYMENT COMMANDS
# =============================================================================
#
# ENVIRONMENT CONFIGURATION:
#   - .env: Container hostnames for Docker services (required by compose.yaml)
#   - .env.local: Localhost overrides for justfile commands (default: most recipes use this)
#
# QUICK START:
#   just start             # Full Docker setup (creates .env files automatically)
#   just restart           # After code changes (no rebuild)
#   just test              # Run tests
#   just logs api          # View API logs
#
# =============================================================================

_env := ".env"
_local_env := ".env.local"

default:
    @just --list

## Docker ##

# Start service
start profile="robosystems" build="--build" detached="--detach":
    @just compose-up {{profile}} {{build}} {{detached}}

# Stop service
stop profile="robosystems":
    @just compose-down {{profile}}

# Quick restart containers to pick up code changes via volume mounts (no rebuild)
restart profile="robosystems":
    docker compose -f compose.yaml --profile {{profile}} restart

# Restart specific service(s) without stopping everything
restart-service service:
    docker compose -f compose.yaml restart {{service}}

# Rebuild containers (full rebuild with new images)
rebuild profile="robosystems":
    @just compose-down {{profile}}
    @just compose-up {{profile}} --build --detach

# Docker commands
compose-up profile="robosystems" build="--build" detached="--detach" env=_env:
    @test -f {{env}} || cp .env.example {{env}}
    docker compose -f compose.yaml --env-file {{env}} --profile {{profile}} up {{build}} {{detached}}

compose-down profile="robosystems":
    docker compose -f compose.yaml --profile {{profile}} down

# Docker logs (without follow to prevent hanging)
logs container="worker" lines="100":
    docker logs {{container}} --tail {{lines}}

# Docker logs with follow (tail -f style)
logs-follow container="worker":
    docker logs -f {{container}}

# Docker logs with grep filter
logs-grep container="worker" pattern="ERROR" lines="100":
    docker logs {{container}} --tail {{lines}} | grep -E "{{pattern}}"

## Development Environment ##

# Initialize complete development environment (run after bootstrap)
init:
    uv python install $(cat .python-version)
    @test -f {{_env}} || cp .env.example {{_env}}
    @test -f {{_local_env}} || cp .env.local.example {{_local_env}}
    @just venv

# Create virtual environment (assumes uv is installed)
venv:
    uv venv
    source .venv/bin/activate
    @just install

# Install dependencies from lock file
install:
    uv sync --all-extras --dev

# Update dependencies and regenerate lock file
update:
    uv lock --upgrade
    uv sync --all-extras --dev

## Demo Scripts ##

# Setup SEC repository demo - loads data, grants access, updates config
demo-sec ticker="NVDA" year="2025" skip_queries="false":
    uv run examples/sec_demo/main.py --ticker {{ticker}} --year {{year}} {{ if skip_queries == "true" { "--skip-queries" } else { "" } }}

# Run SEC demo preset queries
demo-sec-query all="false":
    uv run examples/sec_demo/query_examples.py {{ if all == "true" { "--all" } else { "" } }}

# Run accounting demo end-to-end (flags: new-user,new-graph,skip-queries)
demo-accounting flags="new-graph" base_url="http://localhost:8000":
    uv run examples/accounting_demo/main.py --base-url {{base_url}} {{ if flags != "" { "--flags " + flags } else { "" } }}

# Run custom graph demo end-to-end (flags: new-user,new-graph,skip-queries)
demo-custom-graph flags="new-graph" base_url="http://localhost:8000":
    uv run examples/custom_graph_demo/main.py --base-url {{base_url}} {{ if flags != "" { "--flags " + flags } else { "" } }}

## Testing ##

# Run all tests (excludes slow tests)
test-all:
    @just test
    @just lint-fix
    @just lint
    @just format
    @just typecheck

# Run tests (exclude integration and slow tests)
test:
    uv run pytest --ignore=tests/integration -m "not slow"

# Run tests (exclude integration and slow tests)
test-module module:
    uv run pytest tests/{{module}}

# Run ALL tests including slow ones
test-full:
    uv run pytest

# Run integration tests
test-integration:
    uv run pytest tests/integration

# Run tests with coverage (excludes integration)
test-cov:
    uv run pytest --cov=robosystems tests/ --ignore=tests/integration

# Run code quality checks
test-code-quality:
    @just lint
    @just format
    @just typecheck

# Run linting
lint:
    uv run ruff check .

# Fix linting errors
lint-fix:
    uv run ruff check . --fix

# Format code
format:
    uv run ruff format .

# Run type checking
typecheck:
    uv run basedpyright

# CloudFormantion linting
cf-lint template:
    uv run cfn-lint -t cloudformation/{{template}}.yaml

cf-validate template:
    uv run aws cloudformation validate-template --template-body file://cloudformation/{{template}}.yaml

## CI/CD ##

# Create a feature branch
create-feature type="feature" name="" base="main":
    @bin/tools/create-feature.sh {{type}} {{name}} {{base}}

# Create a pull request
create-pr target="main" review="true":
    @bin/tools/create-pr.sh {{target}} {{review}}

# Create a release branch from main with deployment option
create-release version="patch" deploy="staging":
    @bin/tools/create-release.sh {{version}} {{deploy}}

# Deploy current branch/tag to specified environment
deploy environment="prod" ref="":
    @bin/tools/deploy.sh {{environment}} {{ref}}

# Bastion tunnel
bastion-tunnel environment service key:
    @bin/tools/tunnels.sh {{environment}} {{service}} --key ~/.ssh/{{key}}

# AWS Secrets Manager - Initial Setup
setup-aws:
    @bin/setup/aws.sh

# GitHub Repository - Initial Setup
setup-gha:
    @bin/setup/gha.sh

# GitHub Actions Runner - Bootstrap infrastructure
bootstrap branch="main":
    gh workflow run gha-runner.yml --ref {{branch}}

# Generate secure random key for secrets
generate-key:
    @echo "Generated secure 32-byte base64 key:"
    @openssl rand -base64 32

# Generate multiple secure keys for all secrets
generate-keys:
    @echo "JWT_SECRET_KEY=$(openssl rand -base64 32)"
    @echo "CONNECTION_CREDENTIALS_KEY=$(openssl rand -base64 32)"
    @echo "GRAPH_BACKUP_ENCRYPTION_KEY=$(openssl rand -base64 32)"

## Apps ##

# Install apps
install-apps:
    @test -d '../roboledger-app' || git clone https://github.com/RoboFinSystems/roboledger-app.git '../roboledger-app'
    @test -d '../roboinvestor-app' || git clone https://github.com/RoboFinSystem/roboinvestor-app.git '../roboinvestor-app'
    @test -d '../robosystems-app' || git clone https://github.com/RoboFinSystem/robosystems-app.git '../robosystems-app'

## Development Server ##

# Start development server
api env=_local_env:
    UV_ENV_FILE={{env}} uv run uvicorn main:app --reload

# Start Graph API server with Kuzu backend (configurable node type)
graph-api backend="kuzu" type="writer" port="8001" env=_local_env:
    UV_ENV_FILE={{env}} GRAPH_BACKEND_TYPE={{backend}} KUZU_NODE_TYPE={{type}} uv run python -m robosystems.graph_api --port {{port}}

# Start worker
worker num_workers="1" queue="robosystems" env=_local_env:
    UV_ENV_FILE={{env}} uv run celery -A robosystems worker -B -n rsworkerbeat --concurrency={{num_workers}} -Q {{queue}} -l info -Ofair --prefetch-multiplier=0

# Start beat worker (Celery scheduler)
beat env=_local_env:
    UV_ENV_FILE={{env}} uv run celery -A robosystems beat -l info

# Start Flower (Celery monitoring web UI)
flower env=_local_env:
    UV_ENV_FILE={{env}} uv run celery -A robosystems flower --port=5555

## Database Operations ##

# Create new migration
migrate-create message env=_local_env:
    UV_ENV_FILE={{env}} uv run alembic revision --autogenerate -m "{{message}}"

# Run migrations
migrate-up env=_local_env:
    UV_ENV_FILE={{env}} uv run alembic upgrade head

# Rollback migration
migrate-down env=_local_env:
    UV_ENV_FILE={{env}} uv run alembic downgrade -1

# Show migration history
migrate-history env=_local_env:
    UV_ENV_FILE={{env}} uv run alembic history

# Show current migration
migrate-current env=_local_env:
    UV_ENV_FILE={{env}} uv run alembic current

# Run migrations on remote environment via bastion
migrate-remote environment key:
    @just bastion-tunnel {{environment}} migrate {{key}}

# Reset database (drop and recreate all auth tables)
db-reset env=_local_env:
    UV_ENV_FILE={{env}} uv run alembic downgrade base
    UV_ENV_FILE={{env}} uv run alembic upgrade head

# Database management commands
db-info env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.db_manager info

db-list-users env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.db_manager list-users

db-create-user email name password env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.db_manager create-user {{email}} "{{name}}" {{password}}

db-create-key email key_name env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.db_manager create-key {{email}} "{{key_name}}"

# Create test user (comma-separated modes: json,file,sec,industry,economic - e.g., 'json,sec,industry' for JSON output with multiple repository access)
db-create-test-user mode="" base_url="http://localhost:8000" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.create_test_user --base-url "{{base_url}}" {{ if mode != "" { "--modes " + mode } else { "" } }}

## Graph API ##

# Graph API - health check
graph-health url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.graph_query --url {{url}} --command health

# Graph API - get database info
graph-info graph_id url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.graph_query --url {{url}} --graph-id {{graph_id}} --command info

# Graph API - execute Cypher query (single quotes auto-converted to double quotes for Cypher)
# Examples:
#   just graph-query sec "MATCH (e:Entity {ticker: 'AAPL'}) RETURN e.name"
#   just graph-query sec "MATCH (e:Entity) WHERE e.ticker IN ['AAPL', 'MSFT'] RETURN e.name"
graph-query graph_id query format="table" url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.graph_query --url {{url}} --graph-id {{graph_id}} --query "{{query}}" --format {{format}}

# Graph API - execute SQL query on staging tables (DuckDB-based)
tables-query graph_id query format="table" url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.tables_query --url {{url}} --graph-id {{graph_id}} --query "{{query}}" --format {{format}}

# Kuzu embedded database direct query (bypasses API)
kuzu-query graph_id query format="table" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.kuzu_query --db-path ./data/kuzu-dbs/{{graph_id}}.kuzu --query "{{query}}" --format {{format}}

# DuckDB staging database direct query (bypasses API)
duckdb-query graph_id query format="table" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.duckdb_query --db-path ./data/staging/{{graph_id}}.duckdb --query "{{query}}" --format {{format}}

# Interactive query modes - launch REPL for each database type
graph-query-i graph_id url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.graph_query --url {{url}} --graph-id {{graph_id}}

tables-query-i graph_id url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.tables_query --url {{url}} --graph-id {{graph_id}}

kuzu-query-i graph_id env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.kuzu_query --db-path ./data/kuzu-dbs/{{graph_id}}.kuzu

duckdb-query-i graph_id env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.duckdb_query --db-path ./data/staging/{{graph_id}}.duckdb

## SEC Local Pipeline - Testing and Development ##
# SEC Local supports two ingestion approaches:
#   - "duckdb" (default): DuckDB staging → Direct ingestion (fast, many small files, S3 as source of truth)
#   - "copy": Consolidation → COPY-based ingestion (emulates production pipeline, uses consolidated files)
# Examples:
#   just sec-load NVDA 2025                              # Load NVIDIA 2025 data using defaults (duckdb, kuzu)
#   just sec-load NVDA 2025 [duckdb|copy] [kuzu|neo4j]   # Specify ingestion method and/or backend

# SEC Local - Load single company by ticker and year(s)
sec-load ticker year="" ingest_method="duckdb" backend="kuzu" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_local load --ticker {{ticker}} {{ if year != "" { "--year " + year } else { "" } }} --backend {{backend}} {{ if ingest_method == "copy" { "--use-copy-pipeline" } else { "" } }}

# SEC Local - Health check (use --verbose for detailed report, --json for JSON output)
sec-health verbose="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_local_health {{ if verbose == "v" { "--verbose" } else { "" } }}

# SEC Local - Reset database with proper schema
sec-reset backend="kuzu" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_local reset --backend {{backend}}

## SEC Production Pipeline - Large-scale orchestrated processing ##
# Uses proven consolidation + COPY approach for large-scale data processing.
# Pipeline phases: download → process → consolidate → ingest (COPY-based)
# Examples:
#   just sec-plan 2020 2025 100                                 # Plan processing for 100 companies
#   just sec-phase [download|process|consolidate|ingest]        # Start a specific phase

# SEC Production - Plan processing with optional company limit for testing
sec-plan start_year="2020" end_year="2025" max_companies="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_orchestrator plan --start-year {{start_year}} --end-year {{end_year}} --max-companies {{max_companies}}

# SEC Production - Start a specific phase: download, process, consolidate, ingest
sec-phase phase env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_orchestrator start-phase --phase {{phase}}

# SEC Production - Resume a phase from last checkpoint
sec-phase-resume phase env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_orchestrator start-phase --phase {{phase}} --resume

# SEC Production - Retry failed companies in a phase
sec-phase-retry phase env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_orchestrator start-phase --phase {{phase}} --retry-failed

# SEC Production - Get status of all phases
sec-status env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_orchestrator status

# SEC Production - Reset database (requires confirmation)
sec-reset-remote confirm="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_orchestrator reset {{ if confirm == "yes" { "--confirm" } else { "" } }}

## Repository Access Management ##
# Manage user access to shared repositories (SEC, industry data, etc.)

# Grant repository access
repo-grant-access user_id repository access_level="read" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager grant {{user_id}} {{repository}} {{access_level}}

# Grant repository access with expiration
repo-grant-access-expire user_id repository access_level expires_days env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager grant {{user_id}} {{repository}} {{access_level}} --expires-days {{expires_days}}

# Revoke repository access
repo-revoke-access user_id repository env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager revoke {{user_id}} {{repository}}

# List all repository access
repo-list-access env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager list

# List users with access to a specific repository
repo-list-users repository env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager list --repository {{repository}}

# List all available repositories
repo-list-repositories env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager repositories

# Check user access to all repositories
repo-check-access user_id env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager check {{user_id}}

# Check user access to a specific repository
repo-check-repo user_id repository env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager check {{user_id}} --repository {{repository}}

## Admin CLI ##
# Remote administration via admin API
# - dev: Uses localhost:8000 and ADMIN_API_KEY from .env.local
# - staging/prod: Uses remote URLs and AWS Secrets Manager for auth
#
# Examples:
#   just admin-subscriptions-list                    # Dev (default)
#   just admin-subscriptions-list prod               # Production
#   just admin-subscriptions-list staging            # Staging

# List all subscriptions
admin-subscriptions-list environment="dev" status="" tier="" email="" include_canceled="" limit="100" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.admin.cli -e {{environment}} subscriptions list {{ if status != "" { "--status " + status } else { "" } }} {{ if tier != "" { "--tier " + tier } else { "" } }} {{ if email != "" { "--email " + email } else { "" } }} {{ if include_canceled == "true" { "--include-canceled" } else { "" } }} --limit {{limit}}

# Get subscription details
admin-subscription-get subscription_id environment="dev" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.admin.cli -e {{environment}} subscriptions get {{subscription_id}}

# Create new subscription
admin-subscription-create resource_id user_id plan_name billing_interval="monthly" environment="dev" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.admin.cli -e {{environment}} subscriptions create --resource-id {{resource_id}} --user-id {{user_id}} --plan-name {{plan_name}} --billing-interval {{billing_interval}}

# Update subscription
admin-subscription-update subscription_id environment="dev" status="" plan_name="" price="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.admin.cli -e {{environment}} subscriptions update {{subscription_id}} {{ if status != "" { "--status " + status } else { "" } }} {{ if plan_name != "" { "--plan-name " + plan_name } else { "" } }} {{ if price != "" { "--price " + price } else { "" } }}

# View subscription audit log
admin-subscription-audit subscription_id environment="dev" event_type="" limit="50" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.admin.cli -e {{environment}} subscriptions audit {{subscription_id}} {{ if event_type != "" { "--event-type " + event_type } else { "" } }} --limit {{limit}}

# List all customers
admin-customers-list environment="dev" limit="100" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.admin.cli -e {{environment}} customers list --limit {{limit}}

# Update customer billing settings
admin-customer-update user_id environment="dev" invoice_billing="" billing_email="" billing_contact_name="" payment_terms="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.admin.cli -e {{environment}} customers update {{user_id}} {{ if invoice_billing == "true" { "--invoice-billing" } else if invoice_billing == "false" { "--no-invoice-billing" } else { "" } }} {{ if billing_email != "" { "--billing-email " + billing_email } else { "" } }} {{ if billing_contact_name != "" { "--billing-contact-name " + billing_contact_name } else { "" } }} {{ if payment_terms != "" { "--payment-terms " + payment_terms } else { "" } }}

# Show subscription and customer statistics
admin-stats environment="dev" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.admin.cli -e {{environment}} stats

## Credit Admin Tools ##
# Monthly credit allocation is automated via Celery Beat - these commands are for manual operations only

# Credit admin - add bonus credits to a user graph
credit-bonus-graph graph_id amount description env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.credit_admin bonus-graph {{graph_id}} --amount {{amount}} --description "{{description}}"

# Credit admin - add bonus credits to a user's repository subscription
credit-bonus-repository user_id repository_name amount description env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.credit_admin bonus-repository {{user_id}} {{repository_name}} --amount {{amount}} --description "{{description}}"

# Credit admin - check credit system health
credit-health env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.credit_admin health

# Credit admin - manual allocation for specific user (DANGEROUS - requires confirm="yes")
credit-force-allocate-user user_id confirm="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.credit_admin force-allocate-user {{user_id}} {{ if confirm == "yes" { "--confirm" } else { "" } }}

# Credit admin - manual allocation for ALL users (DANGEROUS - requires confirm="yes")
credit-force-allocate-all confirm="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.credit_admin force-allocate-all {{ if confirm == "yes" { "--confirm" } else { "" } }}

## Valkey/Redis ##

# Clear Valkey/Redis queues
valkey-clear-queue queue env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.clear_valkey_queues {{queue}}

# Clear Valkey/Redis queues including unacknowledged messages
valkey-clear-queue-all queue env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.clear_valkey_queues --clear-unacked {{queue}}

# List Valkey/Redis queue contents without clearing
valkey-list-queue queue env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.clear_valkey_queues --list-only {{queue}}

## Misc ##

# Clean up development artifacts
clean:
    rm -rf .pytest_cache
    rm -rf .ruff_cache
    rm -rf __pycache__
    rm -rf robosystems_service.egg-info
    find . -type d -name "__pycache__" -exec rm -rf {} +
    find . -type f -name "*.pyc" -delete

# Clean up development data (reset all local data)
clean-data:
    @just clean
    rm -rf ./data/kuzu-dbs
    rm -rf ./data/staging
    rm -rf ./data/arelle
    rm -rf ./data/neo4j
    rm -rf ./data/localstack
    rm -rf ./data/postgres
    rm -rf ./data/valkey
    rm -rf ./local/creds
    rm -f ./examples/credentials/config.json

# Show help
help:
    @just --list
