## Default ##
default:
    @just --list

## Docker ##
# Start service
start profile="robosystems" env=".env" build="--build" detached="--detach":
    @just compose-up {{profile}} {{env}} {{build}} {{detached}}

# Stop service
stop profile="robosystems":
    @just compose-down {{profile}}

# Quick restart containers to pick up code changes via volume mounts (no rebuild)
restart profile="robosystems" env=".env":
    docker compose -f compose.yaml --env-file {{env}} --profile {{profile}} restart

# Restart specific service(s) without stopping everything
restart-service service env=".env":
    docker compose -f compose.yaml --env-file {{env}} restart {{service}}

# Rebuild containers (full rebuild with new images)
rebuild profile="robosystems" env=".env":
    @just compose-down {{profile}}
    @just compose-up {{profile}} {{env}} --build --detach

# Docker commands
compose-up profile="robosystems" env=".env" build="--build" detached="":
    test -f {{env}} || cp .env.example {{env}}
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
    @just venv

# Create virtual environment (assumes uv is installed)
venv:
    test -f .env || cp .env.example .env
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

## Testing ##
# Run all tests (excludes slow tests)
test-all:
    @just test-full
    @just lint-fix
    @just lint
    @just format
    @just typecheck

# Run ALL tests including slow ones
test-full:
    uv run pytest

# Run integration tests
test-integration:
    uv run pytest tests/integration

# Run tests (exclude integration and slow tests)
test:
    uv run pytest --ignore=tests/integration -m "not slow"

# Run tests with coverage
test-cov:
    uv run pytest --cov=robosystems tests/ --ignore=tests/integration

# Run linting
lint:
    uv run ruff check .

# Format code
format:
    uv run ruff format .

# Fix linting errors
lint-fix:
    uv run ruff check . --fix

# Run type checking
typecheck:
    uv run basedpyright

# CloudFormantion linting
cf-lint template:
    uv run cfn-lint -t cloudformation/{{template}}.yaml

cf-validate template:
    aws cloudformation validate-template --template-body file://cloudformation/{{template}}.yaml

## CI/CD ##
# Create a feature branch
create-feature type="feature" name="" base="main":
    @bin/tools/create-feature.sh {{type}} {{name}} {{base}}

# Create a release branch from main with deployment option
create-release version="patch" deploy="staging":
    @bin/tools/create-release.sh {{version}} {{deploy}}

# Create a pull request
create-pr target="main" review="true":
    @bin/tools/create-pr.sh {{target}} {{review}}

# Deploy current branch/tag to specified environment
deploy environment="prod" ref="":
    @bin/tools/deploy.sh {{environment}} {{ref}}

# Initial Setup - AWS Secrets Manager
setup-aws:
    @bin/setup/aws.sh

# Initial Setup - GitHub Repository
setup-gha:
    @bin/setup/gha.sh

# Generate secure random key for JWT_SECRET_KEY and other secrets
generate-key:
    @echo "Generated secure 32-byte base64 key:"
    @openssl rand -base64 32

# Generate multiple secure keys for all secret fields
generate-keys:
    @echo "# Add these to your .env file:"
    @echo "JWT_SECRET_KEY=$(openssl rand -base64 32)"
    @echo "CONNECTION_CREDENTIALS_KEY=$(openssl rand -base64 32)"
    @echo "KUZU_BACKUP_ENCRYPTION_KEY=$(openssl rand -base64 32)"

## SSH ##
# Bastion tunnel
bastion-tunnel environment service key:
    @bin/tools/tunnels.sh {{environment}} {{service}} --key ~/.ssh/{{key}}

# SSH EC2 instance
ssh-ec2 url key:
    ssh -i ~/.ssh/{{key}} ec2-user@{{url}}

# SSH EC2 port forward
ssh-ec2-port-forward url port key:
    ssh -i ~/.ssh/{{key}} -L {{port}}:localhost:{{port}} ec2-user@{{url}}

## Apps ##
# Install apps
install-apps:
    test -d '../roboledger-app' || git clone https://github.com/RoboFinSystems/roboledger-app.git '../roboledger-app'
    test -d '../roboinvestor-app' || git clone https://github.com/RoboFinSystem/roboinvestor-app.git '../roboinvestor-app'
    test -d '../robosystems-app' || git clone https://github.com/RoboFinSystem/robosystems-app.git '../robosystems-app'

## Development Server ##
# Environment Detection and Hostname Overrides
_dev := if env_var_or_default("DEV_OVERRIDE", "false") == "true" { "" } else {
  "DATABASE_URL=postgresql://postgres:"+env_var_or_default("PG_PWD", "postgres")+"@localhost:5432/robosystems " +
  "TEST_DATABASE_URL=postgresql://postgres:"+env_var_or_default("PG_PWD", "postgres")+"@localhost:5432/robosystems_test " +
  "CELERY_BROKER_URL=redis://:"+env_var_or_default("VALKEY_PWD", "valkey")+"@localhost:6379/0 " +
  "CELERY_RESULT_BACKEND=redis://:"+env_var_or_default("VALKEY_PWD", "valkey")+"@localhost:6379/1 " +
  "VALKEY_URL=redis://:"+env_var_or_default("VALKEY_PWD", "valkey")+"@localhost:6379 " +
  "KUZU_API_URL=http://localhost:8001 " +
  "AWS_ENDPOINT_URL=http://localhost:4566 " +
  "OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318"
}

# Start development server
api env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run uvicorn main:app --reload

# Start Kuzu API server (configurable node type)
kuzu-api type="writer" port="8001" env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.kuzu_api --node-type {{type}} --port {{port}} --base-path ./data/kuzu-dbs

# Start worker
worker env=".env" num_workers="1" queue="robosystems":
    {{_dev}} UV_ENV_FILE={{env}} uv run celery -A robosystems worker -B -n rsworkerbeat --concurrency={{num_workers}} -Q {{queue}} -l info -Ofair --prefetch-multiplier=0

# Start beat worker (Celery scheduler)
beat env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run celery -A robosystems beat -l info

# Start Flower (Celery monitoring web UI)
flower env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run celery -A robosystems flower --port=5555

## Database Operations ##
# Create new migration
migrate-create message env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run alembic revision --autogenerate -m "{{message}}"

# Run migrations
migrate-up env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run alembic upgrade head

# Rollback migration
migrate-down env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run alembic downgrade -1

# Show migration history
migrate-history env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run alembic history

# Show current migration
migrate-current env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run alembic current

# Run migrations on remote environment via bastion
migrate-remote environment key:
    @just bastion-tunnel {{environment}} migrate {{key}}

# Initialize database with sample data
# Reset database (drop and recreate all auth tables)
db-reset env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run alembic downgrade base
    {{_dev}} UV_ENV_FILE={{env}} uv run alembic upgrade head

# Database management commands
db-info env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.db_manager info

db-list-users env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.db_manager list-users

db-create-user email name password env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.db_manager create-user {{email}} "{{name}}" {{password}}

db-create-key email key_name env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.db_manager create-key {{email}} "{{key_name}}"

# Create test user (add 'file' to save creds, 'json' for JSON output, 'sec' for SEC access)
db-create-test-user mode="" base_url="http://localhost:8000" env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.create_test_user --base-url "{{base_url}}" {{ if mode == "file" { "--save-file" } else if mode == "json" { "--json" } else if mode == "sec" { "--with-sec-access" } else { "" } }}

## Kuzu Database ##
# Kuzu API client - health check
kuzu-health url="http://localhost:8001" env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.kuzu_api --url {{url}} health

# Kuzu API client - get database info
kuzu-info url="http://localhost:8001" env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.kuzu_api --url {{url}} info

# Kuzu API client - execute query
kuzu-query graph_id query format="table" env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.kuzu_api cli query "{{query}}" --database {{graph_id}} --format {{format}}

# Kuzu embedded database direct query tool
kuzu-db-query graph_id query format="table" env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.kuzu_query --db-path ./data/kuzu-dbs/{{graph_id}}.kuzu --query "{{query}}" --format {{format}}

## SEC Local Pipeline - Testing and Development ##
# SEC Local - Load single company by ticker and (all) years (e.g., just sec-load NVDA)
sec-load ticker year="" env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_local load --ticker {{ticker}} {{ if year != "" { "--year " + year } else { "" } }} --force-reconsolidate

# SEC Local - Health check (use --verbose for detailed report, --json for JSON output)
sec-health verbose="" env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_local_health {{ if verbose == "v" { "--verbose" } else { "" } }}

# SEC Local - Reset database with proper schema
sec-reset-local env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_local reset

## SEC Orchestrator - For large-scale production processing ##
# SEC - Plan processing with optional company limit for testing
sec-plan start_year="2020" end_year="2025" max_companies="" env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_orchestrator plan --start-year {{start_year}} --end-year {{end_year}} --max-companies {{max_companies}}

# SEC - Start a specific phase: download, process, consolidate, ingest
sec-phase phase env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_orchestrator start-phase --phase {{phase}}

# SEC - Resume a phase from last checkpoint
sec-phase-resume phase env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_orchestrator start-phase --phase {{phase}} --resume

# SEC - Retry failed companies in a phase
sec-phase-retry phase env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_orchestrator start-phase --phase {{phase}} --retry-failed

# SEC - Get status of all phases
sec-status env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_orchestrator status

# SEC - Reset database (requires confirmation)
sec-reset confirm="" env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_orchestrator reset {{ if confirm == "yes" { "--confirm" } else { "" } }}

## Arelle Schema Cache Management ##
# Arelle Cache commands: update, check, extract, clean, download, fetch-edgar, bundle
arelle-cache command="update":
    uv run python -m robosystems.scripts.arelle_cache_manager {{command}}

## Dead Letter Queue (DLQ) Management ##
# DLQ Stats - Check failed task statistics
dlq-stats env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.manage_dlq stats

# DLQ Health - Check DLQ health status
dlq-health env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.manage_dlq health

# DLQ List - List failed tasks in the queue
dlq-list limit="10" env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.manage_dlq list --limit {{limit}}

# DLQ Reprocess - Retry a failed task
dlq-reprocess task_id env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.manage_dlq reprocess {{task_id}}

# DLQ Purge - Remove all failed tasks
dlq-purge env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.manage_dlq purge --confirm

## Repository Access Management ##
# Repository Access Management
repo-grant-access user_id repository access_level="read" env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager grant {{user_id}} {{repository}} {{access_level}}

# Grant repository access with expiration
repo-grant-access-expire user_id repository access_level expires_days env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager grant {{user_id}} {{repository}} {{access_level}} --expires-days {{expires_days}}

# Revoke repository access
repo-revoke-access user_id repository env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager revoke {{user_id}} {{repository}}

# List all repository access
repo-list-access env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager list

# List repository access
repo-list-access-repo repository env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager list --repository {{repository}}

# Check user access
repo-check-access user_id env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager check {{user_id}}

# Check repository access
repo-check-access-repo user_id repository env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager check {{user_id}} --repository {{repository}}

# List all repositories
repo-list-repositories env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.repository_access_manager repositories

## Credit Admin Tools ##
# Credit admin - allocate credits for a specific user
credit-admin-user user_id env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.credit_admin allocate-user {{user_id}}

# Credit admin - allocate credits for a specific graph
credit-admin-graph graph_id env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.credit_admin allocate-graph {{graph_id}}

# Credit admin - run global credit allocation (all users/graphs)
credit-admin-allocate-all env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.credit_admin allocate-all

# Credit admin - add bonus credits to a graph
credit-admin-bonus graph_id amount description env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.credit_admin bonus {{graph_id}} --amount {{amount}} --description "{{description}}"

# Credit admin - check credit system health
credit-admin-health env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.credit_admin health

## Valkey/Redis ##
# Clear Valkey/Redis queues
valkey-clear-queue queue env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.clear_valkey_queues {{queue}}

# Clear Valkey/Redis queues including unacknowledged messages
valkey-clear-queue-all queue env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.clear_valkey_queues --clear-unacked {{queue}}

# List Valkey/Redis queue contents without clearing
valkey-list-queue queue env=".env":
    {{_dev}} UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.clear_valkey_queues --list-only {{queue}}

## Misc ##
# Clean up development artifacts
clean:
    rm -rf .pytest_cache
    rm -rf .ruff_cache
    rm -rf __pycache__
    rm -rf robosystems_service.egg-info
    find . -type d -name "__pycache__" -exec rm -rf {} +
    find . -type f -name "*.pyc" -delete

# Clean up development data (more aggressive cleanup)
clean-data:
    @just clean
    rm -rf ./data/output
    rm -rf ./data/kuzu-dbs

# Show help
help:
    @just --list
