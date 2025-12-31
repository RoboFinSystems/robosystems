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

# Docker commands
compose-up profile="robosystems" build="--build" detached="--detach" env=_env:
    @test -f {{env}} || cp .env.example {{env}}
    docker compose -f compose.yaml --env-file {{env}} --profile {{profile}} up {{build}} {{detached}}

compose-down profile="robosystems":
    docker compose -f compose.yaml --profile {{profile}} down

# Rebuild containers (rebuilds images and restarts - for package/env changes)
rebuild profile="robosystems":
    @test -f {{_env}} || cp .env.example {{_env}}
    docker compose -f compose.yaml --env-file {{_env}} --profile {{profile}} up --build --force-recreate --detach

# Quick restart containers to pick up code changes via volume mounts (no rebuild)
restart profile="robosystems":
    docker compose -f compose.yaml --profile {{profile}} restart

# Restart specific service(s) without stopping everything
restart-container container="worker":
    docker compose -f compose.yaml restart robosystems-{{container}}

# Docker logs (without follow to prevent hanging)
logs container="worker" lines="100":
    docker logs robosystems-{{container}} --tail {{lines}}

# Docker logs with follow (tail -f style)
logs-follow container="worker":
    docker logs -f robosystems-{{container}}

# Docker logs with grep filter
logs-grep container="worker" pattern="ERROR" lines="100":
    docker logs robosystems-{{container}} --tail {{lines}} | grep -E "{{pattern}}"

# Clone frontend app repositories
clone-apps:
    @test -d ../robosystems-app || git clone https://github.com/RoboFinSystems/robosystems-app.git ../robosystems-app
    @test -d ../roboledger-app || git clone https://github.com/RoboFinSystems/roboledger-app.git ../roboledger-app
    @test -d ../roboinvestor-app || git clone https://github.com/RoboFinSystems/roboinvestor-app.git ../roboinvestor-app


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

# Run all demos
demo-all:
    @just demo-accounting
    @just demo-custom-graph
    @just demo-element-mapping
    @just demo-sec

# Create or reuse demo user (uses shared examples/credentials/config.json)
demo-user *args="":
    uv run examples/credentials/main.py {{args}}

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

# Run element mapping demo end-to-end (demonstrates CoA → US-GAAP aggregation)
demo-element-mapping flags="new-graph" base_url="http://localhost:8000":
    uv run examples/element_mapping_demo/main.py --base-url {{base_url}} {{ if flags != "" { "--flags " + flags } else { "" } }}


## Testing ##

# Run all tests (excludes slow tests)
test-all:
    @just test
    @just lint fix
    @just lint
    @just format
    @just typecheck

# Run tests (exclude integration and slow tests)
test module="":
    uv run pytest {{ if module != "" { "tests/" + module } else { "" } }} --ignore=tests/integration -m "not slow"

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
test-code:
    @just lint
    @just format
    @just typecheck

# Run linting
lint fix="":
    uv run ruff check . {{ if fix != "" { "--fix" } else { "" } }}

# Format code
format:
    uv run ruff format .

# Run type checking
typecheck module="":
    uv run basedpyright {{ if module != "" { "robosystems/" + module } else { "" } }}

# CloudFormation linting and validation
cf-lint template:
    @uv run cfn-lint -t cloudformation/{{template}}.yaml
    @uv run aws cloudformation validate-template --template-body file://cloudformation/{{template}}.yaml > /dev/null


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


## Admin CLI ##

# Admin CLI for remote administration via admin API
# Examples: just admin dev stats
#           just admin dev customers list
#           just admin dev subscriptions list --status active --tier ladybug-standard
admin environment="dev" *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.admin.cli -e {{environment}} {{args}}


## Development Server ##

# Start development server
api env=_local_env:
    UV_ENV_FILE={{env}} uv run uvicorn main:app --reload

# Start Graph API server with LadybugDB backend (configurable node type)
graph-api backend="ladybug" type="writer" port="8001" env=_local_env:
    UV_ENV_FILE={{env}} GRAPH_BACKEND_TYPE={{backend}} LBUG_NODE_TYPE={{type}} uv run python -m robosystems.graph_api --port {{port}}

stripe-webhook url="http://localhost:8000" env=_local_env:
    UV_ENV_FILE={{env}} uv run stripe listen --forward-to {{url}}/admin/v1/webhooks/stripe


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

# Reset database (drop and recreate all auth tables)
migrate-reset env=_local_env:
    UV_ENV_FILE={{env}} uv run alembic downgrade base
    UV_ENV_FILE={{env}} uv run alembic upgrade head


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

# LadybugDB embedded database direct query (bypasses API)
lbug-query graph_id query format="table" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.lbug_query --db-path ./data/lbug-dbs/{{graph_id}}.lbug --query "{{query}}" --format {{format}}

# DuckDB staging database direct query (bypasses API)
duckdb-query graph_id query format="table" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.duckdb_query --db-path ./data/staging/{{graph_id}}.duckdb --query "{{query}}" --format {{format}}

# Interactive query modes - launch REPL for each database type
graph-query-i graph_id url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.graph_query --url {{url}} --graph-id {{graph_id}}

tables-query-i graph_id url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.tables_query --url {{url}} --graph-id {{graph_id}}

lbug-query-i graph_id env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.lbug_query --db-path ./data/lbug-dbs/{{graph_id}}.lbug

duckdb-query-i graph_id env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.duckdb_query --db-path ./data/staging/{{graph_id}}.duckdb


## SEC Pipeline ##
# Examples:
#   just sec-load NVDA 2024
#   just sec-download 50 2024
#   just sec-process 2024
#   just sec-pipeline 50 2024

# Load single ticker end-to-end (download + process + materialize)
sec-load ticker year="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_pipeline run --tickers {{ticker}} {{ if year != "" { "--year " + year } else { "" } }}

# Download raw XBRL ZIPs to S3 (top N companies by market cap)
sec-download count="10" year="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_pipeline download --count {{count}} {{ if year != "" { "--year " + year } else { "" } }}

# Process downloaded filings to parquet (parallel)
sec-process year="" limit="" concurrency="2" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_pipeline process-parallel \
        {{ if year != "" { "--year " + year } else { "" } }} \
        {{ if limit != "" { "--limit " + limit } else { "" } }} \
        --concurrency {{concurrency}}

# Materialize processed parquet files to graph (ingests all available data)
sec-materialize env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_pipeline materialize

# Full pipeline: download → process → materialize (top N companies by market cap)
sec-pipeline count="10" year="2025" limit="":
    @just sec-download {{count}} {{year}}
    @just sec-process {{year}} {{limit}}
    @just sec-materialize

# Reset SEC database and optionally S3 data
sec-reset clear_s3="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_pipeline reset {{ if clear_s3 != "" { "--clear-s3" } else { "" } }}

# Validate SEC repository integrity
sec-health verbose="" json="" api_url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.graph_health sec --api-url {{api_url}} {{ if verbose != "" { "--verbose" } else { "" } }} {{ if json != "" { "--json" } else { "" } }}


## Setup ##

# AWS Secrets Manager setup
setup-aws:
    @bin/setup/aws.sh

# GitHub Repository setup
setup-gha:
    @bin/setup/gha.sh

# Generate secure random key for secrets
generate-key:
    @echo "Generated secure 32-byte base64 key:"
    @openssl rand -base64 32

# Generate multiple secure keys for all secrets
generate-keys:
    @echo "CONNECTION_CREDENTIALS_KEY=$(openssl rand -base64 32)"
    @echo "GRAPH_BACKUP_ENCRYPTION_KEY=$(openssl rand -base64 32)"
    @echo "JWT_SECRET_KEY=$(openssl rand -base64 32)"
    @echo "ADMIN_API_KEY=$(openssl rand -base64 32)"


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
    rm -rf ./data/lbug-dbs
    rm -rf ./data/staging
    rm -rf ./data/arelle
    rm -rf ./data/neo4j
    rm -rf ./data/localstack
    rm -rf ./data/postgres
    rm -rf ./data/valkey
    rm -f ./examples/credentials/config.json

# Show help
help:
    @just --list
