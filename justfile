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
start profile="robosystems" build="":
    @test -f {{_env}} || cp .env.example {{_env}}
    docker compose -f compose.yaml --env-file {{_env}} --profile {{profile}} up \
        {{ if build != "" { "--build" } else { "" } }} --detach

# Stop service
stop profile="robosystems":
    docker compose -f compose.yaml --profile {{profile}} down

# Rebuild containers (rebuilds images and force recreates - for package/env changes)
rebuild profile="robosystems":
    @test -f {{_env}} || cp .env.example {{_env}}
    docker compose -f compose.yaml --env-file {{_env}} --profile {{profile}} up \
        --build --force-recreate --detach

# Quick restart containers to pick up code changes via volume mounts (no rebuild)
restart profile="robosystems":
    docker compose -f compose.yaml --profile {{profile}} restart

# Restart specific service(s) without stopping everything
restart-container container="worker":
    docker compose -f compose.yaml restart robosystems-{{container}}

# Show running containers
ps:
    docker compose -f compose.yaml ps

# Docker logs (use follow=1 for tail -f style)
logs container="worker" lines="100" follow="":
    docker logs robosystems-{{container}} --tail {{lines}} \
        {{ if follow != "" { "--follow" } else { "" } }}

# Docker logs with grep filter
logs-grep container="worker" pattern="ERROR" lines="100":
    docker logs robosystems-{{container}} --tail {{lines}} | grep -E "{{pattern}}"

# Shell into a container
exec container="api" shell="bash":
    docker exec -it robosystems-{{container}} {{shell}}


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


## Testing ##

# Run all tests (excludes slow tests)
test-all:
    @just test
    @just lint fix
    @just lint
    @just format
    @just typecheck
    @just cf-lint-all

# Run tests (exclude integration and slow tests)
test module="":
    uv run pytest \
        {{ if module != "" { "tests/" + module } else { "" } }} \
        --ignore=tests/integration \
        -m "not slow"

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
    @just cf-lint-all

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

# Lint all CloudFormation templates
cf-lint-all:
    @uv run cfn-lint -t cloudformation/*.yaml


## CI/CD ##

# Create a feature branch
create-feature type="feature" name="" base="main" update="yes":
    @bin/tools/create-feature.sh {{type}} {{name}} {{base}} {{update}}

# Create a pull request
create-pr target="main" review="true":
    @bin/tools/create-pr.sh {{target}} {{review}}

# Create a release branch from main with deployment option
create-release version="patch" deploy="staging":
    @bin/tools/create-release.sh {{version}} {{deploy}}

# Deploy current branch/tag to specified environment
deploy environment="prod" ref="":
    @bin/tools/deploy.sh {{environment}} {{ref}}

# Bastion tunnel (uses AWS SSM - no SSH keys required)
bastion-tunnel environment service="all":
    @bin/tools/tunnels.sh {{environment}} {{service}}


## Bootstrap ##

# Bootstrap AWS OIDC federation for GitHub Actions
# Usage: just bootstrap [profile] [region]
#   profile: AWS SSO profile name (default: robosystems-sso)
#   region:  AWS region (default: us-east-1)
bootstrap profile="robosystems-sso" region="us-east-1":
    @bin/setup/bootstrap.sh "{{profile}}" "{{region}}"

# AWS setup (Secrets Manager + SSM Parameter Store)
setup-aws:
    @bin/setup/aws.sh

# GitHub Repository setup
setup-gha:
    @bin/setup/gha.sh

# Bedrock local development setup (creates IAM user, updates .env)
setup-bedrock:
    @bin/setup/bedrock.sh
# Generate a secure random key for a single secret
generate-key:
    @echo "Generated secure 32-byte base64 key:"
    @openssl rand -base64 32

# Generate secure random keys for all secrets
generate-keys:
    @echo "CONNECTION_CREDENTIALS_KEY=$(openssl rand -base64 32)"
    @echo "GRAPH_BACKUP_ENCRYPTION_KEY=$(openssl rand -base64 32)"
    @echo "JWT_SECRET_KEY=$(openssl rand -base64 32)"
    @echo "ADMIN_API_KEY=$(openssl rand -base64 32)"


## AWS SSM Parameter Store ##

# List SSM parameters by category (features, tuning)
ssm-list env category:
    @aws ssm get-parameters-by-path \
        --path "/robosystems/{{env}}/{{category}}" \
        --recursive \
        --query "Parameters[*].[Name,Value]" \
        --output table

# Get a single SSM parameter
ssm-get env path:
    @aws ssm get-parameter \
        --name "/robosystems/{{env}}/{{path}}" \
        --query "Parameter.Value" \
        --output text

# Set a single SSM parameter
ssm-set env path value:
    @aws ssm put-parameter \
        --name "/robosystems/{{env}}/{{path}}" \
        --value "{{value}}" \
        --type String \
        --overwrite
    @echo "Set /robosystems/{{env}}/{{path}} = {{value}}"

# Delete a single SSM parameter
ssm-delete env path:
    @aws ssm delete-parameter \
        --name "/robosystems/{{env}}/{{path}}"
    @echo "Deleted /robosystems/{{env}}/{{path}}"


## GitHub Actions Variables ##

# List all GitHub repository variables (optionally filter by pattern)
gha-list filter="":
    @gh variable list {{ if filter != "" { "| grep -i " + filter } else { "" } }}

# Get a single GitHub variable value
gha-get name:
    @gh variable get {{name}}

# Set a single GitHub variable
gha-set name value:
    @gh variable set {{name}} --body "{{value}}"
    @echo "Set {{name}} = {{value}}"

# Delete a single GitHub variable
gha-delete name:
    @gh variable delete {{name}} --yes
    @echo "Deleted {{name}}"


## Admin CLI ##

# Admin CLI for remote administration via admin API
# For staging/prod: start tunnel first with ./bin/tools/tunnels.sh <env> all
# Examples: just admin dev stats                    (local dev, no tunnel needed)
#           just admin prod stats                   (requires tunnel running)
#           just admin prod subscriptions list
#           just admin prod credits health
admin environment="dev" *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.admin.cli -e {{environment}} {{args}}


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
    uv run examples/sec_demo/main.py \
        --ticker {{ticker}} \
        --year {{year}} \
        {{ if skip_queries == "true" { "--skip-queries" } else { "" } }}

# Create SEC subscription only (no data loading) - for connecting to existing SEC graph
demo-sec-subscribe plan="sec-starter":
    uv run examples/sec_demo/main.py --subscribe-only --plan {{plan}}

# Run SEC demo preset queries
demo-sec-query all="false":
    uv run examples/sec_demo/query_examples.py {{ if all == "true" { "--all" } else { "" } }}

# Run accounting demo end-to-end (flags: new-user,new-graph,skip-queries)
demo-accounting flags="new-graph" real_s3="false" base_url="http://localhost:8000":
    uv run examples/accounting_demo/main.py \
        --base-url {{base_url}} \
        {{ if flags != "" { "--flags " + flags } else { "" } }} \
        {{ if real_s3 == "true" { "--real-s3" } else { "" } }}

# Run custom graph demo end-to-end (flags: new-user,new-graph,skip-queries)
demo-custom-graph flags="new-graph" real_s3="false" base_url="http://localhost:8000":
    uv run examples/custom_graph_demo/main.py \
        --base-url {{base_url}} \
        {{ if flags != "" { "--flags " + flags } else { "" } }} \
        {{ if real_s3 == "true" { "--real-s3" } else { "" } }}

# Run element mapping demo end-to-end (demonstrates CoA → US-GAAP aggregation)
demo-element-mapping flags="new-graph" real_s3="false" base_url="http://localhost:8000":
    uv run examples/element_mapping_demo/main.py \
        --base-url {{base_url}} \
        {{ if flags != "" { "--flags " + flags } else { "" } }} \
        {{ if real_s3 == "true" { "--real-s3" } else { "" } }}


## Graph API ##

# Graph API - health check
graph-health url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.graph_query \
        --url {{url}} \
        --command health

# Graph API - get database info
graph-info graph_id url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.graph_query \
        --url {{url}} \
        --graph-id {{graph_id}} \
        --command info

# Graph API - execute Cypher query (single quotes auto-converted to double quotes for Cypher)
# Examples:
#   just graph-query sec "MATCH (e:Entity {ticker: 'AAPL'}) RETURN e.name"
#   just graph-query sec "MATCH (e:Entity) WHERE e.ticker IN ['AAPL', 'MSFT'] RETURN e.name"
graph-query graph_id query format="table" url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.graph_query \
        --url {{url}} \
        --graph-id {{graph_id}} \
        --query "{{query}}" \
        --format {{format}}

# Graph API - execute SQL query on staging tables (DuckDB-based)
tables-query graph_id query format="table" url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.tables_query \
        --url {{url}} \
        --graph-id {{graph_id}} \
        --query "{{query}}" \
        --format {{format}}

# LadybugDB embedded database direct query (bypasses API)
lbug-query graph_id query format="table" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.lbug_query \
        --db-path ./data/lbug-dbs/{{graph_id}}.lbug \
        --query "{{query}}" \
        --format {{format}}

# DuckDB staging database direct query (bypasses API)
duckdb-query graph_id query format="table" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.duckdb_query \
        --db-path ./data/staging/{{graph_id}}.duckdb \
        --query "{{query}}" \
        --format {{format}}


## SEC Pipeline ##
# Examples:
#   just sec-load NVDA 2024
#   just sec-download 50 2024
#   just sec-process all=1                    # Process all pending files
#   just sec-process reset_errors=1           # Retry failed files
#   just sec-pipeline 50 2024

# --- Full Pipeline (convenience) ---

# Full pipeline: download → process → materialize (top N companies by market cap)
sec-pipeline count="10" year="":
    @just sec-download {{count}} {{year}}
    @just sec-process
    @just sec-materialize

# Load single ticker end-to-end (download + process + materialize)
sec-load ticker year="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_pipeline run \
        --tickers {{ticker}} \
        {{ if year != "" { "--year " + year } else { "" } }}

# --- Phase 1: Download ---

# Download raw XBRL ZIPs to S3 (top N companies by market cap)
sec-download count="10" year="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_pipeline download \
        --count {{count}} \
        {{ if year != "" { "--year " + year } else { "" } }}

# --- Phase 2: Process ---

# Process pending filings by quarter (sensor-driven in prod)
# In local dev, this triggers sec_process runs for each quarter with pending files.
# In prod, enable the sec_processing_sensor. Use --reset-errors to retry failed files.
sec-process reset_errors="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_pipeline process \
        {{ if reset_errors != "" { "--reset-errors" } else { "" } }}

# --- Phase 3: Materialize ---

# Materialize processed parquet files to graph (combined: staging + ingestion)
sec-materialize env=_local_env:
    @just sec-stage "" {{env}}
    @just sec-materialize-graph {{env}}

# Stage to persistent DuckDB only (decoupled Stage 1)
# Use this to save 2+ hours of work that persists if materialization fails
sec-stage year="" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_pipeline stage \
        --graph-id sec \
        {{ if year != "" { "--year " + year } else { "" } }}

# Materialize graph from existing DuckDB staging (decoupled Stage 2)
# Use this to retry materialization without re-staging
sec-materialize-graph env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_pipeline materialize-graph \
        --graph-id sec

# --- Utilities ---

# Reset SEC database and S3 data (use clear_s3="" to skip S3/SourceFiles cleanup)
sec-reset clear_s3="true" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.sec_pipeline reset \
        {{ if clear_s3 != "" { "--clear-s3" } else { "" } }}

# Validate SEC repository integrity
sec-health verbose="" json="" api_url="http://localhost:8001" env=_local_env:
    UV_ENV_FILE={{env}} uv run python -m robosystems.scripts.graph_health sec \
        --api-url {{api_url}} \
        {{ if verbose != "" { "--verbose" } else { "" } }} \
        {{ if json != "" { "--json" } else { "" } }}


## Misc ##

# Forward Stripe webhook events to local API
stripe-webhook url="http://localhost:8000":
    stripe listen --forward-to {{url}}/admin/v1/webhooks/stripe

# Clone frontend app repositories
clone-apps:
    @test -d ../robosystems-app || git clone https://github.com/RoboFinSystems/robosystems-app.git ../robosystems-app
    @test -d ../roboledger-app || git clone https://github.com/RoboFinSystems/roboledger-app.git ../roboledger-app
    @test -d ../roboinvestor-app || git clone https://github.com/RoboFinSystems/roboinvestor-app.git ../roboinvestor-app

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

reset-local:
    @just stop
    @just clean-data
    @just rebuild

# Show help
help:
    @just --list
