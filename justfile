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
#   just upgrade           # Fetch the latest images / rebuild, recreate what changed
#   just restart           # After code changes (no rebuild)
#   just test              # Run tests
#   just logs api          # View API logs
#
# =============================================================================

_env := ".env"
_local_env := ".env.local"

# Default recipe (runs when `just` is invoked with no args) — lists all recipes
default:
    @just --list


## Docker ##

# Start service
start profile="robosystems" build="":
    @test -f {{_env}} || cp .env.example {{_env}}
    @test -f {{_local_env}} || cp .env.local.example {{_local_env}}
    @just install-hooks
    docker compose -f compose.yaml --env-file {{_env}} --profile {{profile}} up \
        {{ if build != "" { "--build" } else { "" } }} --detach

# Stop containers without removing them (restart with `just start`)
stop profile="robosystems":
    docker compose -f compose.yaml --profile {{profile}} stop

# Tear down and remove containers entirely
teardown profile="robosystems":
    docker compose -f compose.yaml --profile {{profile}} down

# Fetch latest images (or rebuild) and recreate what changed
upgrade profile="robosystems" scope="":
    @bin/tools/upgrade.sh {{profile}} {{scope}}

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

# Restart graph-api if running and wait until it is healthy
graph-api-restart:
    @[ -z "$(docker ps --filter name=^robosystems-graph-api$ -q)" ] || docker compose -f compose.yaml restart graph-api
    @[ -z "$(docker ps --filter name=^robosystems-graph-api$ -q)" ] || (curl -fs --retry 30 --retry-delay 2 --retry-all-errors -o /dev/null http://localhost:8001/health && echo "graph-api healthy")

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
    @just install-hooks
    @just venv

# Install git hooks (points core.hooksPath at .githooks; idempotent, safe to re-run)
install-hooks:
    git config core.hooksPath .githooks

# Create virtual environment (assumes uv is installed)
venv:
    uv venv
    @just install

# Install dependencies from lock file
install:
    uv sync --all-extras --dev

# Update dependencies and regenerate lock file
update:
    uv lock --upgrade
    uv sync --all-extras --dev

# Install local Python SDK (editable, overrides PyPI version; `just install` to restore)
sdk-local:
    uv pip install -e ../robosystems-python-client

# Update Python SDK
sdk-update:
    uv pip install --upgrade robosystems-client
    @just install


## Testing ##

# Run all tests (excludes slow tests)
test-all:
    @just test
    @just test-dbt quickbooks
    -@just lint fix
    @just lint
    @just format
    @just typecheck
    @just cf-lint-all
    @just lint-actions

# Run tests, excluding slow ones — optional module path
test module="":
    uv run pytest \
        {{ if module != "" { "tests/" + module } else { "" } }} \
        -n auto --dist loadfile \
        -m "not slow"

# Run ALL tests including slow ones
test-full:
    uv run pytest -n auto --dist loadfile

# Run tests with coverage
test-cov:
    uv run pytest -n auto --dist loadfile --cov=robosystems tests/

# Run the tenant-isolation harness against a deployment (default: local stack)
test-isolation target="http://localhost:8000":
    TARGET_API_URL={{ target }} uv run pytest tests/security/isolation -m isolation \
        -o addopts="-v -ra" -s

# Run dbt models and tests for an adapter
test-dbt adapter tmpdir=`mktemp -d`:
    DBT_DUCKDB_PATH="{{ tmpdir }}/{{ adapter }}.duckdb" uv run dbt build \
        --profiles-dir "robosystems/adapters/{{ adapter }}/dbt" \
        --project-dir "robosystems/adapters/{{ adapter }}/dbt" \
        --target-path "{{ tmpdir }}/target" \
        --vars '{"use_seeds": true}'

# Run code quality checks (auto-fix first, then verify)
test-code:
    -@just lint fix
    @just lint
    @just format
    @just typecheck
    @just cf-lint-all
    @just lint-actions

# Run linting
lint fix="":
    @uv run ruff check . {{ if fix != "" { "--fix --unsafe-fixes" } else { "" } }}

# Format code
format:
    @uv run ruff format .

# Run type checking
typecheck module="":
    @uv run basedpyright {{ if module != "" { "robosystems/" + module } else { "" } }}

# Lint + validate one CloudFormation template
cf-lint template:
    @uv run cfn-lint -t cloudformation/{{template}}.yaml
    @size=$(wc -c < cloudformation/{{template}}.yaml | tr -d ' '); \
    if [ "$size" -gt 51200 ]; then \
      echo "{{template}}.yaml is $size bytes (over the 51,200-byte --template-body limit); deploys from S3, skipping validate-template"; \
    else \
      uv run aws cloudformation validate-template --template-body file://cloudformation/{{template}}.yaml > /dev/null; \
    fi

# Lint all CloudFormation templates
cf-lint-all:
    @uv run cfn-lint -t cloudformation/*.yaml

# Lint GitHub Actions workflows and composite actions
lint-actions:
    @uv run actionlint -shellcheck=

# Same, with shellcheck integration on (noisy; not part of the gate)
lint-actions-shell:
    @uv run actionlint

# Validate the rs-gaap framework: structure, package integrity, CoA coverage
framework-validate *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.framework_validate {{args}}


## Demo Scripts ##

# Run all demos except SEC, in dependency order (long)
demo:
    @just demo-user
    @just demo-roboledger
    @just demo-coffee-roaster
    @just demo-saas-startup
    @just demo-roboinvestor
    @just demo-custom-graph
    @just demo-seattle-method
    @just demo-world-online

# Create or reuse demo user (uses shared .local/config.json)
demo-user *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m examples.credentials.main {{args}}

# Set up the SEC repository demo — load filings, subscribe, run queries
demo-sec *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m examples.sec_demo.main {{args}}

# Create the SEC subscription only, no data load
demo-sec-subscribe plan="sec-starter":
    UV_ENV_FILE={{_local_env}} uv run python -m examples.sec_demo.main --subscribe-only --plan {{plan}}

# Run SEC demo preset queries (pass any args: --all, --preset NAME, --search "query", --list)
demo-sec-query *args:
    UV_ENV_FILE={{_local_env}} uv run python -m examples.sec_demo.query_examples {{args}}

# Run the RoboLedger demo — Cascade Advisory, the full accounting arc
demo-roboledger *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m examples.roboledger_demo.main {{args}}

# Run the Coffee Roaster showcase — Driftline, profitable but cash-poor
demo-coffee-roaster *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m examples.coffee_roaster_demo.main {{args}}

# Run the SaaS Startup showcase — Cadence Labs, burn behind deferred revenue
demo-saas-startup *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m examples.saas_startup_demo.main {{args}}

# Run the RoboInvestor demo — Meridian fund, incl. the cross-graph report share
demo-roboinvestor *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m examples.roboinvestor_demo.main {{args}}

# Run the custom graph demo end-to-end — your own schema
demo-custom-graph *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m examples.custom_graph_demo.main {{args}}

# Run the Seattle Method cross-taxonomy demo — Charlie Hoffman's mini, 14 JEs
demo-seattle-method *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m examples.seattle_method_demo.main {{args}}

# Run The World Online demo — Seattle Method at scale, 22,288 GL lines
demo-world-online *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m examples.seattle_method_world_online.main {{args}}


## CI/CD ##

# Create a feature branch
create-feature type="feature" name="" base="main" update="no":
    @bin/tools/create-feature.sh {{type}} {{name}} {{base}} {{update}}

# Create a release branch from main with deployment option
create-release version="patch" deploy="staging":
    @bin/tools/create-release.sh {{version}} {{deploy}}

# Deploy current branch/tag to specified environment
deploy environment="prod" ref="":
    @bin/tools/deploy.sh {{environment}} {{ref}}

# SSM tunnel to private resources (via bastion host, no SSH keys required)
tunnel environment service="all":
    @bin/tools/tunnels.sh {{environment}} {{service}}


## Bootstrap ##

# Bootstrap AWS OIDC federation for GitHub Actions
bootstrap profile="robosystems-sso" region="us-east-1":
    @bin/setup/bootstrap.sh "{{profile}}" "{{region}}"

# Re-apply the GitHub OIDC stack only — previews the change set, asks before applying
bootstrap-oidc profile="robosystems-sso" region="us-east-1":
    @bin/setup/bootstrap.sh --oidc "{{profile}}" "{{region}}"

# AWS setup (Secrets Manager + SSM Parameter Store)
setup-aws:
    @bin/setup/aws.sh

# GitHub Repository setup
setup-gha:
    @bin/setup/gha.sh

# Bedrock local dev setup — creates IAM user, updates .env; pass "rotate" to rotate the key
setup-bedrock *args:
    @bin/setup/bedrock.sh {{ args }}

# Reconcile the ECR image lifecycle policy from bin/setup/ecr-lifecycle-policy.json
bootstrap-ecr-lifecycle repo="robosystems" region="us-east-1":
    @echo "Applying ECR lifecycle policy to {{repo}} ({{region}})..."
    @aws ecr put-lifecycle-policy \
        --repository-name "{{repo}}" \
        --region "{{region}}" \
        --lifecycle-policy-text "file://bin/setup/ecr-lifecycle-policy.json" \
        --query 'repositoryName' --output text

# Generate a secure random key for a single secret
generate-key:
    @echo "Generated secure 32-byte base64 key:"
    @openssl rand -base64 32

# Generate secure random keys for all secrets
generate-keys:
    @echo "CONNECTION_CREDENTIALS_KEY=$(openssl rand -base64 32)"
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
    @gh variable delete {{name}}
    @echo "Deleted {{name}}"

# List GitHub organization variables (optionally filter by pattern)
gha-list-org filter="":
    @gh variable list --org $(gh repo view --json owner -q .owner.login) {{ if filter != "" { "| grep -i " + filter } else { "" } }}


## Admin CLI ##

# Admin CLI via the admin API — dev | staging | prod (staging/prod need a tunnel)
admin environment="dev" *args="":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.admin.cli -e {{environment}} {{args}}


## Database Operations ##
# Usage: just migrate-up [db] — db is "platform" (default) or "extensions"

# Create new migration
migrate-create message db="platform":
    UV_ENV_FILE={{_local_env}} uv run alembic -c migrations/{{db}}.ini revision --autogenerate -m "{{message}}"

# Run migrations
migrate-up db="platform":
    UV_ENV_FILE={{_local_env}} uv run alembic -c migrations/{{db}}.ini upgrade head

# Rollback migration
migrate-down db="platform":
    UV_ENV_FILE={{_local_env}} uv run alembic -c migrations/{{db}}.ini downgrade -1

# Show migration history
migrate-history db="platform":
    UV_ENV_FILE={{_local_env}} uv run alembic -c migrations/{{db}}.ini history

# Show current migration
migrate-current db="platform":
    UV_ENV_FILE={{_local_env}} uv run alembic -c migrations/{{db}}.ini current

# Reset database
migrate-reset db="platform":
    UV_ENV_FILE={{_local_env}} uv run alembic -c migrations/{{db}}.ini downgrade base
    UV_ENV_FILE={{_local_env}} uv run alembic -c migrations/{{db}}.ini upgrade head


## Graph API ##

# Graph API - health check
graph-health url="http://localhost:8001":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.graph_query \
        --url {{url}} \
        --command health

# Graph API - get database info
graph-info graph_id url="http://localhost:8001":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.graph_query \
        --url {{url}} \
        --graph-id {{graph_id}} \
        --command info

# Examples:
#   just graph-query sec "MATCH (e:Entity {ticker: 'AAPL'}) RETURN e.name"
#   just graph-query sec "MATCH (e:Entity) WHERE e.ticker IN ['AAPL', 'MSFT'] RETURN e.name"
# Graph API - execute Cypher query (single quotes auto-converted to double quotes for Cypher)
graph-query graph_id query format="table" url="http://localhost:8001":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.graph_query \
        --url {{url}} \
        --graph-id {{graph_id}} \
        --query "{{query}}" \
        --format {{format}}

# Graph API - execute SQL query on staging tables (DuckDB-based)
tables-query graph_id query format="table" url="http://localhost:8001":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.tables_query \
        --url {{url}} \
        --graph-id {{graph_id}} \
        --query "{{query}}" \
        --format {{format}}

# LadybugDB embedded database direct query (bypasses API)
lbug-query graph_id query format="table":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.lbug_query \
        --db-path ./data/lbug-dbs/{{graph_id}}.lbug \
        --query "{{query}}" \
        --format {{format}}

# DuckDB staging database direct query (bypasses API)
duckdb-query graph_id query format="table":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.duckdb_query \
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
#   just sec-dump                             # Pull the prebuilt public dump instead of running the pipeline

# --- Dump (the prebuilt corpus, no pipeline) ---

# Download the public SEC .lbug dump from Hugging Face (~128 GiB), restart graph-api
sec-dump *flags="":
    @just sec-dump-no-restart {{flags}}
    @just graph-api-restart

# Same download, without the graph-api restart
sec-dump-no-restart *flags="":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.sec_dump {{flags}}

# --- Full Pipeline (convenience) ---

# Full pipeline: download → process → materialize (top N companies by market cap)
sec-pipeline count="10" year="":
    @just sec-download {{count}} {{year}}
    @just sec-process
    @just sec-materialize

# Load single ticker end-to-end (download + process + materialize)
sec-load ticker year="":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.sec_pipeline run \
        --tickers {{ticker}} \
        {{ if year != "" { "--year " + year } else { "" } }}

# --- Phase 1: Download ---

# Download raw XBRL ZIPs to S3 (top N companies by market cap)
sec-download count="10" year="":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.sec_pipeline download \
        --count {{count}} \
        {{ if year != "" { "--year " + year } else { "" } }}

# --- Phase 2: Process ---

# Process pending SEC filings by quarter
sec-process reset_errors="":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.sec_pipeline process \
        {{ if reset_errors != "" { "--reset-errors" } else { "" } }}

# --- Phase 3: Materialize ---

# Materialize processed parquet files to graph (combined: staging + ingestion)
sec-materialize:
    @just sec-stage ""
    @just sec-materialize-graph

# Stage SEC filings to persistent DuckDB only (decoupled Stage 1)
sec-stage year="":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.sec_pipeline stage \
        --graph-id sec \
        {{ if year != "" { "--year " + year } else { "" } }}

# Materialize the graph from existing DuckDB staging (decoupled Stage 2)
sec-materialize-graph:
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.sec_pipeline materialize-graph \
        --graph-id sec

# --- Phase 4: Text Search Indexing ---

# Index text blocks + narratives into OpenSearch (partitioned by quarter)
sec-index quarter:
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.sec_pipeline index {{quarter}} \
        --graph-id sec

# --- Phase 5: Text Search Query ---

# Search OpenSearch for filing text content (semantic search on by default)
search graph_id query *flags:
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.search_query \
        --graph-id {{graph_id}} \
        --semantic \
        {{flags}} \
        "{{query}}"

# Show OpenSearch document count and breakdown
search-count graph_id="sec":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.search_query \
        --graph-id {{graph_id}} --count

# --- Utilities ---

# Reset SEC database and S3 data (use clear_s3="" to skip S3/SourceFiles cleanup)
sec-reset clear_s3="true":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.sec_pipeline reset \
        {{ if clear_s3 != "" { "--clear-s3" } else { "" } }}

# Validate SEC repository integrity
sec-health verbose="" json="" api_url="http://localhost:8001":
    UV_ENV_FILE={{_local_env}} uv run python -m robosystems.scripts.graph_health sec \
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
    rm -rf ./data/artifacts
    rm -rf ./data/lbug-dbs
    rm -rf ./data/staging
    rm -rf ./data/lance
    rm -rf ./data/localstack
    rm -rf ./data/opensearch
    rm -rf ./data/postgres
    rm -rf ./data/valkey
    rm -rf ./.local/config.json

# Full local reset — tear down, wipe local data, rebuild the stack
reset-local:
    @just teardown
    @just clean-data
    @just rebuild

# Show help
help:
    @just --list
