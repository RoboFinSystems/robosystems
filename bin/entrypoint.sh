#!/bin/bash
set -euo pipefail

# Signal handling for graceful shutdown
trap 'echo "Received shutdown signal"; kill -TERM $PID 2>/dev/null || true; wait $PID; exit 0' SIGTERM SIGINT

# ============================================================================
# Dagster Run Task Detection
# ============================================================================
# When EcsRunLauncher launches a run task, it overrides CMD with:
#   dagster api execute_run <args>
# We must detect this and execute the command instead of running a profile.
# This check must come BEFORE profile handling.
if [[ $# -gt 0 && "$1" == "dagster" ]]; then
    echo "Dagster run task detected, executing: $*"
    exec uv run "$@"
fi

# Default to API mode if not specified
DOCKER_PROFILE=${DOCKER_PROFILE:-api}
RUN_MIGRATIONS=${RUN_MIGRATIONS:-false}

# Validate common required environment variables
validate_env_vars() {
    local required_vars=("ENVIRONMENT")
    local missing_vars=()

    # Only ENVIRONMENT is required to know which secrets to fetch

    for var in "${required_vars[@]}"; do
        if [ -z "${!var:-}" ]; then
            missing_vars+=("$var")
        fi
    done

    if [ ${#missing_vars[@]} -ne 0 ]; then
        echo "Error: Missing required environment variables: ${missing_vars[*]}"
        echo "Please set all required environment variables and try again."
        exit 1
    fi
}

# Validate environment variables
validate_env_vars

# Ensure a PostgreSQL database exists, creating it if needed
ensure_database_exists() {
    local db_name="${1:?database name required}"
    local host="${DAGSTER_POSTGRES_HOST:-}"
    local port="${DAGSTER_POSTGRES_PORT:-5432}"
    local user="${DAGSTER_POSTGRES_USER:-postgres}"
    local password="${DAGSTER_POSTGRES_PASSWORD:-}"

    if [[ -z "$host" || -z "$password" ]]; then
        echo "Skipping database check for '$db_name' - missing connection details"
        return 0
    fi

    local sslmode="require"
    if [[ "${ENVIRONMENT:-}" == "dev" ]]; then
        sslmode="disable"
    fi

    echo "Ensuring database '$db_name' exists..."
    if PGPASSWORD="$password" psql \
        "host=$host port=$port user=$user dbname=postgres sslmode=$sslmode" \
        -tc "SELECT 1 FROM pg_database WHERE datname = '$db_name'" | grep -q 1; then
        echo "✓ Database '$db_name' already exists"
    else
        PGPASSWORD="$password" psql \
            "host=$host port=$port user=$user dbname=postgres sslmode=$sslmode" \
            -c "CREATE DATABASE $db_name" && echo "✓ Database '$db_name' created" || {
            echo "✗ Failed to create database '$db_name'"
            return 1
        }
    fi
}

# Resolve a feature flag from SSM Parameter Store (staging/prod only).
# Falls back to the environment variable, then to the provided default.
resolve_feature_flag() {
    local flag_name="${1:?flag name required}"
    local default_value="${2:-false}"

    # Use env var if already set (local dev, CI, explicit override)
    local env_value="${!flag_name:-}"
    if [[ -n "$env_value" ]]; then
        echo "$env_value"
        return
    fi

    # In staging/prod, read from SSM Parameter Store via boto3.
    # The runtime image does not ship the awscli binary, but boto3 is in the
    # application venv, so we shell out to Python instead.
    if [[ "${ENVIRONMENT:-dev}" != "dev" ]]; then
        local ssm_path="/robosystems/${ENVIRONMENT}/features/${flag_name}"
        local ssm_value
        ssm_value=$(python -c "
import os, sys, boto3
from botocore.exceptions import ClientError
try:
    client = boto3.client('ssm', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
    print(client.get_parameter(Name='${ssm_path}')['Parameter']['Value'], end='')
except ClientError:
    sys.exit(0)
" 2>/dev/null || echo "")
        if [[ -n "$ssm_value" ]]; then
            echo "$ssm_value"
            return
        fi
    fi

    echo "$default_value"
}

# Database initialization function
run_db_init() {
    echo "Running database initialization..."

    sleep 3
    echo "Running database migrations..."
    # Run platform migrations (always required)
    if ! uv run alembic -c migrations/platform.ini upgrade head; then
        echo "✗ Platform migration failed"
        return 1
    fi

    # Run extensions migrations if either product domain is enabled.
    # EXTENSIONS_ENABLED is a derived Python property (true when either
    # ROBOLEDGER_ENABLED or ROBOINVESTOR_ENABLED is true), so the shell
    # must check the underlying domain flags directly.
    local roboledger_enabled roboinvestor_enabled
    roboledger_enabled=$(resolve_feature_flag "ROBOLEDGER_ENABLED" "false")
    roboinvestor_enabled=$(resolve_feature_flag "ROBOINVESTOR_ENABLED" "false")
    if [[ "$roboledger_enabled" == "true" || "$roboinvestor_enabled" == "true" ]]; then
        echo "Extensions enabled — running extensions migrations..."
        # Alembic cannot CREATE DATABASE; ensure the extensions DB exists first.
        # Locally this is handled by docker/postgres-init.sh; in staging/prod
        # nothing else creates it on RDS.
        ensure_database_exists "extensions" || {
            echo "✗ Failed to ensure extensions database exists"
            return 1
        }
        if ! uv run alembic -c migrations/extensions.ini upgrade head; then
            echo "✗ Extensions migration failed"
            return 1
        fi
    else
        echo "Extensions disabled — skipping extensions migrations"
    fi

    echo "✓ Migrations completed successfully"

    echo "Database initialization complete"
}

# For local development - wait for LocalStack to be ready
if [[ "${ENVIRONMENT:-}" == "dev" ]]; then
    echo "Development environment detected - waiting for LocalStack to initialize..."
    sleep 5
fi

# Configure Dagster based on environment
configure_dagster() {
    DAGSTER_HOME=${DAGSTER_HOME:-/app/dagster_home}

    # For production/staging, use the production config with EcsRunLauncher
    if [[ "${ENVIRONMENT:-}" == "prod" || "${ENVIRONMENT:-}" == "staging" ]]; then
        if [[ -f "${DAGSTER_HOME}/dagster_prod.yaml" ]]; then
            echo "Using production Dagster configuration (EcsRunLauncher)"
            cp "${DAGSTER_HOME}/dagster_prod.yaml" "${DAGSTER_HOME}/dagster.yaml"
        fi
    else
        echo "Using development Dagster configuration (DefaultRunLauncher)"
        # Dev config is already the default dagster.yaml
    fi
}

case $DOCKER_PROFILE in
  "api")
    echo "Starting API service..."
    exec uv run uvicorn main:app \
      --host 0.0.0.0 \
      --port 8000 \
      --loop uvloop \
      --http httptools \
      --access-log \
      --proxy-headers
    ;;
  "dagster")
    echo "Starting Dagster webserver..."
    configure_dagster
    exec uv run dagster-webserver \
      -h 0.0.0.0 \
      -p ${DAGSTER_PORT:-3000} \
      -m robosystems.dagster
    ;;
  "dagster-daemon")
    echo "Starting Dagster daemon..."
    configure_dagster

    # Run migrations on daemon startup in staging/prod
    # Daemon is singleton (DesiredCount: 1) so safe for migrations
    # This mirrors the previous beat scheduler behavior
    if [[ "${RUN_MIGRATIONS:-}" == "true" ]]; then
      ensure_database_exists "${DAGSTER_POSTGRES_DB:-dagster}" || echo "Dagster database check failed, but continuing..."
      run_db_init || echo "Database initialization failed, but continuing..."
    fi

    exec uv run dagster-daemon run \
      -m robosystems.dagster
    ;;
  "ladybug-writer")
    echo "Starting LadybugDB Writer API..."
    # max-databases will be loaded from tier configuration based on CLUSTER_TIER
    exec uv run python -m robosystems.graph_api \
      --node-type writer \
      --repository-type entity \
      --port ${LBUG_PORT:-8001} \
      --base-path ${LBUG_DATABASE_PATH:-/app/data/lbug-dbs}
    ;;
  "ladybug-shared-writer")
    # Determine if this is a master or replica based on LBUG_ROLE
    if [[ "${LBUG_ROLE:-master}" == "replica" ]]; then
      echo "Starting LadybugDB Shared Replica API..."
      LBUG_NODE_TYPE="shared_replica"
      READONLY_FLAG="--read-only"
    else
      echo "Starting LadybugDB Shared Master API..."
      LBUG_NODE_TYPE="shared_master"
      READONLY_FLAG=""
    fi
    # max-databases will be loaded from tier configuration based on CLUSTER_TIER
    exec uv run python -m robosystems.graph_api \
      --node-type ${LBUG_NODE_TYPE} \
      --repository-type shared \
      --port ${LBUG_PORT:-8002} \
      --base-path ${LBUG_DATABASE_PATH:-/app/data/lbug-dbs} \
      ${READONLY_FLAG}
    ;;
  "worker")
    echo "Starting Background Worker..."
    exec uv run python -m robosystems.worker
    ;;
  *)
    echo "Unknown profile: $DOCKER_PROFILE"
    exit 1
    ;;
esac
