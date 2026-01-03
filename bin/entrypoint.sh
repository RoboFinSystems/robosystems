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

# Database initialization function
run_db_init() {
    echo "Running database initialization..."

    sleep 3
    echo "Running database migrations..."
    # Run migrations
    if uv run alembic upgrade head; then
      echo "✓ Migrations completed successfully"
    else
        echo "✗ Migration failed"
        return 1
    fi

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
  "neo4j-writer")
    echo "Starting Neo4j Graph API..."
    # Graph API with Neo4j backend
    # Backend type determined by GRAPH_BACKEND_TYPE env var (neo4j_community or neo4j_enterprise)
    # Note: base-path is for metadata only (actual data stored in Neo4j database via Bolt)
    exec uv run python -m robosystems.graph_api \
      --node-type writer \
      --repository-type entity \
      --port ${GRAPH_API_PORT:-8002} \
      --base-path /app/data/neo4j-metadata
    ;;
  *)
    echo "Unknown profile: $DOCKER_PROFILE"
    exit 1
    ;;
esac
