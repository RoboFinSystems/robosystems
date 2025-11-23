#!/bin/bash
set -euo pipefail

# Signal handling for graceful shutdown
trap 'echo "Received shutdown signal"; kill -TERM $PID 2>/dev/null || true; wait $PID; exit 0' SIGTERM SIGINT

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

# For local development - always run migrations and seeds automatically
if [[ "${ENVIRONMENT:-}" == "dev" && "${DOCKER_PROFILE:-}" == "worker"  && "${RUN_MIGRATIONS:-}" == "true" ]]; then
    run_db_init || echo "Database initialization failed, but continuing..."
fi

case $DOCKER_PROFILE in
  "api")
    echo "Starting API service..."
    exec uv run uvicorn main:app \
      --host 0.0.0.0 \
      --port 8000 \
      --access-log \
      --proxy-headers
    ;;
  "worker")
    # Run EDGAR init for development environment
    if [[ "${ENVIRONMENT:-}" == "dev" ]]; then
        uv run python /app/robosystems/scripts/arelle_cache_manager.py dev-init || echo "Warning: EDGAR init failed"
    fi

    QUEUES="${WORKER_QUEUE:-default}"

    # Special handling for specific queues
    if [[ "${QUEUES}" == "shared-extraction" ]]; then
      # Shared extraction workers for SEC XBRL downloads
      echo "Worker listening to queue: shared-extraction"
      # Force solo mode and no prefetch for extraction workers
      WORKER_AUTOSCALE=1
      WORKER_PREFETCH_MULTIPLIER=0
    elif [[ "${QUEUES}" == "shared-processing" ]]; then
      # Shared processing workers only handle shared-processing queue
      # shared-ingestion moved to dedicated workers for concurrency control
      echo "Worker listening to queue: shared-processing"
      # Force solo mode and no prefetch for shared-processing workers
      WORKER_AUTOSCALE=1
      WORKER_PREFETCH_MULTIPLIER=0
    elif [[ "${QUEUES}" == "shared-ingestion" ]]; then
      # Dedicated shared-ingestion workers with strict concurrency control
      echo "Worker listening to queue: shared-ingestion"
      # Force solo mode and no prefetch for ingestion workers
      WORKER_AUTOSCALE=1
      WORKER_PREFETCH_MULTIPLIER=0
    else
      echo "Worker listening to queues: ${QUEUES}"
    fi

    # Only embed beat scheduler in dev (no separate beat container)
    # In prod/staging, beat runs as separate ECS service
    if [[ "${ENVIRONMENT:-}" == "dev" ]]; then
      echo "Starting Celery worker with embedded Beat scheduler..."
      exec uv run celery -A robosystems.celery worker -B \
        --loglevel=info \
        --concurrency=${WORKER_AUTOSCALE:-1} \
        --prefetch-multiplier=${WORKER_PREFETCH_MULTIPLIER:-0} \
        -Q ${QUEUES} \
        --without-gossip \
        --without-heartbeat
    else
      echo "Starting Celery worker (beat scheduler runs separately)..."
      exec uv run celery -A robosystems.celery worker \
        --loglevel=info \
        --concurrency=${WORKER_AUTOSCALE:-1} \
        --prefetch-multiplier=${WORKER_PREFETCH_MULTIPLIER:-0} \
        -Q ${QUEUES} \
        --without-gossip \
        --without-heartbeat
    fi
    ;;
  "beat")
    echo "Starting Celery Beat scheduler..."

    # Run migrations on beat startup in staging/prod
    # Beat is singleton (DesiredCount: 1) so safe for migrations
    if [[ "${RUN_MIGRATIONS:-}" == "true" ]]; then
      run_db_init || echo "Database initialization failed, but continuing..."
    fi

    exec uv run celery -A robosystems.celery beat \
      --loglevel=info \
      -s /tmp/celerybeat-schedule \
      --pidfile=/tmp/celerybeat.pid
    ;;
  "lbug-writer")
    echo "Starting LadybugDB Writer API..."
    # max-databases will be loaded from tier configuration based on CLUSTER_TIER
    exec uv run python -m robosystems.graph_api \
      --node-type writer \
      --repository-type entity \
      --port ${LBUG_PORT:-8001} \
      --base-path ${LBUG_DATABASE_PATH:-/app/data/lbug-dbs}
    ;;
  "lbug-shared-writer")
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
