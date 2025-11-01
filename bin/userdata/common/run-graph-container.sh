#!/bin/bash
# Universal Graph Database Container Runner
# Supports both Kuzu and Neo4j with shared infrastructure patterns

set -e

# Validate required environment variables
: ${DATABASE_TYPE:?"DATABASE_TYPE must be set (kuzu|neo4j)"}
: ${NODE_TYPE:?"NODE_TYPE must be set"}
: ${CONTAINER_PORT:?"CONTAINER_PORT must be set"}
: ${ECR_IMAGE:?"ECR_IMAGE must be set"}
: ${ENVIRONMENT:?"ENVIRONMENT must be set"}
: ${INSTANCE_ID:?"INSTANCE_ID must be set"}
: ${PRIVATE_IP:?"PRIVATE_IP must be set"}
: ${AVAILABILITY_ZONE:?"AVAILABILITY_ZONE must be set"}
: ${INSTANCE_TYPE:?"INSTANCE_TYPE must be set"}
: ${AWS_REGION:?"AWS_REGION must be set"}
: ${CLUSTER_TIER:?"CLUSTER_TIER must be set"}

# Optional variables with defaults
DATA_MOUNT_SOURCE="${DATA_MOUNT_SOURCE:-/mnt/${DATABASE_TYPE}-data/databases}"
DATA_MOUNT_TARGET="${DATA_MOUNT_TARGET:-/app/data}"
LOGS_MOUNT_SOURCE="${LOGS_MOUNT_SOURCE:-/mnt/${DATABASE_TYPE}-data/logs}"
LOGS_MOUNT_TARGET="${LOGS_MOUNT_TARGET:-/app/logs}"
STAGING_MOUNT_SOURCE="${STAGING_MOUNT_SOURCE:-}"
STAGING_MOUNT_TARGET="${STAGING_MOUNT_TARGET:-}"
DOCKER_PROFILE="${DOCKER_PROFILE:-${DATABASE_TYPE}-writer}"

# Determine container name - backend-agnostic
# Container name represents the service (graph-api), not the implementation (kuzu/neo4j)
# Backend type is determined by DOCKER_PROFILE and environment variables
determine_container_name() {
    if [ "${NODE_TYPE}" = "shared_master" ] || [ "${NODE_TYPE}" = "shared_replica" ]; then
        echo "graph-api-shared"
    else
        echo "graph-api"
    fi
}

CONTAINER_NAME=$(determine_container_name)

echo "=== Starting ${DATABASE_TYPE} Container ==="
echo "Container name: ${CONTAINER_NAME}"
echo "Node type: ${NODE_TYPE}"
echo "Port: ${CONTAINER_PORT}"

# Stop any existing container
echo "Stopping existing container if present..."
docker stop ${CONTAINER_NAME} 2>/dev/null || true
docker rm ${CONTAINER_NAME} 2>/dev/null || true

# Use unified log group for all Graph API instances
# The unified log group should be created by graph-infra CloudFormation stack
UNIFIED_LOG_GROUP="/robosystems/${ENVIRONMENT}/graph-api"
echo "Using unified log group: ${UNIFIED_LOG_GROUP}"

# Calculate memory limits dynamically based on available memory
# Leave 2GB for OS on instances with >8GB, 1GB for smaller instances
TOTAL_MEMORY_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
TOTAL_MEMORY_GB=$((TOTAL_MEMORY_KB / 1024 / 1024))

if [ ${TOTAL_MEMORY_GB} -gt 8 ]; then
  # Leave 2GB for OS on larger instances
  MEMORY_LIMIT="$((${TOTAL_MEMORY_GB} - 2))g"
  MEMORY_RESERVATION="$((${TOTAL_MEMORY_GB} - 3))g"
elif [ ${TOTAL_MEMORY_GB} -gt 4 ]; then
  # Leave 1GB for OS on medium instances
  MEMORY_LIMIT="$((${TOTAL_MEMORY_GB} - 1))g"
  MEMORY_RESERVATION="$((${TOTAL_MEMORY_GB} - 2))g"
else
  # Don't set limits on very small instances
  MEMORY_LIMIT=""
  MEMORY_RESERVATION=""
fi

echo "System has ${TOTAL_MEMORY_GB}GB total memory"

# Build memory flags if limits are set
MEMORY_FLAGS=""
if [ -n "${MEMORY_LIMIT}" ]; then
  MEMORY_FLAGS="--memory=${MEMORY_LIMIT} --memory-reservation=${MEMORY_RESERVATION}"
  echo "Setting container memory limits: ${MEMORY_LIMIT} (reservation: ${MEMORY_RESERVATION})"
fi

# Build database-specific environment variables
case "${DATABASE_TYPE}" in
    kuzu)
        EXTRA_ENV_VARS="-e KUZU_NODE_TYPE=${NODE_TYPE} \
            -e REPOSITORY_TYPE=${REPOSITORY_TYPE:-shared} \
            -e SHARED_REPOSITORIES=${SHARED_REPOSITORIES:-} \
            -e KUZU_DATABASE_PATH=${DATA_MOUNT_TARGET}/kuzu-dbs \
            -e KUZU_PORT=${CONTAINER_PORT} \
            -e KUZU_ROLE=$(if [ "${NODE_TYPE}" = "shared_replica" ]; then echo "replica"; else echo "master"; fi) \
            -e KUZU_ACCESS_PATTERN=api_writer"
        HEALTH_CMD="timeout 10 curl -f http://localhost:${CONTAINER_PORT}/health || exit 1"
        ;;
    neo4j)
        # Neo4j requires special environment variables
        EXTRA_ENV_VARS="-e NEO4J_AUTH=${NEO4J_AUTH:?"NEO4J_AUTH must be set for Neo4j"} \
            -e NEO4J_server_bolt_listen__address=0.0.0.0:7687 \
            -e NEO4J_server_http_listen__address=0.0.0.0:7474 \
            -e NEO4J_server_memory_heap_initial__size=512m \
            -e NEO4J_server_memory_heap_max__size=${MEMORY_LIMIT:-2g} \
            -e NEO4J_server_memory_pagecache_size=512m \
            -e NEO4J_dbms_connector_bolt_enabled=true \
            -e NEO4J_dbms_connector_http_enabled=true"
        HEALTH_CMD="timeout 10 curl -f http://localhost:7474 || exit 1"
        CONTAINER_PORT="7474"  # Override for Neo4j HTTP port
        ;;
    *)
        echo "ERROR: Unsupported DATABASE_TYPE: ${DATABASE_TYPE}"
        exit 1
        ;;
esac

# Build optional volume mounts
VOLUME_MOUNTS="-v ${DATA_MOUNT_SOURCE}:${DATA_MOUNT_TARGET} -v ${LOGS_MOUNT_SOURCE}:${LOGS_MOUNT_TARGET}"
if [ -n "${STAGING_MOUNT_SOURCE}" ] && [ -n "${STAGING_MOUNT_TARGET}" ]; then
  VOLUME_MOUNTS="${VOLUME_MOUNTS} -v ${STAGING_MOUNT_SOURCE}:${STAGING_MOUNT_TARGET}"
  echo "Adding staging volume mount: ${STAGING_MOUNT_SOURCE} -> ${STAGING_MOUNT_TARGET}"
fi

# Run the container with CloudWatch logging
echo "Starting ${DATABASE_TYPE} container..."
docker run -d \
  --name ${CONTAINER_NAME} \
  --restart unless-stopped \
  --health-cmd="${HEALTH_CMD}" \
  --health-interval=120s \
  --health-timeout=30s \
  --health-retries=20 \
  --health-start-period=300s \
  ${MEMORY_FLAGS} \
  --log-driver awslogs \
  --log-opt awslogs-region=${AWS_REGION} \
  --log-opt awslogs-group="${UNIFIED_LOG_GROUP}" \
  --log-opt awslogs-stream="${CLUSTER_TIER}/${INSTANCE_ID}/${NODE_TYPE}" \
  --log-opt awslogs-create-group=false \
  -p ${CONTAINER_PORT}:${CONTAINER_PORT} \
  ${VOLUME_MOUNTS} \
  -e ENVIRONMENT=${ENVIRONMENT} \
  -e INSTANCE_ID=${INSTANCE_ID} \
  -e INSTANCE_IP=${PRIVATE_IP} \
  -e INSTANCE_AZ=${AVAILABILITY_ZONE} \
  -e INSTANCE_TYPE=${INSTANCE_TYPE} \
  -e CLUSTER_TIER="${CLUSTER_TIER}" \
  -e AWS_REGION=${AWS_REGION} \
  -e LOG_LEVEL=INFO \
  -e OTEL_ENABLED=false \
  -e OTEL_SERVICE_NAME=${DATABASE_TYPE}-writer-${NODE_TYPE} \
  -e OTEL_EXPORTER_OTLP_ENDPOINT=http://172.17.0.1:4318 \
  -e DOCKER_PROFILE=${DOCKER_PROFILE} \
  -e GRAPH_BACKEND_TYPE=${DATABASE_TYPE} \
  -e DUCKDB_STAGING_PATH=${STAGING_MOUNT_TARGET:-/app/data/staging} \
  ${EXTRA_ENV_VARS} \
  ${ECR_IMAGE} \
  ${ENTRYPOINT_OVERRIDE:-/app/bin/entrypoint.sh} || {
    echo "ERROR: Failed to start Docker container"
    docker logs ${CONTAINER_NAME} || true
    exit 1
}

# Wait for container to be healthy
echo "Waiting for ${DATABASE_TYPE} container to become healthy..."
echo "Checking health on port ${CONTAINER_PORT}..."

# Shared writers may need more time to initialize
if [ "${NODE_TYPE}" = "shared_master" ] || [ "${NODE_TYPE}" = "shared_replica" ]; then
  echo "Shared writer detected, waiting additional time for initialization..."
  sleep 30
else
  sleep 10
fi

HEALTH_CHECK_PASSED=false
for i in {1..12}; do  # 12 * 5 = 60 seconds
  if curl -f http://localhost:${CONTAINER_PORT}/health >/dev/null 2>&1; then
    echo "${DATABASE_TYPE} container is healthy on port ${CONTAINER_PORT}"
    HEALTH_CHECK_PASSED=true
    break
  fi
  echo "Waiting for ${DATABASE_TYPE} container to start on port ${CONTAINER_PORT}... ($i/12)"
  # Check if container is still running
  if ! docker ps | grep -q ${CONTAINER_NAME}; then
    echo "ERROR: Container ${CONTAINER_NAME} is not running"
    docker logs ${CONTAINER_NAME} || true
    break
  fi
  sleep 5
done

if [ "${HEALTH_CHECK_PASSED}" != "true" ]; then
  echo "ERROR: ${DATABASE_TYPE} container failed to become healthy after 60 seconds"
  echo "Container logs:"
  docker logs ${CONTAINER_NAME} || true
  exit 1
fi

echo "✅ ${DATABASE_TYPE} container started successfully"
exit 0
