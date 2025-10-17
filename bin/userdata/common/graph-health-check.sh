#!/bin/bash
# Universal Graph Database Health Check
# Supports both Kuzu and Neo4j with ingestion-aware checking

set -e

# Validate required environment variables
: ${DATABASE_TYPE:?"DATABASE_TYPE must be set (kuzu|neo4j)"}
: ${NODE_TYPE:?"NODE_TYPE must be set"}
: ${CONTAINER_PORT:?"CONTAINER_PORT must be set"}
: ${ENVIRONMENT:?"ENVIRONMENT must be set"}
: ${REGISTRY_TABLE:?"REGISTRY_TABLE must be set"}
: ${AWS_REGION:?"AWS_REGION must be set"}
: ${VALKEY_URL:-}

# Get instance metadata
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)

# Determine container name based on database type and node type
case "${DATABASE_TYPE}" in
    kuzu)
        if [ "${NODE_TYPE}" = "shared_master" ] || [ "${NODE_TYPE}" = "shared_replica" ]; then
            CONTAINER_NAME="kuzu-shared-writer"
        else
            CONTAINER_NAME="kuzu-writer"
        fi
        ;;
    neo4j)
        if [ "${NODE_TYPE}" = "shared_master" ] || [ "${NODE_TYPE}" = "shared_replica" ]; then
            CONTAINER_NAME="neo4j-shared-writer"
        else
            CONTAINER_NAME="neo4j-writer"
        fi
        ;;
    *)
        echo "ERROR: Unsupported DATABASE_TYPE: ${DATABASE_TYPE}"
        exit 1
        ;;
esac

# Check if active ingestion is happening (via Redis flag)
# If ingestion is active, ALWAYS mark as healthy regardless of other checks
INGESTION_ACTIVE="false"
if [ -n "${VALKEY_URL}" ] && command -v redis6-cli &> /dev/null; then
  REDIS_HOST=$(echo $VALKEY_URL | sed 's|redis://||' | cut -d: -f1)
  REDIS_PORT=$(echo $VALKEY_URL | sed 's|redis://||' | cut -d: -f2 | cut -d/ -f1)

  INGESTION_FLAG=$(redis6-cli -h $REDIS_HOST -p $REDIS_PORT GET "${DATABASE_TYPE}:ingestion:active:${INSTANCE_ID}" 2>/dev/null || echo "")
  if [ -n "$INGESTION_FLAG" ]; then
    INGESTION_ACTIVE="true"
    # Extract table name if possible
    TABLE_NAME=$(echo "$INGESTION_FLAG" | grep -o '"table_name":"[^"]*"' | cut -d'"' -f4 || echo "unknown")
    echo "[$(date)] Active ingestion detected for table: $TABLE_NAME - marking as healthy"
    HEALTH_STATUS="healthy"
  fi
fi

# Only check container status if not actively ingesting
if [ "$INGESTION_ACTIVE" = "false" ]; then
  if docker ps | grep -q $CONTAINER_NAME; then
    HEALTH_STATUS="healthy"
    echo "[$(date)] Container $CONTAINER_NAME is running - marking as healthy"
  else
    HEALTH_STATUS="unhealthy"
    echo "[$(date)] Container $CONTAINER_NAME is NOT running - marking as unhealthy"
    # Try to restart container once
    echo "[$(date)] Attempting to restart container..."
    if [ -f /usr/local/bin/run-graph-container.sh ]; then
      /usr/local/bin/run-graph-container.sh
    else
      echo "[$(date)] ERROR: run-graph-container.sh not found, cannot restart"
    fi
  fi
fi

# Update DynamoDB with current status
aws dynamodb update-item \
  --table-name ${REGISTRY_TABLE} \
  --key "{\"instance_id\": {\"S\": \"${INSTANCE_ID}\"}}" \
  --update-expression "SET #status = :status, last_health_check = :time" \
  --expression-attribute-names '{"#status": "status"}' \
  --expression-attribute-values "{\":status\": {\"S\": \"${HEALTH_STATUS}\"}, \":time\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}" \
  --region ${AWS_REGION} >/dev/null 2>&1

echo "[$(date)] Health check complete: ${HEALTH_STATUS}"
exit 0
