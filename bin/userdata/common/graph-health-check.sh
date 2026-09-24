#!/bin/bash
# Graph Database Health Check
# Container-state health checks for LadybugDB graph API instances.

set -e

# Validate required environment variables
: ${DATABASE_TYPE:?"DATABASE_TYPE must be set (ladybug)"}
: ${NODE_TYPE:?"NODE_TYPE must be set"}
: ${CONTAINER_PORT:?"CONTAINER_PORT must be set"}
: ${ENVIRONMENT:?"ENVIRONMENT must be set"}
: ${REGISTRY_TABLE:?"REGISTRY_TABLE must be set"}
: ${AWS_REGION:?"AWS_REGION must be set"}

# Get instance metadata
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)

# Validate the runtime type
if [ "${DATABASE_TYPE}" != "ladybug" ]; then
  echo "ERROR: Unsupported DATABASE_TYPE: ${DATABASE_TYPE}"
  exit 1
fi

# run-graph-container.sh owns the container name mapping. Without it this check
# can neither identify nor restart the container, so fail rather than guess.
CONTAINER_NAME=$(/usr/local/bin/run-graph-container.sh --print-container-name) || {
  echo "[$(date)] ERROR: could not determine container name from run-graph-container.sh"
  exit 1
}

# Check container status
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
