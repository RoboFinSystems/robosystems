#!/bin/bash
# Graph Database Instance Registration in DynamoDB
# Registers LadybugDB graph instances with runtime metadata

set -e

# Validate required environment variables
: ${DATABASE_TYPE:?"DATABASE_TYPE must be set (ladybug)"}
: ${NODE_TYPE:?"NODE_TYPE must be set"}
: ${ENVIRONMENT:?"ENVIRONMENT must be set"}
: ${INSTANCE_ID:?"INSTANCE_ID must be set"}
: ${PRIVATE_IP:?"PRIVATE_IP must be set"}
: ${AVAILABILITY_ZONE:?"AVAILABILITY_ZONE must be set"}
: ${INSTANCE_TYPE:?"INSTANCE_TYPE must be set"}
: ${CLUSTER_TIER:?"CLUSTER_TIER must be set"}
: ${CONTAINER_PORT:?"CONTAINER_PORT must be set"}
: ${AWS_REGION:?"AWS_REGION must be set"}
: ${AWS_STACK_NAME:?"AWS_STACK_NAME must be set"}

# Optional variables with defaults
REGISTRY_TABLE="${REGISTRY_TABLE:-robosystems-graph-${ENVIRONMENT}-instance-registry}"
GRAPH_REGISTRY_TABLE="${GRAPH_REGISTRY_TABLE:-robosystems-graph-${ENVIRONMENT}-graph-registry}"
MAX_DATABASES="${MAX_DATABASES:-1}"

if [ "${DATABASE_TYPE}" != "ladybug" ]; then
  echo "ERROR: Unsupported DATABASE_TYPE: ${DATABASE_TYPE}"
  exit 1
fi

# Get VPC ID
VPC_ID=$(curl -s http://169.254.169.254/latest/meta-data/network/interfaces/macs/$(curl -s http://169.254.169.254/latest/meta-data/mac)/vpc-id)

# Get ASG name for cluster grouping
ASG_NAME=$(aws ec2 describe-instances \
  --instance-ids ${INSTANCE_ID} \
  --query 'Reservations[0].Instances[0].Tags[?Key==`aws:autoscaling:groupName`].Value' \
  --output text --region ${AWS_REGION} || echo "unknown")

echo "=== Registering graph instance in DynamoDB ==="
echo "Instance ID: ${INSTANCE_ID}"
echo "Node type: ${NODE_TYPE}"
echo "Runtime type: ${DATABASE_TYPE}"
echo "Cluster tier: ${CLUSTER_TIER}"

# Build the base item JSON
TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

if [ "${NODE_TYPE}" = "writer" ]; then
  # User database writer registration
  echo "Registering user writer instance..."

  # A replacement instance adopts the databases already on its reattached
  # volume, so registering with a hardcoded count of 0 makes the allocator
  # believe an occupied dedicated writer has free capacity and double-book
  # it. The volume-manager Lambda response written during storage setup
  # carries the authoritative database list; count parent graphs from it
  # (subgraph IDs contain "_" and share the parent's capacity slot).
  DB_COUNT=0
  if [ -f /tmp/volume-response.json ]; then
    DB_COUNT=$(jq -r '[.databases[]? | select(contains("_") | not)] | length' \
      /tmp/volume-response.json 2>/dev/null || echo "0")
    case "${DB_COUNT}" in
      ''|*[!0-9]*) DB_COUNT=0 ;;
    esac
  fi
  echo "Registering with database_count=${DB_COUNT} (from volume-manager response)"

  aws dynamodb put-item \
    --table-name "$REGISTRY_TABLE" \
    --item "{
      \"instance_id\": {\"S\": \"${INSTANCE_ID}\"},
      \"private_ip\": {\"S\": \"${PRIVATE_IP}\"},
      \"availability_zone\": {\"S\": \"${AVAILABILITY_ZONE}\"},
      \"instance_type\": {\"S\": \"${INSTANCE_TYPE}\"},
      \"node_type\": {\"S\": \"${NODE_TYPE}\"},
      \"backend_type\": {\"S\": \"${DATABASE_TYPE}\"},
      \"status\": {\"S\": \"initializing\"},
      \"created_at\": {\"S\": \"${TIMESTAMP}\"},
      \"last_health_check\": {\"S\": \"${TIMESTAMP}\"},
      \"database_count\": {\"N\": \"${DB_COUNT}\"},
      \"max_databases\": {\"N\": \"${MAX_DATABASES}\"},
      \"tier\": {\"S\": \"${CLUSTER_TIER}\"},
      \"region\": {\"S\": \"${AWS_REGION}\"},
      \"cluster_tier\": {\"S\": \"${CLUSTER_TIER}\"},
      \"cluster_group\": {\"S\": \"${AWS_REGION}-writers-${ASG_NAME}\"},
      \"available_capacity_pct\": {\"N\": \"100\"},
      \"private_dns\": {\"S\": \"$(hostname -f)\"},
      \"endpoint_url\": {\"S\": \"http://${PRIVATE_IP}:${CONTAINER_PORT}\"},
      \"launch_time\": {\"S\": \"${TIMESTAMP}\"},
      \"replication_role\": {\"S\": \"none\"},
      \"total_size_gb\": {\"N\": \"0\"},
      \"cpu_utilization\": {\"N\": \"0\"},
      \"memory_utilization\": {\"N\": \"0\"},
      \"iops_utilization\": {\"N\": \"0\"},
      \"vpc_id\": {\"S\": \"${VPC_ID}\"},
      \"stack_name\": {\"S\": \"${AWS_STACK_NAME}\"}
    }" \
    --region ${AWS_REGION}

elif [ "${NODE_TYPE}" = "shared_master" ] || [ "${NODE_TYPE}" = "shared_replica" ]; then
  # Shared writer registration
  # Count number of repositories for shared writers
  if [ -n "${SHARED_REPOSITORIES}" ]; then
    REPO_COUNT=$(echo ${SHARED_REPOSITORIES} | tr ',' '\n' | grep -c .)
  else
    REPO_COUNT=1
  fi

  echo "Registering shared writer instance with ${REPO_COUNT} repositories..."
  aws dynamodb put-item \
    --table-name "$REGISTRY_TABLE" \
    --item "{
      \"instance_id\": {\"S\": \"${INSTANCE_ID}\"},
      \"private_ip\": {\"S\": \"${PRIVATE_IP}\"},
      \"availability_zone\": {\"S\": \"${AVAILABILITY_ZONE}\"},
      \"instance_type\": {\"S\": \"${INSTANCE_TYPE}\"},
      \"node_type\": {\"S\": \"${NODE_TYPE}\"},
      \"backend_type\": {\"S\": \"${DATABASE_TYPE}\"},
      \"repository_type\": {\"S\": \"${REPOSITORY_TYPE:-shared}\"},
      \"shared_repositories\": {\"S\": \"${SHARED_REPOSITORIES:-}\"},
      \"status\": {\"S\": \"initializing\"},
      \"created_at\": {\"S\": \"${TIMESTAMP}\"},
      \"last_health_check\": {\"S\": \"${TIMESTAMP}\"},
      \"database_count\": {\"N\": \"${REPO_COUNT}\"},
      \"max_databases\": {\"N\": \"${REPO_COUNT}\"},
      \"tier\": {\"S\": \"${CLUSTER_TIER}\"},
      \"region\": {\"S\": \"${AWS_REGION}\"},
      \"cluster_tier\": {\"S\": \"${CLUSTER_TIER}\"},
      \"cluster_group\": {\"S\": \"${AWS_REGION}-shared-writers\"},
      \"available_capacity_pct\": {\"N\": \"0\"},
      \"private_dns\": {\"S\": \"$(hostname -f)\"},
      \"endpoint_url\": {\"S\": \"http://${PRIVATE_IP}:${CONTAINER_PORT}\"},
      \"launch_time\": {\"S\": \"${TIMESTAMP}\"},
      \"replication_role\": {\"S\": \"none\"},
      \"total_size_gb\": {\"N\": \"0\"},
      \"cpu_utilization\": {\"N\": \"0\"},
      \"memory_utilization\": {\"N\": \"0\"},
      \"iops_utilization\": {\"N\": \"0\"},
      \"vpc_id\": {\"S\": \"${VPC_ID}\"},
      \"stack_name\": {\"S\": \"${AWS_STACK_NAME}\"}
    }" \
    --region ${AWS_REGION}

  # Register shared repositories in graph registry if applicable
  # IMPORTANT: Only shared_master registers in graph-registry to avoid overwrites.
  # Replicas are behind an ALB and don't need individual graph-registry entries.
  # Routing for shared repos uses:
  #   - Writes: instance-registry lookup for node_type=shared_master
  #   - Reads: SHARED_REPLICA_ALB_URL (auto-discovered from CloudFormation)
  if [ -n "${SHARED_REPOSITORIES}" ] && [ "${NODE_TYPE}" = "shared_master" ]; then
    echo "Registering shared repositories in graph registry (master only)..."
    # Convert comma-separated list to space-separated for iteration
    REPOS_LIST=$(echo "${SHARED_REPOSITORIES}" | tr ',' ' ')
    for repo in $REPOS_LIST; do
      repo=$(echo "$repo" | xargs) # Trim whitespace
      if [ -n "$repo" ]; then
        echo "  Registering shared repository: $repo"
        aws dynamodb put-item \
          --table-name "${GRAPH_REGISTRY_TABLE}" \
          --item "{
            \"graph_id\": {\"S\": \"$repo\"},
            \"instance_id\": {\"S\": \"$INSTANCE_ID\"},
            \"backend_type\": {\"S\": \"${DATABASE_TYPE}\"},
            \"status\": {\"S\": \"active\"},
            \"repository_type\": {\"S\": \"shared\"},
            \"created_at\": {\"S\": \"${TIMESTAMP}\"},
            \"entity_id\": {\"S\": \"shared_$repo\"}
          }" \
          --region ${AWS_REGION} || echo "WARNING: Failed to register shared repository $repo"
      fi
    done
  elif [ -n "${SHARED_REPOSITORIES}" ] && [ "${NODE_TYPE}" = "shared_replica" ]; then
    echo "Skipping graph-registry for replica (routing uses ALB, not individual instance IPs)"
  fi
fi

if [ $? -eq 0 ]; then
  echo "✅ Successfully registered graph instance in DynamoDB"
else
  echo "WARNING: Failed to register instance in DynamoDB"
  # Continue anyway - instance can still function
fi

exit 0
