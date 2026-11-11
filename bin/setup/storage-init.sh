#!/bin/bash
# Creates the dev stack's S3 buckets (SeaweedFS) and DynamoDB registry tables
# (DynamoDB Local). Runs in the storage-init container on every `up`, and
# leaves whatever exists alone, so both stores keep their contents across
# restarts. A table schema change needs `just clean-volumes`.
set -euo pipefail

S3="aws --endpoint-url ${S3_ENDPOINT_URL}"
DDB="aws --endpoint-url ${DYNAMODB_ENDPOINT_URL}"
GRAPH_REGISTRY_TABLE=${GRAPH_REGISTRY_TABLE:-robosystems-graph-dev-graph-registry}
INSTANCE_REGISTRY_TABLE=${INSTANCE_REGISTRY_TABLE:-robosystems-graph-dev-instance-registry}

wait_for() {
  local name=$1
  shift
  for _ in $(seq 1 60); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "$name did not answer within 60s" >&2
  return 1
}

wait_for "S3 at ${S3_ENDPOINT_URL}" $S3 s3api list-buckets
wait_for "DynamoDB at ${DYNAMODB_ENDPOINT_URL}" $DDB dynamodb list-tables

# ==============================================================================
# S3 BUCKETS
# ==============================================================================

ensure_bucket() {
  if $S3 s3api head-bucket --bucket "$1" >/dev/null 2>&1; then
    echo "Bucket $1 exists"
  else
    $S3 s3api create-bucket --bucket "$1" >/dev/null
    echo "Created bucket $1"
  fi
}

ensure_bucket robosystems-shared-raw       # external source downloads (SEC, FRED, BLS)
ensure_bucket robosystems-shared-processed # parquet files for graph ingestion
ensure_bucket robosystems-user             # user uploads, staging tables, exports
ensure_bucket robosystems-public-data      # externalized fact HTML, read anonymously
ensure_bucket robosystems-local            # pytest

# The S3 API key is MaxAgeSeconds (not MaxAge, which fails validation silently
# and leaves the bucket with no CORS).
$S3 s3api put-bucket-cors \
  --bucket robosystems-public-data \
  --cors-configuration '{"CORSRules":[{"AllowedOrigins":["*"],"AllowedMethods":["GET","HEAD"],"AllowedHeaders":["*"],"ExposeHeaders":["ETag","Content-Length","Content-Type"],"MaxAgeSeconds":3600}]}'

# The apps upload files straight to presigned URLs on the user bucket, so the
# browser's preflight needs their origins allowed (production: s3.yaml): the
# localhost ones, plus the app URLs a server on its own names sets in .env.
APP_ORIGINS=""
for url in http://localhost:3000 http://localhost:3001 http://localhost:3002 \
  "${ROBOSYSTEMS_URL:-}" "${ROBOLEDGER_URL:-}" "${ROBOINVESTOR_URL:-}"; do
  origin=$(printf '%s' "$url" | sed -nE 's#^(https?://[^/]+).*#\1#p')
  [ -n "$origin" ] || continue
  case ",$APP_ORIGINS," in *",\"$origin\","*) continue ;; esac
  APP_ORIGINS="${APP_ORIGINS:+$APP_ORIGINS,}\"$origin\""
done
$S3 s3api put-bucket-cors \
  --bucket robosystems-user \
  --cors-configuration "{\"CORSRules\":[{\"AllowedOrigins\":[${APP_ORIGINS}],\"AllowedMethods\":[\"PUT\",\"HEAD\"],\"AllowedHeaders\":[\"*\"],\"ExposeHeaders\":[\"ETag\"],\"MaxAgeSeconds\":3600}]}"
echo "User bucket CORS origins: ${APP_ORIGINS}"

echo "Buckets:"
$S3 s3api list-buckets --query 'Buckets[].Name' --output text

# ==============================================================================
# DYNAMODB REGISTRY TABLES
# ==============================================================================

table_exists() {
  $DDB dynamodb describe-table --table-name "$1" >/dev/null 2>&1
}

if table_exists "$GRAPH_REGISTRY_TABLE"; then
  echo "Table $GRAPH_REGISTRY_TABLE exists"
else
  $DDB dynamodb create-table \
    --table-name "$GRAPH_REGISTRY_TABLE" \
    --attribute-definitions \
      AttributeName=graph_id,AttributeType=S \
      AttributeName=entity_id,AttributeType=S \
      AttributeName=instance_id,AttributeType=S \
      AttributeName=current_region,AttributeType=S \
      AttributeName=status,AttributeType=S \
      AttributeName=replication_status,AttributeType=S \
      AttributeName=last_accessed,AttributeType=S \
    --key-schema \
      AttributeName=graph_id,KeyType=HASH \
    --global-secondary-indexes \
      '[
        {
          "IndexName": "entity-index",
          "KeySchema": [{"AttributeName": "entity_id", "KeyType": "HASH"}],
          "Projection": {"ProjectionType": "ALL"},
          "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        },
        {
          "IndexName": "instance-index",
          "KeySchema": [{"AttributeName": "instance_id", "KeyType": "HASH"}],
          "Projection": {"ProjectionType": "ALL"},
          "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        },
        {
          "IndexName": "region-status-index",
          "KeySchema": [
            {"AttributeName": "current_region", "KeyType": "HASH"},
            {"AttributeName": "status", "KeyType": "RANGE"}
          ],
          "Projection": {"ProjectionType": "ALL"},
          "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        },
        {
          "IndexName": "entity-region-index",
          "KeySchema": [
            {"AttributeName": "entity_id", "KeyType": "HASH"},
            {"AttributeName": "current_region", "KeyType": "RANGE"}
          ],
          "Projection": {"ProjectionType": "ALL"},
          "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        },
        {
          "IndexName": "replication-status-index",
          "KeySchema": [
            {"AttributeName": "replication_status", "KeyType": "HASH"},
            {"AttributeName": "last_accessed", "KeyType": "RANGE"}
          ],
          "Projection": {"ProjectionType": "ALL"},
          "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        }
      ]' \
    --provisioned-throughput \
      ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --stream-specification \
      StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES >/dev/null
  echo "Created table $GRAPH_REGISTRY_TABLE"
fi

if table_exists "$INSTANCE_REGISTRY_TABLE"; then
  echo "Table $INSTANCE_REGISTRY_TABLE exists"
else
  $DDB dynamodb create-table \
    --table-name "$INSTANCE_REGISTRY_TABLE" \
    --attribute-definitions \
      AttributeName=instance_id,AttributeType=S \
      AttributeName=region,AttributeType=S \
      AttributeName=cluster_type,AttributeType=S \
      AttributeName=cluster_group,AttributeType=S \
      AttributeName=available_capacity_pct,AttributeType=N \
      AttributeName=status,AttributeType=S \
    --key-schema \
      AttributeName=instance_id,KeyType=HASH \
    --global-secondary-indexes \
      '[
        {
          "IndexName": "region-cluster-index",
          "KeySchema": [
            {"AttributeName": "region", "KeyType": "HASH"},
            {"AttributeName": "cluster_type", "KeyType": "RANGE"}
          ],
          "Projection": {"ProjectionType": "ALL"},
          "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        },
        {
          "IndexName": "cluster-capacity-index",
          "KeySchema": [
            {"AttributeName": "cluster_group", "KeyType": "HASH"},
            {"AttributeName": "available_capacity_pct", "KeyType": "RANGE"}
          ],
          "Projection": {"ProjectionType": "ALL"},
          "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        },
        {
          "IndexName": "health-region-index",
          "KeySchema": [
            {"AttributeName": "status", "KeyType": "HASH"},
            {"AttributeName": "region", "KeyType": "RANGE"}
          ],
          "Projection": {"ProjectionType": "ALL"},
          "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        }
      ]' \
    --provisioned-throughput \
      ReadCapacityUnits=5,WriteCapacityUnits=5 >/dev/null
  echo "Created table $INSTANCE_REGISTRY_TABLE"
fi

# The one local writer. Rewritten on every start so its timestamps are fresh;
# graph-api keeps database_count and available_capacity_pct current.
NOW=$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")
$DDB dynamodb put-item \
  --table-name "$INSTANCE_REGISTRY_TABLE" \
  --item '{
    "instance_id": {"S": "local-lbug-writer"},
    "private_ip": {"S": "graph-api"},
    "availability_zone": {"S": "docker-local"},
    "status": {"S": "healthy"},
    "database_count": {"N": "0"},
    "max_databases": {"N": "'"${LBUG_DATABASES_PER_INSTANCE:-50}"'"},
    "created_at": {"S": "'"$NOW"'"},
    "last_health_check": {"S": "'"$NOW"'"},
    "node_type": {"S": "writer"},
    "api_endpoint": {"S": "http://graph-api:8001"},
    "region": {"S": "docker-local"},
    "cluster_type": {"S": "writer"},
    "cluster_tier": {"S": "ladybug-standard"},
    "cluster_group": {"S": "docker-local-writers"},
    "available_capacity_pct": {"N": "100"},
    "private_dns": {"S": "graph-api.local"},
    "endpoint_url": {"S": "http://graph-api:8001"},
    "launch_time": {"S": "'"$NOW"'"},
    "replication_role": {"S": "none"},
    "total_size_gb": {"N": "0"},
    "cpu_utilization": {"N": "0"},
    "memory_utilization": {"N": "0"},
    "iops_utilization": {"N": "0"}
  }'
echo "Registered local-lbug-writer"

echo "Tables:"
$DDB dynamodb list-tables --query 'TableNames' --output text

echo "Storage initialization complete"
