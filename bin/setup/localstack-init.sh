#!/bin/bash
# LocalStack initialization script
# This runs inside the LocalStack container when it's ready

echo "Creating S3 buckets for RoboSystems..."

# ==============================================================================
# NEW BUCKET STRUCTURE (2026-01 Restructure)
# ==============================================================================

echo "Creating new structure buckets..."

# Shared Raw Data Bucket (external source downloads: SEC, FRED, BLS, etc.)
awslocal s3api create-bucket \
  --bucket robosystems-shared-raw \
  --region us-east-1 || echo "Bucket robosystems-shared-raw already exists"

# Shared Processed Data Bucket (parquet files for graph ingestion)
awslocal s3api create-bucket \
  --bucket robosystems-shared-processed \
  --region us-east-1 || echo "Bucket robosystems-shared-processed already exists"

# User Data Bucket (user uploads, staging tables, exports)
awslocal s3api create-bucket \
  --bucket robosystems-user \
  --region us-east-1 || echo "Bucket robosystems-user already exists"

# NOTE: Public Data Bucket (robosystems-public-data) is created in legacy section
# and remains unchanged - it's actively serving CDN content

# ==============================================================================
# LEGACY BUCKETS (kept for backwards compatibility during migration)
# ==============================================================================

echo "Creating legacy buckets for backwards compatibility..."

# Legacy local bucket
awslocal s3api create-bucket \
  --bucket robosystems-local \
  --region us-east-1 || echo "Bucket robosystems-local already exists"

# Legacy dev bucket
awslocal s3api create-bucket \
  --bucket robosystems-dev \
  --region us-east-1 || echo "Bucket robosystems-dev already exists"

# Legacy SEC buckets (will be deprecated)
awslocal s3api create-bucket \
  --bucket robosystems-sec-raw \
  --region us-east-1 || echo "Bucket robosystems-sec-raw already exists"

awslocal s3api create-bucket \
  --bucket robosystems-sec-processed \
  --region us-east-1 || echo "Bucket robosystems-sec-processed already exists"

# Legacy public data bucket
awslocal s3api create-bucket \
  --bucket robosystems-public-data \
  --region us-east-1 || echo "Bucket robosystems-public-data already exists"

awslocal s3api put-bucket-cors \
  --bucket robosystems-public-data \
  --cors-configuration '{"CORSRules":[{"AllowedOrigins":["*"],"AllowedMethods":["GET","HEAD"],"AllowedHeaders":["*"],"MaxAge":3600}]}' || echo "CORS already configured for robosystems-public-data"

# Legacy graph database buckets
awslocal s3api create-bucket \
  --bucket robosystems-graph-databases \
  --region us-east-1 || echo "Bucket robosystems-graph-databases already exists"

awslocal s3api create-bucket \
  --bucket robosystems-graph-databases-dev \
  --region us-east-1 || echo "Bucket robosystems-graph-databases-dev already exists"

echo "S3 buckets created successfully!"

# List buckets to verify
echo "Available S3 buckets:"
awslocal s3 ls

echo ""
echo "Creating DynamoDB tables for LadybugDB allocation management..."

# Delete existing tables if they exist (to ensure clean schema)
awslocal dynamodb delete-table --table-name robosystems-graph-dev-graph-registry 2>/dev/null || true
awslocal dynamodb delete-table --table-name robosystems-graph-dev-instance-registry 2>/dev/null || true

# Wait a moment for deletion
sleep 2

# Create graph registry table with new multi-region schema
awslocal dynamodb create-table \
  --table-name robosystems-graph-dev-graph-registry \
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
    StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
  --region us-east-1 || echo "Table robosystems-graph-dev-graph-registry already exists"

# Create instance registry table with new multi-region schema
awslocal dynamodb create-table \
  --table-name robosystems-graph-dev-instance-registry \
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
    ReadCapacityUnits=5,WriteCapacityUnits=5 \
  --region us-east-1 || echo "Table robosystems-graph-dev-instance-registry already exists"

echo "DynamoDB tables created with multi-region schema!"

# List tables to verify
echo "Available DynamoDB tables:"
awslocal dynamodb list-tables

echo ""
echo "Registering local LadybugDB instance in DynamoDB..."

# First, delete any existing registration to ensure clean state
awslocal dynamodb delete-item \
  --table-name robosystems-graph-dev-instance-registry \
  --key '{"instance_id": {"S": "local-lbug-writer"}}' \
  --region us-east-1 2>/dev/null || true

# Register local LadybugDB writer instance with regional fields
awslocal dynamodb put-item \
  --table-name robosystems-graph-dev-instance-registry \
  --item '{
    "instance_id": {"S": "local-lbug-writer"},
    "private_ip": {"S": "graph-api"},
    "availability_zone": {"S": "docker-local"},
    "status": {"S": "healthy"},
    "database_count": {"N": "0"},
    "max_databases": {"N": "'${LBUG_DATABASES_PER_INSTANCE:-50}'"},
    "created_at": {"S": "'$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")'"},
    "last_health_check": {"S": "'$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")'"},
    "node_type": {"S": "writer"},
    "api_endpoint": {"S": "http://graph-api:8001"},
    "region": {"S": "docker-local"},
    "cluster_type": {"S": "writer"},
    "cluster_tier": {"S": "ladybug-standard"},
    "cluster_group": {"S": "docker-local-writers"},
    "available_capacity_pct": {"N": "100"},
    "private_dns": {"S": "graph-api.local"},
    "endpoint_url": {"S": "http://graph-api:8001"},
    "launch_time": {"S": "'$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")'"},
    "replication_role": {"S": "none"},
    "total_size_gb": {"N": "0"},
    "cpu_utilization": {"N": "0"},
    "memory_utilization": {"N": "0"},
    "iops_utilization": {"N": "0"}
  }' \
  --region us-east-1 || echo "Local instance already registered"

echo "Local LadybugDB instance registered!"

echo ""
echo "LocalStack initialization complete!"
