#!/bin/bash
# Neo4j Writer Instance UserData Script (Using Shared Components)
# This script is uploaded to S3 and executed during instance initialization

set -e
set -o pipefail

# Create log file for setup process
LOG_FILE="/var/log/neo4j-writer-setup.log"

# Logging setup - append to the setup log
exec > >(tee -a "$LOG_FILE")
exec 2>&1
echo "Starting Neo4j writer setup at $(date)"

# ==================================================================================
# ENVIRONMENT VARIABLE VALIDATION
# ==================================================================================
# Configuration - these should be set as environment variables by CloudFormation
: ${ENVIRONMENT:?"ENVIRONMENT variable must be set"}
: ${NEO4J_NODE_TYPE:?"NEO4J_NODE_TYPE variable must be set"}
: ${NEO4J_AUTH:?"NEO4J_AUTH variable must be set"}

# REPOSITORY_TYPE is required for single-repo writers, SHARED_REPOSITORIES for multi-repo shared writers
if [ "${NEO4J_NODE_TYPE}" = "shared_master" ] || [ "${NEO4J_NODE_TYPE}" = "shared_replica" ]; then
    : ${SHARED_REPOSITORIES:?"SHARED_REPOSITORIES variable must be set for shared writers"}
else
    : ${REPOSITORY_TYPE:?"REPOSITORY_TYPE variable must be set for user writers"}
fi

# Additional variables needed (passed from CloudFormation)
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
: ${AWS_ACCOUNT_ID:?"AWS_ACCOUNT_ID must be set"}
: ${ECR_URI:?"ECR_URI must be set"}
: ${ECR_IMAGE_TAG:?"ECR_IMAGE_TAG must be set"}
: ${CLUSTER_TIER:?"CLUSTER_TIER must be set"}
: ${AWS_STACK_NAME:?"AWS_STACK_NAME must be set"}

# Optional variables with defaults
INSTANCE_TYPE="${INSTANCE_TYPE:-c6g.medium}"
NEO4J_HTTP_PORT="${NEO4J_HTTP_PORT:-7474}"
NEO4J_BOLT_PORT="${NEO4J_BOLT_PORT:-7687}"
SHARED_INSTANCE_NAME="${SHARED_INSTANCE_NAME:-shared-writer}"

# Set CloudWatch namespace with environment suffix (unified for all graph backends)
ENV_CAPITALIZED=$(echo "${ENVIRONMENT}" | awk '{print toupper(substr($0,1,1)) tolower(substr($0,2))}')
CLOUDWATCH_NAMESPACE="${CloudWatchNamespace:-RoboSystemsGraph/${ENV_CAPITALIZED}/Neo4j}"

# ==================================================================================
# SYSTEM SETUP
# ==================================================================================
echo "Updating system packages..."
yum update -y
yum install -y amazon-cloudwatch-agent jq cronie nmap-ncat redis6

# Install Docker
echo "Installing Docker..."
yum install -y docker
systemctl enable docker
systemctl start docker
usermod -a -G docker ec2-user

# Configure Docker daemon
cat > /etc/docker/daemon.json << EOF
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m",
    "max-file": "3"
  },
  "storage-driver": "overlay2"
}
EOF

systemctl restart docker

# Login to ECR
echo "Logging into ECR..."
echo "Using ECR URI: ${ECR_URI}"
echo "Using Region: ${REGION}"
aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${ECR_URI} || {
    echo "ERROR: Failed to login to ECR"
    echo "Region: ${REGION}"
    echo "ECR URI: ${ECR_URI}"
    exit 1
}

# ==================================================================================
# INSTANCE METADATA
# ==================================================================================
# Get instance metadata with IMDSv2
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
AVAILABILITY_ZONE=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/availability-zone)
PRIVATE_IP=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)

echo "Instance setup: ID=${INSTANCE_ID}, AZ=${AVAILABILITY_ZONE}, IP=${PRIVATE_IP}"

# ==================================================================================
# STORAGE SETUP
# ==================================================================================
echo "Setting up storage..."
mkdir -p /mnt/neo4j-data

# Request volume attachment from Volume Manager
echo "Requesting volume attachment from Volume Manager..."
DATABASES=""
if [ "${NEO4J_NODE_TYPE}" = "shared_master" ]; then
  # For shared master, specify databases
  DATABASES='["sec"]'
fi

# Prepare Lambda payload
PAYLOAD="{
  \"action\": \"instance_launch\",
  \"instance_id\": \"${INSTANCE_ID}\",
  \"node_type\": \"${NEO4J_NODE_TYPE}\",
  \"tier\": \"${WRITER_TIER:-shared}\",
  \"availability_zone\": \"${AVAILABILITY_ZONE}\",
  \"databases\": ${DATABASES:-[]}
}"

# Lambda CLI v2 requires base64-encoded payload
ENCODED_PAYLOAD=$(echo -n "$PAYLOAD" | base64)

# Invoke Volume Manager Lambda to attach volume
if ! aws lambda invoke \
  --function-name "RoboSystemsGraphVolumes${ENVIRONMENT^}-volume-manager" \
  --payload "$ENCODED_PAYLOAD" \
  --region ${REGION} \
  /tmp/volume-response.json 2>&1 | tee /tmp/volume-manager-error.log; then
  echo "Failed to invoke Volume Manager Lambda"
  echo "Error details: $(cat /tmp/volume-manager-error.log 2>/dev/null || echo 'No error log available')"
  echo "Function: RoboSystemsGraphVolumes${ENVIRONMENT^}-volume-manager"
  echo "Region: ${REGION}"
fi

# Check if volume attachment was successful
if [ -f /tmp/volume-response.json ]; then
  VOLUME_ID=$(jq -r '.volume_id' /tmp/volume-response.json 2>/dev/null)
  echo "Volume Manager attached volume: ${VOLUME_ID}"
fi

# Wait for the data volume to be attached
echo "Waiting for data volume to be attached..."
DATA_DEVICE=""
COUNTER=0
MAX_WAIT=120

while [ -z "$DATA_DEVICE" ] && [ $COUNTER -lt $MAX_WAIT ]; do
  if [ -e /dev/nvme1n1 ]; then
    DATA_DEVICE="/dev/nvme1n1"
  elif [ -e /dev/xvdf ]; then
    DATA_DEVICE="/dev/xvdf"
  else
    sleep 1
    COUNTER=$((COUNTER + 1))
    if [ $((COUNTER % 10)) -eq 0 ]; then
      echo "Still waiting for volume... ($COUNTER seconds)"
    fi
  fi
done

if [ -z "$DATA_DEVICE" ]; then
  echo "ERROR: Data volume not found after ${MAX_WAIT} seconds"
  exit 1
fi

echo "Found data volume at: $DATA_DEVICE"

# Format only if not already formatted
if ! blkid $DATA_DEVICE; then
  echo "Formatting data volume with XFS..."
  mkfs -t xfs $DATA_DEVICE
fi

# Mount the volume
mount $DATA_DEVICE /mnt/neo4j-data
echo "$DATA_DEVICE /mnt/neo4j-data xfs defaults,nofail 0 2" >> /etc/fstab

# Create Neo4j directory structure with proper ownership
# Neo4j container expects specific directories for data, logs, import, plugins
mkdir -p /mnt/neo4j-data/{data,logs,import,plugins,backups}

# Neo4j official container runs as user neo4j (UID 7474)
chown -R 7474:7474 /mnt/neo4j-data
chmod -R 755 /mnt/neo4j-data

# ==================================================================================
# EBS VOLUME TAGGING
# ==================================================================================
echo "Tagging EBS volumes..."
ROOT_VOLUME=$(aws ec2 describe-instances \
  --instance-ids ${INSTANCE_ID} \
  --query "Reservations[0].Instances[0].BlockDeviceMappings[?DeviceName=='/dev/xvda'].Ebs.VolumeId" \
  --output text --region ${REGION})

DATA_VOLUME=$(aws ec2 describe-instances \
  --instance-ids ${INSTANCE_ID} \
  --query "Reservations[0].Instances[0].BlockDeviceMappings[?DeviceName=='/dev/xvdf'].Ebs.VolumeId" \
  --output text --region ${REGION})

# Tag root volume
aws ec2 create-tags \
  --resources ${ROOT_VOLUME} \
  --tags \
    Key=Name,Value="robosystems-neo4j-${NEO4J_NODE_TYPE}-${ENVIRONMENT}-root" \
    Key=Environment,Value=${ENVIRONMENT} \
    Key=Service,Value=RoboSystems \
    Key=Component,Value=Neo4jWriter \
    Key=NodeType,Value=${NEO4J_NODE_TYPE} \
    Key=VolumeType,Value=RootVolume \
    Key=InstanceId,Value=${INSTANCE_ID} \
  --region ${REGION}

# Tag data volume
if [ "${NEO4J_NODE_TYPE}" = "shared_master" ] || [ "${NEO4J_NODE_TYPE}" = "shared_replica" ]; then
  DATA_VOLUME_NAME="robosystems-neo4j-shared-${ENVIRONMENT}-data"
else
  DATA_VOLUME_NAME="robosystems-neo4j-${NEO4J_NODE_TYPE}-${ENVIRONMENT}-data"
fi

aws ec2 create-tags \
  --resources ${DATA_VOLUME} \
  --tags \
    Key=Name,Value="${DATA_VOLUME_NAME}" \
    Key=Environment,Value=${ENVIRONMENT} \
    Key=Service,Value=RoboSystems \
    Key=Component,Value=Neo4jWriter \
    Key=NodeType,Value=${NEO4J_NODE_TYPE} \
    Key=VolumeType,Value=Neo4jData \
    Key=InstanceId,Value=${INSTANCE_ID} \
    Key=DLMManaged,Value=true \
  --region ${REGION}

# ==================================================================================
# DOWNLOAD SHARED SCRIPTS
# ==================================================================================
echo "Downloading shared infrastructure scripts..."

# Download common graph scripts
aws s3 cp s3://robosystems-${ENVIRONMENT}-deployment/userdata/common/setup-cloudwatch-graph.sh \
    /usr/local/bin/setup-cloudwatch-graph.sh || {
  echo "ERROR: Could not download CloudWatch setup script from S3"
  exit 1
}

aws s3 cp s3://robosystems-${ENVIRONMENT}-deployment/userdata/common/register-graph-instance.sh \
    /usr/local/bin/register-graph-instance.sh || {
  echo "ERROR: Could not download instance registration script from S3"
  exit 1
}

aws s3 cp s3://robosystems-${ENVIRONMENT}-deployment/userdata/common/run-graph-container.sh \
    /usr/local/bin/run-graph-container.sh || {
  echo "ERROR: Could not download container runner script from S3"
  exit 1
}

aws s3 cp s3://robosystems-${ENVIRONMENT}-deployment/userdata/common/graph-health-check.sh \
    /usr/local/bin/graph-health-check.sh || {
  echo "ERROR: Could not download health check script from S3"
  exit 1
}

aws s3 cp s3://robosystems-${ENVIRONMENT}-deployment/userdata/common/graph-lifecycle.sh \
    /usr/local/bin/graph-lifecycle.sh || {
  echo "ERROR: Could not download lifecycle script from S3"
  exit 1
}

# Make scripts executable
chmod +x /usr/local/bin/setup-cloudwatch-graph.sh
chmod +x /usr/local/bin/register-graph-instance.sh
chmod +x /usr/local/bin/run-graph-container.sh
chmod +x /usr/local/bin/graph-health-check.sh
chmod +x /usr/local/bin/graph-lifecycle.sh

# ==================================================================================
# CLOUDWATCH SETUP (Using Shared Script)
# ==================================================================================
# Export variables for shared script
export DATABASE_TYPE="neo4j"
export NODE_TYPE="${NEO4J_NODE_TYPE}"
export ENVIRONMENT="${ENVIRONMENT}"
export CLOUDWATCH_NAMESPACE="${CLOUDWATCH_NAMESPACE}"
export DATA_DIR="/mnt/neo4j-data"

# Run shared CloudWatch setup
/usr/local/bin/setup-cloudwatch-graph.sh

# ==================================================================================
# METRICS CONFIGURATION
# ==================================================================================
# Note: OpenTelemetry (OTEL) is disabled by default in the Graph API
# Metrics are still collected and available via the /metrics endpoint as JSON
# ADOT collector setup has been removed to simplify infrastructure
echo "Metrics available at container /metrics endpoint (JSON format)"

# ==================================================================================
# LIFECYCLE MANAGEMENT SETUP
# ==================================================================================
echo "Setting up lifecycle management..."

# Setup systemd service for lifecycle management
cat > /etc/systemd/system/neo4j-lifecycle.service << EOF
[Unit]
Description=Neo4j Instance Lifecycle Manager
After=docker.service
Requires=docker.service

[Service]
Type=simple
ExecStart=/usr/local/bin/graph-lifecycle.sh monitor
Restart=always
RestartSec=30
Environment="DATABASE_TYPE=neo4j"
Environment="NODE_TYPE=${NEO4J_NODE_TYPE}"
Environment="ENVIRONMENT=${ENVIRONMENT}"
Environment="AWS_REGION=${REGION}"
Environment="NEO4J_PASSWORD=$(echo ${NEO4J_AUTH} | cut -d/ -f2)"

[Install]
WantedBy=multi-user.target
EOF

systemctl enable neo4j-lifecycle.service
systemctl start neo4j-lifecycle.service

# ==================================================================================
# INSTANCE REGISTRATION (Using Shared Script)
# ==================================================================================
# Export variables for shared registration script
export DATABASE_TYPE="neo4j"
export NODE_TYPE="${NEO4J_NODE_TYPE}"
export ENVIRONMENT="${ENVIRONMENT}"
export INSTANCE_ID="${INSTANCE_ID}"
export PRIVATE_IP="${PRIVATE_IP}"
export AVAILABILITY_ZONE="${AVAILABILITY_ZONE}"
export INSTANCE_TYPE="${INSTANCE_TYPE}"
export CLUSTER_TIER="${CLUSTER_TIER}"
export CONTAINER_PORT="${NEO4J_HTTP_PORT}"
export AWS_REGION="${REGION}"
export AWS_STACK_NAME="${AWS_STACK_NAME}"
export REPOSITORY_TYPE="${REPOSITORY_TYPE:-}"
export SHARED_REPOSITORIES="${SHARED_REPOSITORIES:-}"

# Run shared registration script
/usr/local/bin/register-graph-instance.sh

# ==================================================================================
# CONTAINER SETUP (Using Shared Script)
# ==================================================================================
echo "Starting Neo4j writer container..."
docker pull ${ECR_URI}:${ECR_IMAGE_TAG}

# Export variables for shared container runner
export DATABASE_TYPE="neo4j"
export NODE_TYPE="${NEO4J_NODE_TYPE}"
export CONTAINER_PORT="${NEO4J_HTTP_PORT}"
export ECR_IMAGE="${ECR_URI}:${ECR_IMAGE_TAG}"
export ENVIRONMENT="${ENVIRONMENT}"
export INSTANCE_ID="${INSTANCE_ID}"
export PRIVATE_IP="${PRIVATE_IP}"
export AVAILABILITY_ZONE="${AVAILABILITY_ZONE}"
export INSTANCE_TYPE="${INSTANCE_TYPE}"
export AWS_REGION="${REGION}"
export CLUSTER_TIER="${CLUSTER_TIER}"
export DATA_MOUNT_SOURCE="/mnt/neo4j-data/data"
export DATA_MOUNT_TARGET="/data"
export LOGS_MOUNT_SOURCE="/mnt/neo4j-data/logs"
export LOGS_MOUNT_TARGET="/logs"
export STAGING_MOUNT_SOURCE="/mnt/duckdb-staging"
export STAGING_MOUNT_TARGET="/app/data/staging"
export DOCKER_PROFILE="neo4j-writer"
export REPOSITORY_TYPE="${REPOSITORY_TYPE:-shared}"
export SHARED_REPOSITORIES="${SHARED_REPOSITORIES:-}"
export NEO4J_AUTH="${NEO4J_AUTH}"
export NEO4J_BOLT_PORT="${NEO4J_BOLT_PORT}"

# Persist all required variables to /etc/environment for container restarts and refreshes
echo "DATABASE_TYPE=neo4j" >> /etc/environment
echo "NODE_TYPE=${NEO4J_NODE_TYPE}" >> /etc/environment
echo "CONTAINER_PORT=${NEO4J_HTTP_PORT}" >> /etc/environment
echo "ECR_URI=${ECR_URI}" >> /etc/environment
echo "ECR_IMAGE_TAG=${ECR_IMAGE_TAG}" >> /etc/environment
echo "ENVIRONMENT=${ENVIRONMENT}" >> /etc/environment
echo "INSTANCE_ID=${INSTANCE_ID}" >> /etc/environment
echo "PRIVATE_IP=${PRIVATE_IP}" >> /etc/environment
echo "AVAILABILITY_ZONE=${AVAILABILITY_ZONE}" >> /etc/environment
echo "INSTANCE_TYPE=${INSTANCE_TYPE}" >> /etc/environment
echo "AWS_REGION=${REGION}" >> /etc/environment
echo "CLUSTER_TIER=${CLUSTER_TIER}" >> /etc/environment
echo "REPOSITORY_TYPE=${REPOSITORY_TYPE:-shared}" >> /etc/environment
echo "SHARED_REPOSITORIES=${SHARED_REPOSITORIES:-}" >> /etc/environment
echo "NEO4J_AUTH=${NEO4J_AUTH}" >> /etc/environment
echo "NEO4J_BOLT_PORT=${NEO4J_BOLT_PORT}" >> /etc/environment

# Run shared container runner
/usr/local/bin/run-graph-container.sh

# Update instance status to healthy (uses shared Kuzu registry)
echo "Marking instance as healthy..."
aws dynamodb update-item \
  --table-name robosystems-graph-${ENVIRONMENT}-instance-registry \
  --key "{\"instance_id\": {\"S\": \"${INSTANCE_ID}\"}}" \
  --update-expression "SET #status = :status" \
  --expression-attribute-names '{"#status": "status"}' \
  --expression-attribute-values '{":status": {"S": "healthy"}}' \
  --region ${REGION}

# ==================================================================================
# HEALTH CHECK CRON SETUP
# ==================================================================================
echo "Setting up health check cron job..."

# Export variables for health check script
export DATABASE_TYPE="neo4j"
export NODE_TYPE="${NEO4J_NODE_TYPE}"
export CONTAINER_PORT="${NEO4J_HTTP_PORT}"
export ENVIRONMENT="${ENVIRONMENT}"
export REGISTRY_TABLE="robosystems-graph-${ENVIRONMENT}-instance-registry"
export AWS_REGION="${REGION}"
export VALKEY_URL="${VALKEY_URL:-}"

# Create wrapper script that sets up environment for health check
cat > /usr/local/bin/neo4j-health-check-wrapper.sh << 'EOF'
#!/bin/bash
# Set up environment and run health check
export DATABASE_TYPE="neo4j"
export NODE_TYPE="${NEO4J_NODE_TYPE}"
export CONTAINER_PORT="${NEO4J_HTTP_PORT}"
export ENVIRONMENT="${ENVIRONMENT}"
export REGISTRY_TABLE="robosystems-graph-${ENVIRONMENT}-instance-registry"
export AWS_REGION="${AWS_REGION}"
export VALKEY_URL="${VALKEY_URL:-}"

/usr/local/bin/graph-health-check.sh
EOF

# Substitute environment variables in wrapper
sed -i "s/\${NEO4J_NODE_TYPE}/${NEO4J_NODE_TYPE}/g" /usr/local/bin/neo4j-health-check-wrapper.sh
sed -i "s/\${NEO4J_HTTP_PORT}/${NEO4J_HTTP_PORT}/g" /usr/local/bin/neo4j-health-check-wrapper.sh
sed -i "s/\${ENVIRONMENT}/${ENVIRONMENT}/g" /usr/local/bin/neo4j-health-check-wrapper.sh
sed -i "s/\${AWS_REGION}/${REGION}/g" /usr/local/bin/neo4j-health-check-wrapper.sh
sed -i "s|\${VALKEY_URL}|${VALKEY_URL:-}|g" /usr/local/bin/neo4j-health-check-wrapper.sh

chmod +x /usr/local/bin/neo4j-health-check-wrapper.sh

# Start cronie service
systemctl enable crond
systemctl start crond

# Add health check to crontab
echo "*/5 * * * * /usr/local/bin/neo4j-health-check-wrapper.sh >> /var/log/neo4j-health-check.log 2>&1" | crontab -

# Setup log rotation
cat > /etc/logrotate.d/neo4j << EOF
/var/log/neo4j-*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 644 root root
}
EOF

# ==================================================================================
# COMPLETION
# ==================================================================================
echo "✅ Neo4j writer setup completed successfully at $(date)"
echo "Instance ID: ${INSTANCE_ID}"
echo "Node Type: ${NEO4J_NODE_TYPE}"
echo "Repository Type: ${REPOSITORY_TYPE:-shared}"
echo "Shared Repositories: ${SHARED_REPOSITORIES:-N/A}"
echo "Private IP: ${PRIVATE_IP}"
echo "HTTP Port: ${NEO4J_HTTP_PORT}"
echo "Bolt Port: ${NEO4J_BOLT_PORT}"
echo ""
echo "Shared scripts used:"
echo "  - setup-cloudwatch-graph.sh"
echo "  - register-graph-instance.sh"
echo "  - run-graph-container.sh"
echo "  - graph-health-check.sh"
echo "  - graph-lifecycle.sh (universal for Kuzu and Neo4j)"
