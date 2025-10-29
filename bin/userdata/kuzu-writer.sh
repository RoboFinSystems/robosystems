#!/bin/bash
# Kuzu Writer Instance UserData Script (Refactored - Using Shared Components)
# This script is uploaded to S3 and executed during instance initialization

set -e
set -o pipefail

# Create log file for setup process
LOG_FILE="/var/log/kuzu-writer-setup.log"

# Logging setup - append to the setup log
exec > >(tee -a "$LOG_FILE")
exec 2>&1
echo "Starting Kuzu writer setup at $(date)"

# ==================================================================================
# ENVIRONMENT VARIABLE VALIDATION
# ==================================================================================
# Configuration - these should be set as environment variables by CloudFormation
: ${ENVIRONMENT:?"ENVIRONMENT variable must be set"}
: ${KUZU_NODE_TYPE:?"KUZU_NODE_TYPE variable must be set"}

# REPOSITORY_TYPE is required for single-repo writers, SHARED_REPOSITORIES for multi-repo shared writers
if [ "${KUZU_NODE_TYPE}" = "shared_master" ] || [ "${KUZU_NODE_TYPE}" = "shared_replica" ]; then
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
KUZU_PORT="${KUZU_PORT:-8001}"
SHARED_INSTANCE_NAME="${SHARED_INSTANCE_NAME:-shared-writer}"

# Set CloudWatch namespace with environment suffix
ENV_CAPITALIZED=$(echo "${ENVIRONMENT}" | awk '{print toupper(substr($0,1,1)) tolower(substr($0,2))}')
CLOUDWATCH_NAMESPACE="${CloudWatchNamespace:-RoboSystemsKuzu/${ENV_CAPITALIZED}}"

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
mkdir -p /mnt/kuzu-data

# Request volume attachment from Volume Manager
echo "Requesting volume attachment from Volume Manager..."
DATABASES=""
if [ "${KUZU_NODE_TYPE}" = "shared_master" ]; then
  # For shared master, specify SEC database
  DATABASES='["sec"]'
fi

# Prepare Lambda payload
PAYLOAD="{
  \"action\": \"instance_launch\",
  \"instance_id\": \"${INSTANCE_ID}\",
  \"node_type\": \"${KUZU_NODE_TYPE}\",
  \"tier\": \"${WRITER_TIER:-shared}\",
  \"availability_zone\": \"${AVAILABILITY_ZONE}\",
  \"databases\": ${DATABASES:-[]}
}"

# Lambda CLI v2 requires base64-encoded payload
ENCODED_PAYLOAD=$(echo -n "$PAYLOAD" | base64)

# Invoke Volume Manager Lambda to attach volume
aws lambda invoke \
  --function-name "RoboSystemsGraphVolumes${ENVIRONMENT^}-volume-manager" \
  --payload "$ENCODED_PAYLOAD" \
  --region ${REGION} \
  /tmp/volume-response.json || echo "Failed to invoke Volume Manager"

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
mount $DATA_DEVICE /mnt/kuzu-data
echo "$DATA_DEVICE /mnt/kuzu-data xfs defaults,nofail 0 2" >> /etc/fstab

# Create directory structure with proper ownership
mkdir -p /mnt/kuzu-data/{databases,backups,logs,staging}

# Docker containers typically run as UID 1000, set ownership accordingly
chown -R 1000:1000 /mnt/kuzu-data
chmod -R 755 /mnt/kuzu-data
chmod 775 /mnt/kuzu-data/databases

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
    Key=Name,Value="robosystems-kuzu-${KUZU_NODE_TYPE}-${ENVIRONMENT}-root" \
    Key=Environment,Value=${ENVIRONMENT} \
    Key=Service,Value=RoboSystems \
    Key=Component,Value=KuzuWriter \
    Key=NodeType,Value=${KUZU_NODE_TYPE} \
    Key=VolumeType,Value=RootVolume \
    Key=InstanceId,Value=${INSTANCE_ID} \
  --region ${REGION}

# Tag data volume
if [ "${KUZU_NODE_TYPE}" = "shared_master" ] || [ "${KUZU_NODE_TYPE}" = "shared_replica" ]; then
  DATA_VOLUME_NAME="robosystems-kuzu-shared-${ENVIRONMENT}-data"
else
  DATA_VOLUME_NAME="robosystems-kuzu-${KUZU_NODE_TYPE}-${ENVIRONMENT}-data"
fi

aws ec2 create-tags \
  --resources ${DATA_VOLUME} \
  --tags \
    Key=Name,Value="${DATA_VOLUME_NAME}" \
    Key=Environment,Value=${ENVIRONMENT} \
    Key=Service,Value=RoboSystems \
    Key=Component,Value=KuzuWriter \
    Key=NodeType,Value=${KUZU_NODE_TYPE} \
    Key=VolumeType,Value=KuzuData \
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
export DATABASE_TYPE="kuzu"
export NODE_TYPE="${KUZU_NODE_TYPE}"
export ENVIRONMENT="${ENVIRONMENT}"
export CLOUDWATCH_NAMESPACE="${CLOUDWATCH_NAMESPACE}"
export DATA_DIR="/mnt/kuzu-data"

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
cat > /etc/systemd/system/kuzu-lifecycle.service << EOF
[Unit]
Description=Kuzu Instance Lifecycle Manager
After=docker.service
Requires=docker.service

[Service]
Type=simple
ExecStart=/usr/local/bin/graph-lifecycle.sh monitor
Restart=always
RestartSec=30
Environment="DATABASE_TYPE=kuzu"
Environment="NODE_TYPE=${KUZU_NODE_TYPE}"
Environment="ENVIRONMENT=${ENVIRONMENT}"
Environment="AWS_REGION=${REGION}"

[Install]
WantedBy=multi-user.target
EOF

systemctl enable kuzu-lifecycle.service
systemctl start kuzu-lifecycle.service

# ==================================================================================
# INSTANCE REGISTRATION (Using Shared Script)
# ==================================================================================
# Export variables for shared registration script
export DATABASE_TYPE="kuzu"
export NODE_TYPE="${KUZU_NODE_TYPE}"
export ENVIRONMENT="${ENVIRONMENT}"
export INSTANCE_ID="${INSTANCE_ID}"
export PRIVATE_IP="${PRIVATE_IP}"
export AVAILABILITY_ZONE="${AVAILABILITY_ZONE}"
export INSTANCE_TYPE="${INSTANCE_TYPE}"
export CLUSTER_TIER="${CLUSTER_TIER}"
export CONTAINER_PORT="${KUZU_PORT}"
export AWS_REGION="${REGION}"
export AWS_STACK_NAME="${AWS_STACK_NAME}"
export REPOSITORY_TYPE="${REPOSITORY_TYPE:-}"
export SHARED_REPOSITORIES="${SHARED_REPOSITORIES:-}"

# Run shared registration script
/usr/local/bin/register-graph-instance.sh

# ==================================================================================
# CONTAINER SETUP (Using Shared Script)
# ==================================================================================
echo "Starting Kuzu writer container..."
docker pull ${ECR_URI}:${ECR_IMAGE_TAG}

# Export variables for shared container runner
export DATABASE_TYPE="kuzu"
export NODE_TYPE="${KUZU_NODE_TYPE}"
export CONTAINER_PORT="${KUZU_PORT}"
export ECR_IMAGE="${ECR_URI}:${ECR_IMAGE_TAG}"
export ENVIRONMENT="${ENVIRONMENT}"
export INSTANCE_ID="${INSTANCE_ID}"
export PRIVATE_IP="${PRIVATE_IP}"
export AVAILABILITY_ZONE="${AVAILABILITY_ZONE}"
export INSTANCE_TYPE="${INSTANCE_TYPE}"
export AWS_REGION="${REGION}"
export CLUSTER_TIER="${CLUSTER_TIER}"
export DATA_MOUNT_SOURCE="/mnt/kuzu-data/databases"
export DATA_MOUNT_TARGET="/app/data/kuzu-dbs"
export LOGS_MOUNT_SOURCE="/mnt/kuzu-data/logs"
export LOGS_MOUNT_TARGET="/app/logs"
export STAGING_MOUNT_SOURCE="/mnt/kuzu-data/staging"
export STAGING_MOUNT_TARGET="/app/data/staging"
export DOCKER_PROFILE="kuzu-writer"
export REPOSITORY_TYPE="${REPOSITORY_TYPE:-shared}"
export SHARED_REPOSITORIES="${SHARED_REPOSITORIES:-}"

# Run shared container runner
/usr/local/bin/run-graph-container.sh

# Update instance status to healthy
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
export DATABASE_TYPE="kuzu"
export NODE_TYPE="${KUZU_NODE_TYPE}"
export CONTAINER_PORT="${KUZU_PORT}"
export ENVIRONMENT="${ENVIRONMENT}"
export REGISTRY_TABLE="robosystems-graph-${ENVIRONMENT}-instance-registry"
export AWS_REGION="${REGION}"
export VALKEY_URL="${VALKEY_URL:-}"

# Create wrapper script that sets up environment for health check
cat > /usr/local/bin/kuzu-health-check-wrapper.sh << 'EOF'
#!/bin/bash
# Set up environment and run health check
export DATABASE_TYPE="kuzu"
export NODE_TYPE="${KUZU_NODE_TYPE}"
export CONTAINER_PORT="${KUZU_PORT}"
export ENVIRONMENT="${ENVIRONMENT}"
export REGISTRY_TABLE="robosystems-graph-${ENVIRONMENT}-instance-registry"
export AWS_REGION="${AWS_REGION}"
export VALKEY_URL="${VALKEY_URL:-}"

/usr/local/bin/graph-health-check.sh
EOF

# Substitute environment variables in wrapper
sed -i "s/\${KUZU_NODE_TYPE}/${KUZU_NODE_TYPE}/g" /usr/local/bin/kuzu-health-check-wrapper.sh
sed -i "s/\${KUZU_PORT}/${KUZU_PORT}/g" /usr/local/bin/kuzu-health-check-wrapper.sh
sed -i "s/\${ENVIRONMENT}/${ENVIRONMENT}/g" /usr/local/bin/kuzu-health-check-wrapper.sh
sed -i "s/\${AWS_REGION}/${REGION}/g" /usr/local/bin/kuzu-health-check-wrapper.sh
sed -i "s|\${VALKEY_URL}|${VALKEY_URL:-}|g" /usr/local/bin/kuzu-health-check-wrapper.sh

chmod +x /usr/local/bin/kuzu-health-check-wrapper.sh

# Start cronie service
systemctl enable crond
systemctl start crond

# Add health check to crontab
echo "*/5 * * * * /usr/local/bin/kuzu-health-check-wrapper.sh >> /var/log/kuzu-health-check.log 2>&1" | crontab -

# Setup log rotation
cat > /etc/logrotate.d/kuzu << EOF
/var/log/kuzu-*.log {
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
echo "✅ Kuzu writer setup completed successfully at $(date)"
echo "Instance ID: ${INSTANCE_ID}"
echo "Node Type: ${KUZU_NODE_TYPE}"
echo "Repository Type: ${REPOSITORY_TYPE:-shared}"
echo "Shared Repositories: ${SHARED_REPOSITORIES:-N/A}"
echo "Private IP: ${PRIVATE_IP}"
echo ""
echo "Shared scripts used:"
echo "  - setup-cloudwatch-graph.sh"
echo "  - register-graph-instance.sh"
echo "  - run-graph-container.sh"
echo "  - graph-health-check.sh"
echo "  - graph-lifecycle.sh (universal for Kuzu and Neo4j)"
