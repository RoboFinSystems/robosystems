#!/bin/bash
# LadybugDB Shared Replica Instance UserData Script
# Replicas boot from EBS snapshots of the shared master and serve read-only queries
# This script uses the same shared components as writers for consistency

set -e
set -o pipefail

# Create log file for setup process
LOG_FILE="/var/log/ladybug-replica-setup.log"

# Logging setup - append to the setup log
exec > >(tee -a "$LOG_FILE")
exec 2>&1
echo "Starting LadybugDB shared replica setup at $(date)"

# ==================================================================================
# ENVIRONMENT VARIABLE VALIDATION
# ==================================================================================
# Configuration - these should be set as environment variables by CloudFormation
: ${ENVIRONMENT:?"ENVIRONMENT variable must be set"}
: ${AWS_REGION:?"AWS_REGION must be set"}
: ${AWS_ACCOUNT_ID:?"AWS_ACCOUNT_ID must be set"}
: ${ECR_URI:?"ECR_URI must be set"}
: ${ECR_IMAGE_TAG:?"ECR_IMAGE_TAG must be set"}
: ${AWS_STACK_NAME:?"AWS_STACK_NAME must be set"}
: ${DEPLOYMENT_BUCKET:?"DEPLOYMENT_BUCKET must be set"}

# Replica-specific - these are fixed for shared replicas
LBUG_NODE_TYPE="shared_replica"
SHARED_REPOSITORIES="${SHARED_REPOSITORIES:-sec}"
CLUSTER_TIER="${CLUSTER_TIER:-ladybug-shared}"
REPOSITORY_TYPE="shared"

# Optional variables with defaults
LBUG_PORT="${LBUG_PORT:-8001}"
CLOUDWATCH_NAMESPACE="${CloudWatchNamespace:-RoboSystems/Graph}"

echo "=== Replica Configuration ==="
echo "Environment: ${ENVIRONMENT}"
echo "Node Type: ${LBUG_NODE_TYPE}"
echo "Shared Repositories: ${SHARED_REPOSITORIES}"
echo "Cluster Tier: ${CLUSTER_TIER}"
echo "ECR Image: ${ECR_URI}:${ECR_IMAGE_TAG}"

# ==================================================================================
# INSTANCE METADATA
# ==================================================================================
echo "Fetching instance metadata..."
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
AVAILABILITY_ZONE=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/availability-zone)
PRIVATE_IP=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)
INSTANCE_TYPE=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-type)

echo "Instance setup: ID=${INSTANCE_ID}, AZ=${AVAILABILITY_ZONE}, IP=${PRIVATE_IP}, Type=${INSTANCE_TYPE}"

# ==================================================================================
# SYSTEM SETUP
# ==================================================================================
echo "Updating system packages..."
yum update -y
yum install -y amazon-cloudwatch-agent jq cronie nmap-ncat

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
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${ECR_URI} || {
    echo "ERROR: Failed to login to ECR"
    exit 1
}

# ==================================================================================
# STORAGE SETUP (Replica-specific: volume from snapshot, already formatted)
# ==================================================================================
echo "Setting up storage from snapshot..."

# Wait for data volume to be attached
echo "Waiting for data volume..."
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

# Create mount point
mkdir -p /mnt/ladybug-data

# Mount the volume (DO NOT FORMAT - data comes from snapshot)
if ! mount | grep -q "/mnt/ladybug-data"; then
  echo "Mounting data volume (pre-formatted from snapshot)..."
  mount $DATA_DEVICE /mnt/ladybug-data || {
    echo "ERROR: Failed to mount data volume. Checking filesystem..."
    blkid $DATA_DEVICE
    exit 1
  }
fi

echo "$DATA_DEVICE /mnt/ladybug-data xfs defaults,nofail 0 2" >> /etc/fstab

# Verify data exists
if [ -d "/mnt/ladybug-data/databases" ]; then
  echo "✅ Database directory found from snapshot"
  ls -la /mnt/ladybug-data/databases/
else
  echo "WARNING: No databases directory found - snapshot may be empty"
  mkdir -p /mnt/ladybug-data/{databases,logs,staging}
fi

# ==================================================================================
# EBS VOLUME PRE-WARMING (Hydrate from S3 snapshot)
# ==================================================================================
# EBS snapshots use lazy loading - blocks are fetched from S3 on first access.
# This causes slow/hanging database opens. Pre-read all database files to force
# AWS to fetch all blocks before the container starts.
echo "Pre-warming EBS volume from snapshot (this may take several minutes)..."
# Find all .lbug files recursively (they may be nested in subdirectories)
find /mnt/ladybug-data/databases -name "*.lbug" -type f | while read db_file; do
  FILE_SIZE=$(stat -c%s "$db_file" 2>/dev/null || stat -f%z "$db_file")
  FILE_SIZE_GB=$((FILE_SIZE / 1024 / 1024 / 1024))
  echo "  Pre-warming: $db_file (${FILE_SIZE_GB}GB)"
  dd if="$db_file" of=/dev/null bs=1M status=progress 2>&1 | tail -3
done
echo "✅ EBS volume pre-warming complete"

# Ensure proper ownership for container
chown -R 1000:1000 /mnt/ladybug-data
chmod -R 755 /mnt/ladybug-data

# ==================================================================================
# DOWNLOAD SHARED SCRIPTS
# ==================================================================================
echo "Downloading shared infrastructure scripts from S3..."

# Download common graph scripts
aws s3 cp s3://${DEPLOYMENT_BUCKET}/userdata/common/setup-cloudwatch-graph.sh \
    /usr/local/bin/setup-cloudwatch-graph.sh || {
  echo "ERROR: Could not download CloudWatch setup script from S3"
  exit 1
}

aws s3 cp s3://${DEPLOYMENT_BUCKET}/userdata/common/register-graph-instance.sh \
    /usr/local/bin/register-graph-instance.sh || {
  echo "ERROR: Could not download instance registration script from S3"
  exit 1
}

aws s3 cp s3://${DEPLOYMENT_BUCKET}/userdata/common/run-graph-container.sh \
    /usr/local/bin/run-graph-container.sh || {
  echo "ERROR: Could not download container runner script from S3"
  exit 1
}

aws s3 cp s3://${DEPLOYMENT_BUCKET}/userdata/common/graph-health-check.sh \
    /usr/local/bin/graph-health-check.sh || {
  echo "ERROR: Could not download health check script from S3"
  exit 1
}

# Make scripts executable
chmod +x /usr/local/bin/setup-cloudwatch-graph.sh
chmod +x /usr/local/bin/register-graph-instance.sh
chmod +x /usr/local/bin/run-graph-container.sh
chmod +x /usr/local/bin/graph-health-check.sh

# ==================================================================================
# CLOUDWATCH SETUP (Using Shared Script)
# ==================================================================================
export DATABASE_TYPE="ladybug"
export NODE_TYPE="${LBUG_NODE_TYPE}"
export ENVIRONMENT="${ENVIRONMENT}"
export CLOUDWATCH_NAMESPACE="${CLOUDWATCH_NAMESPACE}"
export DATA_DIR="/mnt/ladybug-data"

/usr/local/bin/setup-cloudwatch-graph.sh

# ==================================================================================
# INSTANCE REGISTRATION (Using Shared Script)
# ==================================================================================
echo "Registering replica instance in DynamoDB..."

export DATABASE_TYPE="ladybug"
export NODE_TYPE="${LBUG_NODE_TYPE}"
export ENVIRONMENT="${ENVIRONMENT}"
export INSTANCE_ID="${INSTANCE_ID}"
export PRIVATE_IP="${PRIVATE_IP}"
export AVAILABILITY_ZONE="${AVAILABILITY_ZONE}"
export INSTANCE_TYPE="${INSTANCE_TYPE}"
export CLUSTER_TIER="${CLUSTER_TIER}"
export CONTAINER_PORT="${LBUG_PORT}"
export AWS_REGION="${AWS_REGION}"
export AWS_STACK_NAME="${AWS_STACK_NAME}"
export REPOSITORY_TYPE="${REPOSITORY_TYPE}"
export SHARED_REPOSITORIES="${SHARED_REPOSITORIES}"

/usr/local/bin/register-graph-instance.sh

# ==================================================================================
# CONTAINER SETUP (Using Shared Script)
# ==================================================================================
echo "Starting LadybugDB replica container..."
docker pull ${ECR_URI}:${ECR_IMAGE_TAG}

# Export variables for shared container runner
export DATABASE_TYPE="ladybug"
export NODE_TYPE="${LBUG_NODE_TYPE}"
export CONTAINER_PORT="${LBUG_PORT}"
export ECR_IMAGE="${ECR_URI}:${ECR_IMAGE_TAG}"
export ENVIRONMENT="${ENVIRONMENT}"
export INSTANCE_ID="${INSTANCE_ID}"
export PRIVATE_IP="${PRIVATE_IP}"
export AVAILABILITY_ZONE="${AVAILABILITY_ZONE}"
export INSTANCE_TYPE="${INSTANCE_TYPE}"
export AWS_REGION="${AWS_REGION}"
export CLUSTER_TIER="${CLUSTER_TIER}"
export DATA_MOUNT_SOURCE="/mnt/ladybug-data/databases"
export DATA_MOUNT_TARGET="/app/data/lbug-dbs"
export LOGS_MOUNT_SOURCE="/mnt/ladybug-data/logs"
export LOGS_MOUNT_TARGET="/app/logs"

# Replica-specific: use shared profile
export DOCKER_PROFILE="ladybug-shared-writer"
export REPOSITORY_TYPE="${REPOSITORY_TYPE}"
export SHARED_REPOSITORIES="${SHARED_REPOSITORIES}"

# Persist variables to /etc/environment for health checks and restarts
echo "DATABASE_TYPE=ladybug" >> /etc/environment
echo "NODE_TYPE=${LBUG_NODE_TYPE}" >> /etc/environment
echo "CONTAINER_PORT=${LBUG_PORT}" >> /etc/environment
echo "ECR_URI=${ECR_URI}" >> /etc/environment
echo "ECR_IMAGE_TAG=${ECR_IMAGE_TAG}" >> /etc/environment
echo "ENVIRONMENT=${ENVIRONMENT}" >> /etc/environment
echo "INSTANCE_ID=${INSTANCE_ID}" >> /etc/environment
echo "PRIVATE_IP=${PRIVATE_IP}" >> /etc/environment
echo "AVAILABILITY_ZONE=${AVAILABILITY_ZONE}" >> /etc/environment
echo "INSTANCE_TYPE=${INSTANCE_TYPE}" >> /etc/environment
echo "AWS_REGION=${AWS_REGION}" >> /etc/environment
echo "CLUSTER_TIER=${CLUSTER_TIER}" >> /etc/environment
echo "REPOSITORY_TYPE=${REPOSITORY_TYPE}" >> /etc/environment
echo "SHARED_REPOSITORIES=${SHARED_REPOSITORIES}" >> /etc/environment

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
  --region ${AWS_REGION}

# ==================================================================================
# HEALTH CHECK CRON SETUP
# ==================================================================================
echo "Setting up health check cron job..."

# Create wrapper script for health check
cat > /usr/local/bin/ladybug-health-check-wrapper.sh << 'HEALTHEOF'
#!/bin/bash
# Source environment for health check
if [ -f /etc/environment ]; then
  set -a
  source /etc/environment
  set +a
fi

# Get dynamic metadata
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
export INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
export AVAILABILITY_ZONE=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/availability-zone)

export ECR_IMAGE="${ECR_URI}:${ECR_IMAGE_TAG}"
export REGISTRY_TABLE="robosystems-graph-${ENVIRONMENT}-instance-registry"

/usr/local/bin/graph-health-check.sh
HEALTHEOF

chmod +x /usr/local/bin/ladybug-health-check-wrapper.sh

# Start cron service
systemctl enable crond
systemctl start crond

# Add health check to crontab (every 5 minutes)
echo "*/5 * * * * /usr/local/bin/ladybug-health-check-wrapper.sh >> /var/log/ladybug-health-check.log 2>&1" | crontab -

# Setup log rotation
cat > /etc/logrotate.d/ladybug << EOF
/var/log/ladybug-*.log {
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
# COMPLETION & CFN SIGNAL
# ==================================================================================
echo ""
echo "✅ LadybugDB shared replica setup completed successfully at $(date)"
echo "Instance ID: ${INSTANCE_ID}"
echo "Node Type: ${LBUG_NODE_TYPE}"
echo "Shared Repositories: ${SHARED_REPOSITORIES}"
echo "Private IP: ${PRIVATE_IP}"
echo ""
echo "Shared scripts used:"
echo "  - setup-cloudwatch-graph.sh"
echo "  - register-graph-instance.sh"
echo "  - run-graph-container.sh"
echo "  - graph-health-check.sh"

# Signal CloudFormation that we're ready
# Note: CFN_SIGNAL_URL is set by CloudFormation substitution in the wrapper
if [ -n "${CFN_STACK_NAME}" ] && [ -n "${CFN_RESOURCE}" ]; then
  echo "Signaling CloudFormation success..."
  /opt/aws/bin/cfn-signal -e 0 --stack ${CFN_STACK_NAME} --resource ${CFN_RESOURCE} --region ${AWS_REGION}
fi
