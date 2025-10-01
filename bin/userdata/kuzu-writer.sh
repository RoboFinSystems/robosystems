#!/bin/bash
# Kuzu Writer Instance UserData Script
# This script is uploaded to S3 and executed during instance initialization

set -e
set -o pipefail

# Create log file for setup process
LOG_FILE="/var/log/kuzu-writer-setup.log"

# Configuration - these should be set as environment variables by CloudFormation
# Validate required environment variables
: ${ENVIRONMENT:?"ENVIRONMENT variable must be set"}
: ${KUZU_NODE_TYPE:?"KUZU_NODE_TYPE variable must be set"}
# REPOSITORY_TYPE is required for single-repo writers, SHARED_REPOSITORIES for multi-repo shared writers
if [ "${KUZU_NODE_TYPE}" = "shared_master" ] || [ "${KUZU_NODE_TYPE}" = "shared_replica" ]; then
    : ${SHARED_REPOSITORIES:?"SHARED_REPOSITORIES variable must be set for shared writers"}
else
    : ${REPOSITORY_TYPE:?"REPOSITORY_TYPE variable must be set for user writers"}
fi

# Optional variables with defaults
INSTANCE_TYPE="${INSTANCE_TYPE:-c6g.medium}"
KUZU_PORT="${KUZU_PORT:-8001}"
# Additional variables needed (passed from CloudFormation)
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
ACCOUNT_ID="${AWS_ACCOUNT_ID:?"AWS_ACCOUNT_ID must be set"}"
ECR_URI="${ECR_URI:?"ECR_URI must be set"}"
ECR_IMAGE_TAG="${ECR_IMAGE_TAG:?"ECR_IMAGE_TAG must be set"}"
SHARED_INSTANCE_NAME="${SHARED_INSTANCE_NAME:-shared-writer}"
DLM_ROLE_ARN="${DLMServiceRoleArn:-}"
# Set CloudWatch namespace with environment suffix
ENV_CAPITALIZED=$(echo "${ENVIRONMENT}" | awk '{print toupper(substr($0,1,1)) tolower(substr($0,2))}')
CLOUDWATCH_NAMESPACE="${CloudWatchNamespace:-RoboSystemsKuzu/${ENV_CAPITALIZED}}"

# Logging setup - append to the setup log
exec > >(tee -a "$LOG_FILE")
exec 2>&1
echo "Starting Kuzu writer setup at $(date)"

# Update system packages
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

# Get instance metadata with IMDSv2
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
AVAILABILITY_ZONE=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/availability-zone)
PRIVATE_IP=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)

echo "Instance setup: ID=${INSTANCE_ID}, AZ=${AVAILABILITY_ZONE}, IP=${PRIVATE_IP}"

# Create data directory and mount point
echo "Setting up storage..."
mkdir -p /mnt/kuzu-data

# NEW: Request volume attachment from Volume Manager
echo "Requesting volume attachment from Volume Manager..."
DATABASES=""
if [ "${KUZU_NODE_TYPE}" = "shared_master" ]; then
  # For shared master, specify SEC database
  DATABASES='["sec"]'
fi

# Get instance availability zone
AZ=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone)

# Prepare Lambda payload
PAYLOAD="{
  \"action\": \"instance_launch\",
  \"instance_id\": \"${INSTANCE_ID}\",
  \"node_type\": \"${KUZU_NODE_TYPE}\",
  \"tier\": \"${WRITER_TIER:-shared}\",
  \"availability_zone\": \"${AZ}\",
  \"databases\": ${DATABASES:-[]}
}"

# Lambda CLI v2 requires base64-encoded payload
ENCODED_PAYLOAD=$(echo -n "$PAYLOAD" | base64)

# Invoke Volume Manager Lambda to attach volume
aws lambda invoke \
  --function-name "RoboSystemsKuzuVolumes${ENVIRONMENT^}-volume-manager" \
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
# The data volume should be attached as the second disk
# On Nitro instances (c5, m5, r5, etc), this will be /dev/nvme1n1
# On older instances, this might be /dev/xvdf
DATA_DEVICE=""
COUNTER=0
MAX_WAIT=120  # Increased wait time for volume attachment
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
# Main directories
mkdir -p /mnt/kuzu-data/{databases,backups,logs}

# Create the Kuzu database directory
# The container mounts /mnt/kuzu-data/databases as /app/data/kuzu-dbs
# Kuzu databases are single files (e.g., sec.kuzu) created on-demand during ingestion
# No subdirectories needed - databases are created directly in /mnt/kuzu-data/databases/
mkdir -p /mnt/kuzu-data/databases

# Docker containers typically run as UID 1000, set ownership accordingly
# This allows the container to write without using 777 permissions
chown -R 1000:1000 /mnt/kuzu-data
chmod -R 755 /mnt/kuzu-data

# Ensure the database directory is writable by the container user
chmod 775 /mnt/kuzu-data/databases

# Set up tagging for EBS snapshots
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
# For shared repositories, use "shared" in the data volume name instead of "shared_master"
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

# Determine log group name based on node type and tier for CloudWatch agent
if [ "${KUZU_NODE_TYPE}" = "writer" ] && [ -n "${WRITER_TIER}" ]; then
  # User writers with tier (standard/enterprise/premium)
  CW_LOG_GROUP_NAME="/robosystems/${ENVIRONMENT}/kuzu-writer-${WRITER_TIER}"
elif [ "${KUZU_NODE_TYPE}" = "shared_master" ]; then
  CW_LOG_GROUP_NAME="/robosystems/${ENVIRONMENT}/kuzu-shared-master"
elif [ "${KUZU_NODE_TYPE}" = "shared_replica" ]; then
  CW_LOG_GROUP_NAME="/robosystems/${ENVIRONMENT}/kuzu-shared-replica"
else
  # Fallback to generic name
  CW_LOG_GROUP_NAME="/robosystems/${ENVIRONMENT}/kuzu-${KUZU_NODE_TYPE//_/-}"
fi

# Configure CloudWatch Agent
echo "Configuring CloudWatch Agent..."
echo "Environment variable: ${ENVIRONMENT}"
echo "Log group: ${CW_LOG_GROUP_NAME}"
cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << EOF
{
  "agent": {
    "metrics_collection_interval": 60,
    "run_as_user": "cwagent"
  },
  "metrics": {
    "namespace": "${CLOUDWATCH_NAMESPACE}",
    "metrics_collected": {
      "cpu": {
        "measurement": [
          {
            "name": "cpu_usage_idle",
            "rename": "CPU_USAGE_IDLE",
            "unit": "Percent"
          },
          {
            "name": "cpu_usage_iowait",
            "rename": "CPU_USAGE_IOWAIT",
            "unit": "Percent"
          },
          {
            "name": "cpu_usage_active",
            "rename": "CPU_USAGE_ACTIVE",
            "unit": "Percent"
          },
          "cpu_time_guest"
        ],
        "totalcpu": true,
        "metrics_collection_interval": 60
      },
      "disk": {
        "measurement": [
          {
            "name": "used_percent",
            "rename": "DISK_USED_PERCENT",
            "unit": "Percent"
          },
          "used",
          "total"
        ],
        "metrics_collection_interval": 60,
        "resources": [
          "/mnt/kuzu-data"
        ]
      },
      "diskio": {
        "measurement": [
          "io_time",
          "read_bytes",
          "write_bytes"
        ],
        "metrics_collection_interval": 60,
        "resources": [
          "*"
        ]
      },
      "mem": {
        "measurement": [
          "mem_used_percent"
        ],
        "metrics_collection_interval": 60
      },
      "netstat": {
        "measurement": [
          "tcp_established",
          "tcp_time_wait"
        ],
        "metrics_collection_interval": 60
      }
    },
    "append_dimensions": {
      "InstanceId": "\${aws:InstanceId}",
      "InstanceType": "\${aws:InstanceType}",
      "NodeType": "${KUZU_NODE_TYPE}",
      "Environment": "${ENVIRONMENT}"
    }
  },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/mnt/kuzu-data/logs/*.log",
            "log_group_name": "${CW_LOG_GROUP_NAME}",
            "log_stream_name": "{instance_id}/application",
            "retention_in_days": 30
          },
          {
            "file_path": "/var/log/kuzu-writer-setup.log",
            "log_group_name": "${CW_LOG_GROUP_NAME}",
            "log_stream_name": "{instance_id}/setup",
            "retention_in_days": 30
          }
        ]
      }
    }
  }
}
EOF

# Start CloudWatch agent (don't fail if it errors)
echo "Starting CloudWatch Agent..."
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json || {
    echo "WARNING: CloudWatch Agent failed to start, but continuing with setup"
    echo "Error was: $?"
}

# Install ADOT Collector for OpenTelemetry metrics
echo "Installing ADOT Collector..."

# Download and install ADOT Collector
# Detect architecture
ARCH=$(uname -m)
if [ "$ARCH" = "x86_64" ]; then
  ADOT_ARCH="amd64"
elif [ "$ARCH" = "aarch64" ]; then
  ADOT_ARCH="arm64"
else
  echo "WARNING: Unsupported architecture $ARCH for ADOT collector"
  ADOT_ARCH=""
fi

if [ -n "$ADOT_ARCH" ]; then
  echo "Downloading ADOT collector for $ADOT_ARCH architecture..."
  wget -O /tmp/aws-otel-collector.rpm https://aws-otel-collector.s3.amazonaws.com/amazon_linux/${ADOT_ARCH}/latest/aws-otel-collector.rpm || {
    echo "WARNING: Failed to download ADOT collector, continuing without OTEL support"
  }
else
  echo "WARNING: Skipping ADOT collector installation due to unsupported architecture"
fi

if [ -f /tmp/aws-otel-collector.rpm ]; then
  rpm -Uvh /tmp/aws-otel-collector.rpm || {
    echo "WARNING: Failed to install ADOT collector, continuing without OTEL support"
  }

  # Create ADOT configuration if installation succeeded
  if [ -d /opt/aws/aws-otel-collector ]; then
    # Try to get Prometheus endpoint if available
    PROMETHEUS_ENDPOINT=$(aws cloudformation describe-stacks \
      --stack-name ${ENVIRONMENT}-prometheus \
      --query "Stacks[0].Outputs[?OutputKey=='PrometheusWorkspaceEndpoint'].OutputValue" \
      --output text \
      --region ${REGION} 2>/dev/null || echo "")

    if [ -z "$PROMETHEUS_ENDPOINT" ]; then
      echo "No Prometheus endpoint found, using logging-only configuration"
      # Create basic config without Prometheus export
      cat > /opt/aws/aws-otel-collector/etc/config.yaml << ADOT_CONFIG
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:
    timeout: 10s
    send_batch_size: 50
  resource:
    attributes:
    - key: service.name
      value: kuzu-${KUZU_NODE_TYPE}
      action: upsert
    - key: deployment.environment
      value: ${ENVIRONMENT}
      action: upsert

exporters:
  debug:
    verbosity: normal

service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [batch, resource]
      exporters: [debug]
  telemetry:
    logs:
      level: info
    metrics:
      address: 0.0.0.0:8888
ADOT_CONFIG
    else
      echo "Found Prometheus endpoint: $PROMETHEUS_ENDPOINT"
      # Create config with Prometheus export
      cat > /opt/aws/aws-otel-collector/etc/config.yaml << ADOT_CONFIG_FULL
extensions:
  sigv4auth:
    region: ${REGION}
    service: aps

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:
    timeout: 10s
    send_batch_size: 50
  resource:
    attributes:
    - key: service.name
      value: kuzu-${KUZU_NODE_TYPE}
      action: upsert
    - key: deployment.environment
      value: ${ENVIRONMENT}
      action: upsert
    - key: instance.id
      value: ${INSTANCE_ID}
      action: upsert
    - key: availability.zone
      value: ${AVAILABILITY_ZONE}
      action: upsert

exporters:
  prometheusremotewrite:
    endpoint: ${PROMETHEUS_ENDPOINT}api/v1/remote_write
    auth:
      authenticator: sigv4auth
    timeout: 30s
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s

  debug:
    verbosity: normal

service:
  extensions: [sigv4auth]
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [batch, resource]
      exporters: [prometheusremotewrite, debug]
  telemetry:
    logs:
      level: info
    metrics:
      address: 0.0.0.0:8888
ADOT_CONFIG_FULL
    fi

    # Enable and start ADOT Collector service
    systemctl enable aws-otel-collector || true
    systemctl start aws-otel-collector || {
      echo "WARNING: ADOT Collector failed to start"
      journalctl -u aws-otel-collector --no-pager -n 20
    }

    # Verify service is running
    sleep 5
    if systemctl is-active --quiet aws-otel-collector; then
      echo "✅ ADOT Collector started successfully"
    else
      echo "❌ ADOT Collector is not running"
    fi
  fi
fi

# Download and setup lifecycle management script
echo "Setting up lifecycle management..."
aws s3 cp s3://robosystems-${ENVIRONMENT}-deployment/scripts/kuzu-lifecycle.sh /usr/local/bin/kuzu-lifecycle.sh || {
  echo "ERROR: Could not download lifecycle script from S3"
  exit 1
}

chmod +x /usr/local/bin/kuzu-lifecycle.sh

# Setup systemd service for lifecycle management
cat > /etc/systemd/system/kuzu-lifecycle.service << EOF
[Unit]
Description=Kuzu Instance Lifecycle Manager
After=docker.service
Requires=docker.service

[Service]
Type=simple
ExecStart=/usr/local/bin/kuzu-lifecycle.sh monitor
Restart=always
RestartSec=30
Environment="ENVIRONMENT=${ENVIRONMENT}"
Environment="AWS_REGION=${REGION}"
Environment="KUZU_NODE_TYPE=${KUZU_NODE_TYPE}"

[Install]
WantedBy=multi-user.target
EOF

systemctl enable kuzu-lifecycle.service
systemctl start kuzu-lifecycle.service

# Get ASG name for cluster grouping
ASG_NAME=$(aws ec2 describe-instances \
  --instance-ids ${INSTANCE_ID} \
  --query 'Reservations[0].Instances[0].Tags[?Key==`aws:autoscaling:groupName`].Value' \
  --output text --region ${REGION} || echo "unknown")

# Determine instance tier from stack name
echo "Determining instance tier..."
STACK_NAME=$(aws cloudformation describe-stacks \
  --region ${REGION} \
  --query "Stacks[?contains(Tags[?Key=='aws:cloudformation:stack-name'].Value, '${INSTANCE_ID}')].StackName" \
  --output text 2>/dev/null || echo "unknown")

# If we can't find by instance ID, try getting from instance tags
if [ "$STACK_NAME" = "unknown" ] || [ -z "$STACK_NAME" ]; then
  STACK_NAME=$(aws ec2 describe-tags \
    --region ${REGION} \
    --filters "Name=resource-id,Values=${INSTANCE_ID}" "Name=key,Values=aws:cloudformation:stack-name" \
    --query 'Tags[0].Value' --output text 2>/dev/null || echo "unknown")
fi

# Tier is already set from CloudFormation as CLUSTER_TIER environment variable
# The container will load tier-specific configuration from kuzu.yml
echo "Using tier: ${CLUSTER_TIER}"

echo "Stack name: ${STACK_NAME}"

# Register instance in DynamoDB with full regional support
echo "Registering instance in DynamoDB..."
if [ "${KUZU_NODE_TYPE}" = "writer" ]; then
  # User database writer registration (standard/enterprise/premium)
  aws dynamodb put-item \
    --table-name robosystems-kuzu-${ENVIRONMENT}-instance-registry \
    --item "{
      \"instance_id\": {\"S\": \"${INSTANCE_ID}\"},
      \"private_ip\": {\"S\": \"${PRIVATE_IP}\"},
      \"availability_zone\": {\"S\": \"${AVAILABILITY_ZONE}\"},
      \"instance_type\": {\"S\": \"${INSTANCE_TYPE}\"},
      \"node_type\": {\"S\": \"${KUZU_NODE_TYPE}\"},
      \"status\": {\"S\": \"initializing\"},
      \"created_at\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"},
      \"last_health_check\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"},
      \"database_count\": {\"N\": \"0\"},
      \"region\": {\"S\": \"${REGION}\"},
      \"cluster_tier\": {\"S\": \"${CLUSTER_TIER}\"},
      \"cluster_group\": {\"S\": \"${REGION}-writers-${ASG_NAME}\"},
      \"available_capacity_pct\": {\"N\": \"100\"},
      \"private_dns\": {\"S\": \"$(hostname -f)\"},
      \"endpoint_url\": {\"S\": \"http://${PRIVATE_IP}:${KUZU_PORT}\"},
      \"launch_time\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"},
      \"replication_role\": {\"S\": \"none\"},
      \"total_size_gb\": {\"N\": \"0\"},
      \"cpu_utilization\": {\"N\": \"0\"},
      \"memory_utilization\": {\"N\": \"0\"},
      \"iops_utilization\": {\"N\": \"0\"},
      \"vpc_id\": {\"S\": \"$(curl -s http://169.254.169.254/latest/meta-data/network/interfaces/macs/$(curl -s http://169.254.169.254/latest/meta-data/mac)/vpc-id)\"}
    }" \
    --region ${REGION}
else
  # Shared writer registration
  # Count number of repositories for shared writers
  if [ -n "${SHARED_REPOSITORIES}" ]; then
    REPO_COUNT=$(echo ${SHARED_REPOSITORIES} | tr ',' '\n' | grep -c .)
  else
    REPO_COUNT=1
  fi

  aws dynamodb put-item \
    --table-name robosystems-kuzu-${ENVIRONMENT}-instance-registry \
    --item "{
      \"instance_id\": {\"S\": \"${INSTANCE_ID}\"},
      \"private_ip\": {\"S\": \"${PRIVATE_IP}\"},
      \"availability_zone\": {\"S\": \"${AVAILABILITY_ZONE}\"},
      \"instance_type\": {\"S\": \"${INSTANCE_TYPE}\"},
      \"node_type\": {\"S\": \"${KUZU_NODE_TYPE}\"},
      \"repository_type\": {\"S\": \"${REPOSITORY_TYPE:-shared}\"},
      \"shared_repositories\": {\"S\": \"${SHARED_REPOSITORIES:-}\"},
      \"status\": {\"S\": \"initializing\"},
      \"created_at\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"},
      \"last_health_check\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"},
      \"database_count\": {\"N\": \"${REPO_COUNT}\"},
      \"max_databases\": {\"N\": \"${REPO_COUNT}\"},
      \"region\": {\"S\": \"${REGION}\"},
      \"cluster_tier\": {\"S\": \"${CLUSTER_TIER}\"},
      \"cluster_group\": {\"S\": \"${REGION}-shared-writers\"},
      \"available_capacity_pct\": {\"N\": \"0\"},
      \"private_dns\": {\"S\": \"$(hostname -f)\"},
      \"endpoint_url\": {\"S\": \"http://${PRIVATE_IP}:${KUZU_PORT}\"},
      \"launch_time\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"},
      \"replication_role\": {\"S\": \"none\"},
      \"total_size_gb\": {\"N\": \"0\"},
      \"cpu_utilization\": {\"N\": \"0\"},
      \"memory_utilization\": {\"N\": \"0\"},
      \"iops_utilization\": {\"N\": \"0\"},
      \"vpc_id\": {\"S\": \"$(curl -s http://169.254.169.254/latest/meta-data/network/interfaces/macs/$(curl -s http://169.254.169.254/latest/meta-data/mac)/vpc-id)\"}
    }" \
    --region ${REGION}
fi

# Pull and run Kuzu container
echo "Starting Kuzu writer container..."
docker pull ${ECR_URI}:${ECR_IMAGE_TAG}

# Create container run script
cat > /usr/local/bin/run-kuzu-writer.sh << EOF
#!/bin/bash
set -e

# Stop any existing container
CONTAINER_NAME=\$(if [ "${KUZU_NODE_TYPE}" = "shared_master" ] || [ "${KUZU_NODE_TYPE}" = "shared_replica" ]; then echo "kuzu-shared-writer"; else echo "kuzu-writer"; fi)
docker stop \$CONTAINER_NAME 2>/dev/null || true
docker rm \$CONTAINER_NAME 2>/dev/null || true

# Create CloudWatch log group if it doesn't exist
# Log group name is embedded from the parent script
aws logs create-log-group --log-group-name "${CW_LOG_GROUP_NAME}" --region ${REGION} 2>/dev/null || true

# Calculate memory limits dynamically based on available memory
# Leave 2GB for OS on instances with >8GB, 1GB for smaller instances
TOTAL_MEMORY_KB=\$(grep MemTotal /proc/meminfo | awk '{print \$2}')
TOTAL_MEMORY_GB=\$((TOTAL_MEMORY_KB / 1024 / 1024))

if [ \${TOTAL_MEMORY_GB} -gt 8 ]; then
  # Leave 2GB for OS on larger instances
  MEMORY_LIMIT="\$((\${TOTAL_MEMORY_GB} - 2))g"
  MEMORY_RESERVATION="\$((\${TOTAL_MEMORY_GB} - 3))g"
elif [ \${TOTAL_MEMORY_GB} -gt 4 ]; then
  # Leave 1GB for OS on medium instances
  MEMORY_LIMIT="\$((\${TOTAL_MEMORY_GB} - 1))g"
  MEMORY_RESERVATION="\$((\${TOTAL_MEMORY_GB} - 2))g"
else
  # Don't set limits on very small instances
  MEMORY_LIMIT=""
  MEMORY_RESERVATION=""
fi

echo "System has \${TOTAL_MEMORY_GB}GB total memory"

# Build memory flags if limits are set
MEMORY_FLAGS=""
if [ -n "\${MEMORY_LIMIT}" ]; then
  MEMORY_FLAGS="--memory=\${MEMORY_LIMIT} --memory-reservation=\${MEMORY_RESERVATION}"
  echo "Setting container memory limits: \${MEMORY_LIMIT} (reservation: \${MEMORY_RESERVATION})"
fi

# Run the container with CloudWatch logging
docker run -d \
  --name \${CONTAINER_NAME} \
  --restart unless-stopped \
  --health-cmd="timeout 10 curl -f http://localhost:${KUZU_PORT}/health || exit 1" \
  --health-interval=120s \
  --health-timeout=30s \
  --health-retries=20 \
  --health-start-period=300s \
  \${MEMORY_FLAGS} \
  --log-driver awslogs \
  --log-opt awslogs-region=${REGION} \
  --log-opt awslogs-group="${CW_LOG_GROUP_NAME}" \
  --log-opt awslogs-stream="${INSTANCE_ID}/${PRIVATE_IP}/${KUZU_NODE_TYPE}" \
  --log-opt awslogs-create-group=false \
  -p ${KUZU_PORT}:${KUZU_PORT} \
  -v /mnt/kuzu-data/databases:/app/data/kuzu-dbs \
  -v /mnt/kuzu-data/logs:/app/logs \
  -e ENVIRONMENT=${ENVIRONMENT} \
  -e KUZU_NODE_TYPE=${KUZU_NODE_TYPE} \
  -e REPOSITORY_TYPE=${REPOSITORY_TYPE:-shared} \
  -e SHARED_REPOSITORIES="${SHARED_REPOSITORIES:-}" \
  -e INSTANCE_ID=${INSTANCE_ID} \
  -e INSTANCE_IP=${PRIVATE_IP} \
  -e INSTANCE_AZ=${AVAILABILITY_ZONE} \
  -e INSTANCE_TYPE=${INSTANCE_TYPE} \
  -e CLUSTER_TIER="${CLUSTER_TIER}" \
  -e AWS_REGION=${REGION} \
  -e KUZU_DATABASE_PATH=/app/data/kuzu-dbs \
  -e LOG_LEVEL=INFO \
  -e OTEL_ENABLED=false \
  -e OTEL_SERVICE_NAME=kuzu-writer-${KUZU_NODE_TYPE} \
  -e OTEL_EXPORTER_OTLP_ENDPOINT=http://172.17.0.1:4318 \
  -e DOCKER_PROFILE=\$(if [ "${KUZU_NODE_TYPE}" = "shared_master" ] || [ "${KUZU_NODE_TYPE}" = "shared_replica" ]; then echo "kuzu-shared-writer"; else echo "kuzu-writer"; fi) \
  -e KUZU_PORT=${KUZU_PORT} \
  -e KUZU_ROLE=\$(if [ "${KUZU_NODE_TYPE}" = "shared_replica" ]; then echo "replica"; else echo "master"; fi) \
  -e KUZU_ACCESS_PATTERN=api_writer \
  ${ECR_URI}:${ECR_IMAGE_TAG} \
  /app/bin/entrypoint.sh || {
    echo "ERROR: Failed to start Docker container"
    docker logs \$CONTAINER_NAME || true
    exit 1
}

# Wait for container to be healthy
echo "Waiting for Kuzu writer container to become healthy..."
echo "Checking health on port ${KUZU_PORT}..."
# Shared writers may need more time to initialize
if [ "${KUZU_NODE_TYPE}" = "shared_master" ] || [ "${KUZU_NODE_TYPE}" = "shared_replica" ]; then
  echo "Shared writer detected, waiting additional time for initialization..."
  sleep 30
else
  sleep 10
fi
HEALTH_CHECK_PASSED=false
for i in {1..12}; do  # 12 * 5 = 60 seconds (reduced from 150)
  if curl -f http://localhost:${KUZU_PORT}/health >/dev/null 2>&1; then
    echo "Kuzu writer is healthy on port ${KUZU_PORT}"
    HEALTH_CHECK_PASSED=true
    break
  fi
  echo "Waiting for Kuzu writer to start on port ${KUZU_PORT}... (\$i/12)"
  # Check if container is still running
  if ! docker ps | grep -q \$CONTAINER_NAME; then
    echo "ERROR: Container \$CONTAINER_NAME is not running"
    docker logs \$CONTAINER_NAME || true
    break
  fi
  sleep 5
done

if [ "\$HEALTH_CHECK_PASSED" != "true" ]; then
  echo "ERROR: Kuzu writer failed to become healthy after 60 seconds"
  echo "Container logs:"
  docker logs \$CONTAINER_NAME || true
  exit 1
fi
EOF

chmod +x /usr/local/bin/run-kuzu-writer.sh
/usr/local/bin/run-kuzu-writer.sh

# Update instance status to healthy
echo "Marking instance as healthy..."
aws dynamodb update-item \
  --table-name robosystems-kuzu-${ENVIRONMENT}-instance-registry \
  --key "{\"instance_id\": {\"S\": \"${INSTANCE_ID}\"}}" \
  --update-expression "SET #status = :status" \
  --expression-attribute-names '{"#status": "status"}' \
  --expression-attribute-values '{":status": {"S": "healthy"}}' \
  --region ${REGION}

# Setup SIMPLIFIED health check cron job
# IMPORTANT: During ingestion, Kuzu can be unresponsive for HOURS while processing large tables
# This simplified version prioritizes the Redis ingestion flag over API checks
cat > /usr/local/bin/kuzu-health-check.sh << EOF
#!/bin/bash
# Simplified health check for Kuzu writer
# Checks Redis ingestion flag first - if active, always healthy
# Otherwise checks container status only

# Get instance metadata
TOKEN=\$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=\$(curl -s -H "X-aws-ec2-metadata-token: \$TOKEN" http://169.254.169.254/latest/meta-data/instance-id)

# Determine container name based on node type
CONTAINER_NAME=\$(if [ "${KUZU_NODE_TYPE}" = "shared_master" ] || [ "${KUZU_NODE_TYPE}" = "shared_replica" ]; then echo "kuzu-shared-writer"; else echo "kuzu-writer"; fi)

# Check if active ingestion is happening (via Redis flag)
# If ingestion is active, ALWAYS mark as healthy regardless of other checks
INGESTION_ACTIVE="false"
if command -v redis6-cli &> /dev/null; then
  REDIS_URL="${VALKEY_URL:-redis://localhost:6379}"
  REDIS_HOST=\$(echo \$REDIS_URL | sed 's|redis://||' | cut -d: -f1)
  REDIS_PORT=\$(echo \$REDIS_URL | sed 's|redis://||' | cut -d: -f2 | cut -d/ -f1)
  
  INGESTION_FLAG=\$(redis6-cli -h \$REDIS_HOST -p \$REDIS_PORT GET "kuzu:ingestion:active:\${INSTANCE_ID}" 2>/dev/null || echo "")
  if [ -n "\$INGESTION_FLAG" ]; then
    INGESTION_ACTIVE="true"
    # Extract table name if possible
    TABLE_NAME=\$(echo "\$INGESTION_FLAG" | grep -o '"table_name":"[^"]*"' | cut -d'"' -f4 || echo "unknown")
    echo "[\$(date)] Active ingestion detected for table: \$TABLE_NAME - marking as healthy"
    HEALTH_STATUS="healthy"
  fi
fi

# Only check container status if not actively ingesting
if [ "\$INGESTION_ACTIVE" = "false" ]; then
  if docker ps | grep -q \$CONTAINER_NAME; then
    HEALTH_STATUS="healthy"
    echo "[\$(date)] Container \$CONTAINER_NAME is running - marking as healthy"
  else
    HEALTH_STATUS="unhealthy"
    echo "[\$(date)] Container \$CONTAINER_NAME is NOT running - marking as unhealthy"
    # Try to restart container once
    echo "[\$(date)] Attempting to restart container..."
    /usr/local/bin/run-kuzu-writer.sh
  fi
fi

# Update DynamoDB with current status
aws dynamodb update-item \
  --table-name robosystems-kuzu-${ENVIRONMENT}-instance-registry \
  --key "{\"instance_id\": {\"S\": \"\${INSTANCE_ID}\"}}" \
  --update-expression "SET #status = :status, last_health_check = :time" \
  --expression-attribute-names '{"#status": "status"}' \
  --expression-attribute-values "{\":status\": {\"S\": \"\${HEALTH_STATUS}\"}, \":time\": {\"S\": \"\$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}" \
  --region ${REGION} >/dev/null 2>&1

echo "[\$(date)] Health check complete: \${HEALTH_STATUS}"
EOF

chmod +x /usr/local/bin/kuzu-health-check.sh

# Start cronie service
systemctl enable crond
systemctl start crond

# Add health check to crontab
echo "*/5 * * * * /usr/local/bin/kuzu-health-check.sh >> /var/log/kuzu-health-check.log 2>&1" | crontab -

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

# CloudWatch logging is now handled directly by Docker's awslogs driver
echo "CloudWatch logging configured via Docker awslogs driver"

# Final status
echo "Kuzu writer setup completed successfully at $(date)"
echo "Instance ID: ${INSTANCE_ID}"
echo "Node Type: ${KUZU_NODE_TYPE}"
echo "Repository Type: ${REPOSITORY_TYPE:-shared}"
echo "Shared Repositories: ${SHARED_REPOSITORIES:-N/A}"
echo "Private IP: ${PRIVATE_IP}"

# Script completed successfully
# Note: Do not use 'exit' here as this script is sourced by CloudFormation UserData
