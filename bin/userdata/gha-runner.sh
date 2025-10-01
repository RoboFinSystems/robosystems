#!/bin/bash -xe
exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

# Error notification function
notify_error() {
  echo "ERROR: GHA Runner setup failed at $(date)"
  echo "Check /var/log/user-data.log and /var/log/cloud-init-output.log for details"
  # Could add SNS notification here if needed
}

# Set up error trap
trap 'notify_error' ERR

echo "=== GHA Runner Setup Started at $(date) ==="

# Update system and install core dependencies
echo "Updating system packages..."
dnf update -y --allowerasing || { echo "Failed to update system"; exit 1; }

echo "Installing core dependencies..."
dnf install -y --allowerasing libicu docker git jq dotnet-runtime-6.0 aspnetcore-runtime-6.0 \
  openssl-libs krb5-libs zlib libstdc++ libgcc python3.12 python3.12-pip nodejs npm cronie || { echo "Failed to install dependencies"; exit 1; }

# Create symlinks for easier access
ln -sf /usr/bin/python3.12 /usr/local/bin/python
ln -sf /usr/bin/python3.12 /usr/local/bin/python3
ln -sf /usr/bin/pip3.12 /usr/local/bin/pip
ln -sf /usr/bin/pip3.12 /usr/local/bin/pip3

# Install uv for Python package management globally
echo "Installing uv package manager..."
python3.12 -m pip install uv || { echo "Failed to install uv"; exit 1; }

# Install PyYAML for configuration parsing in workflows
echo "Installing PyYAML..."
python3.12 -m pip install pyyaml || { echo "Failed to install PyYAML"; exit 1; }

# Install CloudWatch agent for monitoring
echo "Installing CloudWatch agent..."
dnf install -y amazon-cloudwatch-agent || { echo "Failed to install CloudWatch agent"; exit 1; }

# Create global symlink for uv
UV_PATH=$(find /usr/local /usr -name "uv" -type f 2>/dev/null | head -1)
if [ -n "$UV_PATH" ] && [ "$UV_PATH" != "/usr/local/bin/uv" ]; then
  ln -sf "$UV_PATH" /usr/local/bin/uv
else
  echo "uv already available at /usr/local/bin/uv or not found"
fi

# Start Docker
echo "Starting Docker service..."
systemctl enable docker || { echo "Failed to enable Docker"; exit 1; }
systemctl start docker || { echo "Failed to start Docker"; exit 1; }
usermod -aG docker ec2-user || { echo "Failed to add ec2-user to docker group"; exit 1; }

# Start cron service
echo "Starting cron service..."
systemctl enable crond || { echo "Failed to enable cron service"; exit 1; }
systemctl start crond || { echo "Failed to start cron service"; exit 1; }

# Verify Docker is running
sleep 5
docker --version || { echo "Docker verification failed"; exit 1; }
echo "Docker service started successfully"

# Get unique instance identifier for runner naming
INSTANCE_ID=$(ec2-metadata --instance-id 2>/dev/null | cut -d: -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' || echo "unknown")
# Try to get instance type using ec2-metadata (more reliable than curl)
INSTANCE_TYPE=$(ec2-metadata --instance-type 2>/dev/null | cut -d: -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' || echo "unknown")
# Validate we got a real instance type (should match pattern like t3a.medium)
if [[ ! "$INSTANCE_TYPE" =~ ^[a-z0-9]+\.[a-z0-9]+$ ]]; then
  echo "WARNING: Could not detect valid instance type (got: '$INSTANCE_TYPE'), defaulting to 1 runner"
  INSTANCE_TYPE="unknown"
fi

# Determine runner count based on instance size
# For ASG scaling consistency, we keep 1 runner per instance for most types
case "$INSTANCE_TYPE" in
  c7a.large)
    # c7a.large: 2 vCPUs, 4GB RAM - single runner for consistent ASG scaling
    INST_RUNNER_CNT=1
    echo "c7a.large instance: configuring 1 runner for consistent scaling"
    ;;
  *.xlarge|*.2xlarge)
    # Only xlarge and larger get multiple runners
    INST_RUNNER_CNT=2
    echo "Extra large instance ($INSTANCE_TYPE): configuring 2 runners"
    ;;
  *)
    # Default to single runner for medium, other large types, and unknown instances
    INST_RUNNER_CNT=1
    echo "Standard instance ($INSTANCE_TYPE): configuring 1 runner"
    ;;
esac

echo "Setting up $INST_RUNNER_CNT concurrent runners on instance $INSTANCE_ID ($INSTANCE_TYPE)..."

# Create a separate script to handle runner setup with timestamp identifier
cat << 'RUNNER_SETUP_EOF' > /usr/local/bin/setup-gha-runners.sh
#!/bin/bash
set -euo pipefail

# Use MAC address for consistent runner naming (prevents runner accumulation)
MAC_ADDRESS=$(cat /sys/class/net/$(ip route show default | awk '/default/ {print $5}')/address)
UNIQUE_ID=$(echo "$MAC_ADDRESS" | tr -d ':' | tail -c 7)

# Use runner count passed from main script
RUNNER_COUNT=$1

echo "Setting up $RUNNER_COUNT runners with unique ID: $UNIQUE_ID"

# Setup runners as ec2-user
sudo -u ec2-user bash << INNER_EOF
cd /home/ec2-user

for i in \$(seq 1 $RUNNER_COUNT); do
  RUNNER_NAME="robosystems-gha-runner-$UNIQUE_ID-\$i"
  RUNNER_DIR="actions-runner-\$i"

  echo "Setting up runner: \$RUNNER_NAME in \$RUNNER_DIR"

  echo "Setting up \$RUNNER_NAME..."
  mkdir -p \$RUNNER_DIR && cd \$RUNNER_DIR

  # Download runner
  echo "Downloading GitHub Actions runner for \$RUNNER_NAME..."
  curl -o actions-runner.tar.gz -L "https://github.com/actions/runner/releases/download/v2.322.0/actions-runner-linux-x64-2.322.0.tar.gz"
  tar xzf actions-runner.tar.gz && rm actions-runner.tar.gz


  # Get registration token and configure
  echo "Getting registration token for \$RUNNER_NAME..."
  REG_TOKEN=\$(curl -sX POST -H "Authorization: token ${GitHubToken}" "https://api.github.com/orgs/${GitHubOrg}/actions/runners/registration-token" | jq -r .token)

  if [ "\$REG_TOKEN" = "null" ]; then
    echo "Failed to get registration token for \$RUNNER_NAME"
    cd /home/ec2-user
    continue
  fi

  # Get instance metadata for enhanced traceability
  echo "Configuring \$RUNNER_NAME ..."
  ./config.sh --url "https://github.com/${GitHubOrg}" --token "\$REG_TOKEN" --name "\$RUNNER_NAME" --labels "self-hosted,Linux,X64,AL2023,ci" --unattended --replace

  # Log mapping for traceability
  echo "\$(date): RUNNER_MAPPING: \$RUNNER_NAME -> MAC: $MAC_ADDRESS" | logger -t runner-mapping

  cd /home/ec2-user
done
INNER_EOF

# Install and start services for all runners
echo "Installing and starting runner services..."
for i in $(seq 1 $RUNNER_COUNT); do
  RUNNER_DIR="actions-runner-$i"

  cd /home/ec2-user/$RUNNER_DIR

  echo "Installing runner service $i..."
  ./svc.sh install ec2-user
  ./svc.sh start

  # Verify service started
  sleep 2
  if ./svc.sh status; then
    echo "GHA Runner service $i is running successfully"
  else
    echo "Failed to start GHA Runner service $i"
  fi
done
RUNNER_SETUP_EOF

chmod +x /usr/local/bin/setup-gha-runners.sh

# Optimize system for multiple runners
echo "Optimizing system for multiple concurrent runners..."

# Increase file descriptor limits for multiple runners
cat << 'LIMITS_EOF' >> /etc/security/limits.conf
# Optimizations for multiple GitHub Actions runners
ec2-user soft nofile 65536
ec2-user hard nofile 65536
ec2-user soft nproc 32768
ec2-user hard nproc 32768
LIMITS_EOF

# Increase Docker daemon limits
mkdir -p /etc/docker
cat << 'DOCKER_DAEMON_EOF' > /etc/docker/daemon.json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  },
  "storage-driver": "overlay2",
  "max-concurrent-downloads": 3,
  "max-concurrent-uploads": 5
}
DOCKER_DAEMON_EOF

# Restart Docker with new configuration
systemctl restart docker

# Execute the runner setup script
echo "Executing GHA Runner setup script..."
/usr/local/bin/setup-gha-runners.sh $INST_RUNNER_CNT

# Setup CloudWatch agent configuration with instance-specific log streams
cat << EOF > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
{
  "agent": {
    "metrics_collection_interval": 60,
    "run_as_user": "cwagent"
  },
  "metrics": {
    "namespace": "RoboSystemsGHARunner/${Environment}",
    "metrics_collected": {
      "cpu": {
        "measurement": [
          "cpu_usage_idle",
          "cpu_usage_iowait",
          "cpu_usage_user",
          "cpu_usage_system"
        ],
        "metrics_collection_interval": 300,
        "totalcpu": true
      },
      "disk": {
        "measurement": ["used_percent", "used", "free"],
        "metrics_collection_interval": 300,
        "resources": ["/"],
        "drop_device": true
      },
      "mem": {
        "measurement": ["mem_used_percent", "mem_used", "mem_available"],
        "metrics_collection_interval": 300
      },
      "swap": {
        "measurement": ["swap_used_percent"],
        "metrics_collection_interval": 300
      }
    }
  },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/disk-usage.log",
            "log_group_name": "/robosystems/ci/gha-runner",
            "log_stream_name": "${INSTANCE_ID}/disk-usage",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/user-data.log",
            "log_group_name": "/robosystems/ci/gha-runner",
            "log_stream_name": "${INSTANCE_ID}/user-data",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/messages",
            "log_group_name": "/robosystems/ci/gha-runner",
            "log_stream_name": "${INSTANCE_ID}/system",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/cloud-init-output.log",
            "log_group_name": "/robosystems/ci/gha-runner",
            "log_stream_name": "${INSTANCE_ID}/cloud-init",
            "timezone": "UTC"
          },
          {
            "file_path": "/home/ec2-user/actions-runner-*/_diag/*.log",
            "log_group_name": "/robosystems/ci/gha-runner",
            "log_stream_name": "${INSTANCE_ID}/runner-diag",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/gha-runner-activity.log",
            "log_group_name": "/robosystems/ci/gha-runner",
            "log_stream_name": "${INSTANCE_ID}/runner-activity",
            "timezone": "UTC"
          }
        ]
      }
    }
  }
}
EOF

# Start CloudWatch agent
echo "Starting CloudWatch agent..."
systemctl enable amazon-cloudwatch-agent || { echo "Failed to enable CloudWatch agent"; exit 1; }

# Wait a moment for the agent to be ready
sleep 2

# Start the agent with proper error handling
echo "Fetching CloudWatch agent configuration..."
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config \
  -m ec2 \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json \
  -s || {
    echo "Failed to start CloudWatch agent - checking status..."
    systemctl status amazon-cloudwatch-agent
    cat /opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log 2>/dev/null || true
    exit 1
  }

# Verify CloudWatch agent is running
sleep 5
if systemctl is-active --quiet amazon-cloudwatch-agent; then
  echo "CloudWatch agent started successfully"
  # Create initial log entries to test logging
  echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] CloudWatch agent started on instance $INSTANCE_ID" >> /var/log/gha-runner-activity.log
  echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] GHA Runner setup initiated" >> /var/log/gha-runner-activity.log
else
  echo "WARNING: CloudWatch agent may not be running properly"
  systemctl status amazon-cloudwatch-agent
fi

# Setup automatic cleanup cron jobs optimized for multiple runners
echo "Setting up automatic disk cleanup optimized for multiple runners..."
cat << 'CRON_EOF' | crontab -
# Clean Docker containers/volumes more frequently with multiple runners
0 * * * * /usr/bin/docker container prune -f && /usr/bin/docker volume prune -f
# Aggressive cleanup including images every 2 hours for multiple runners
0 */2 * * * /usr/bin/docker system prune -af --volumes
# Clean package caches daily at 3 AM CST (8 AM UTC)
0 8 * * * /usr/bin/dnf clean all
# Log disk usage every 30 minutes for better monitoring with multiple runners
*/30 * * * * /bin/df -h > /var/log/disk-usage.log 2>&1
# Clean up any orphaned test databases/containers every 6 hours
0 */6 * * * /usr/bin/docker ps -q --filter "name=postgres" --filter "name=kuzu" | xargs -r docker rm -f
# Check disk space and alert if low (under 20%)
*/15 * * * * df -h / | awk 'NR==2 {if (100-$5+0 < 20) print "WARNING: Low disk space - only "100-$5"% free on /"}' | logger -t disk-alert
CRON_EOF

# Setup runner service logging and scale-in protection
echo "Setting up runner service logging and scale-in protection..."
cat << 'RUNNER_LOG_EOF' > /usr/local/bin/gha-runner-log-monitor.sh
#!/bin/bash
# Monitor and log GitHub Actions runner activity + manage scale-in protection

LOG_FILE="/var/log/gha-runner-activity.log"
INSTANCE_ID=$(ec2-metadata --instance-id 2>/dev/null | cut -d: -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
ASG_NAME="RoboSystemsGHARunner-gha-runner-asg"
REGION=$(ec2-metadata --availability-zone 2>/dev/null | cut -d: -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed 's/[a-z]$//')

# Function to check if any runner is busy
check_runners_busy() {
  # Only check for Runner.Worker processes - this indicates an active job
  # Runner.Listener is always running (waiting for jobs) and doesn't indicate activity
  if pgrep -f "Runner.Worker" > /dev/null 2>&1; then
    return 0  # At least one runner is busy
  fi

  # All runners are idle
  return 1
}

# Function to set scale-in protection
set_scale_protection() {
  local protect=$1
  if [ "$protect" = "true" ]; then
    aws autoscaling set-instance-protection \
      --instance-ids "$INSTANCE_ID" \
      --auto-scaling-group-name "$ASG_NAME" \
      --protected-from-scale-in \
      --region "$REGION" 2>/dev/null && \
      echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Scale-in protection ENABLED" >> $LOG_FILE
  else
    aws autoscaling set-instance-protection \
      --instance-ids "$INSTANCE_ID" \
      --auto-scaling-group-name "$ASG_NAME" \
      --no-protected-from-scale-in \
      --region "$REGION" 2>/dev/null && \
      echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Scale-in protection DISABLED" >> $LOG_FILE
  fi
}

PROTECTION_STATUS="unknown"
IDLE_MINUTES=0
IDLE_THRESHOLD=5  # Minutes of idle time before removing protection

while true; do
  # Log runner status
  echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Runner Status Check" >> $LOG_FILE

  # Check each runner service
  ACTIVE_RUNNERS=0
  for i in $(seq 1 10); do
    if [ -d "/home/ec2-user/actions-runner-$i" ]; then
      cd "/home/ec2-user/actions-runner-$i"
      if ./svc.sh status >/dev/null 2>&1; then
        echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Runner $i: RUNNING" >> $LOG_FILE
        ACTIVE_RUNNERS=$((ACTIVE_RUNNERS + 1))
      else
        echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Runner $i: STOPPED" >> $LOG_FILE
      fi
    fi
  done

  # Check if runners are busy and manage scale-in protection
  if check_runners_busy; then
    IDLE_MINUTES=0  # Reset idle counter
    if [ "$PROTECTION_STATUS" != "protected" ]; then
      set_scale_protection true
      PROTECTION_STATUS="protected"
      echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Runners are BUSY - Protection enabled" >> $LOG_FILE
    fi
  else
    IDLE_MINUTES=$((IDLE_MINUTES + 1))
    echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Runners are IDLE for $IDLE_MINUTES minutes" >> $LOG_FILE

    # Only remove protection after being idle for threshold minutes
    if [ $IDLE_MINUTES -ge $IDLE_THRESHOLD ]; then
      if [ "$PROTECTION_STATUS" != "unprotected" ]; then
        set_scale_protection false
        PROTECTION_STATUS="unprotected"
        echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Runners IDLE for $IDLE_THRESHOLD+ minutes - Protection disabled" >> $LOG_FILE
      fi
    fi
  fi

  # Log disk usage
  DISK_USAGE=$(df -h / | awk 'NR==2 {print $5}')
  echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Disk Usage: $DISK_USAGE" >> $LOG_FILE

  # Log Docker status
  if systemctl is-active --quiet docker; then
    CONTAINER_COUNT=$(docker ps -q | wc -l)
    echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Docker: RUNNING, Containers: $CONTAINER_COUNT" >> $LOG_FILE
  else
    echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Docker: STOPPED" >> $LOG_FILE
  fi

  # Sleep for 1 minute (more frequent checks for protection management)
  sleep 60
done
RUNNER_LOG_EOF

chmod +x /usr/local/bin/gha-runner-log-monitor.sh

# Start the log monitor as a background service
cat << 'SYSTEMD_LOG_EOF' > /etc/systemd/system/gha-runner-log-monitor.service
[Unit]
Description=GHA Runner Activity Logger
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/gha-runner-log-monitor.sh
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
SYSTEMD_LOG_EOF

systemctl enable gha-runner-log-monitor.service
systemctl start gha-runner-log-monitor.service

# Create health check script for ASG
cat << 'HEALTH_CHECK_EOF' > /usr/local/bin/gha-runner-health-check.sh
#!/bin/bash
# Health check script for GitHub Actions runners

# Check how many runners are actually running
RUNNING_COUNT=$(ps aux | grep -c "Runner.Listener" || echo 0)
# Subtract 1 for the grep process itself
RUNNING_COUNT=$((RUNNING_COUNT - 1))

if [ $RUNNING_COUNT -eq 0 ]; then
    echo "UNHEALTHY: No runners are running"
    exit 1
fi

echo "HEALTHY: $RUNNING_COUNT runner(s) active"
exit 0
HEALTH_CHECK_EOF

chmod +x /usr/local/bin/gha-runner-health-check.sh

# Create shutdown cleanup script
cat << 'SHUTDOWN_EOF' > /usr/local/bin/gha-runner-shutdown.sh
#!/bin/bash
# Cleanup script to run on instance termination

echo "Starting GHA runner cleanup..."

# Stop all runner services
for i in $(seq 1 10); do
  if [ -d "/home/ec2-user/actions-runner-$i" ]; then
    cd "/home/ec2-user/actions-runner-$i"
    ./svc.sh stop || true
    ./svc.sh uninstall || true

    # Note: Cannot remove runner from GitHub at shutdown as we don't have the token
    # Runners will be automatically cleaned up by GitHub after being offline
  fi
done

echo "GHA runner cleanup completed"
SHUTDOWN_EOF

chmod +x /usr/local/bin/gha-runner-shutdown.sh

# Register shutdown script with systemd
cat << 'SYSTEMD_EOF' > /etc/systemd/system/gha-runner-cleanup.service
[Unit]
Description=GHA Runner Cleanup on Shutdown
DefaultDependencies=no
Before=shutdown.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/gha-runner-shutdown.sh
TimeoutStartSec=0

[Install]
WantedBy=shutdown.target
SYSTEMD_EOF

systemctl enable gha-runner-cleanup.service

# Tag EBS volumes after instance is fully created
echo "Tagging EBS volumes..."
INSTANCE_ID=$(ec2-metadata --instance-id | cut -d: -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
AWS_REGION=$(ec2-metadata --availability-zone | cut -d: -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed 's/[a-z]$//')

# Get all volumes attached to this instance
VOLUMES=$(aws ec2 describe-instances \
  --instance-ids $INSTANCE_ID \
  --query "Reservations[0].Instances[0].BlockDeviceMappings[*].Ebs.VolumeId" \
  --output text \
  --region $AWS_REGION 2>/dev/null || echo "")

if [ -n "$VOLUMES" ]; then
  for VOLUME in $VOLUMES; do
    echo "Tagging volume: $VOLUME"
    aws ec2 create-tags \
      --resources $VOLUME \
      --tags \
        Key=Name,Value="robosystems-gha-runner-${Environment}-volume" \
        Key=Environment,Value="${Environment}" \
        Key=Service,Value="RoboSystems" \
        Key=Component,Value="GHARunner" \
        Key=VolumeType,Value="RootVolume" \
        Key=InstanceId,Value="$INSTANCE_ID" \
        Key=CreatedBy,Value="CloudFormation" \
      --region $AWS_REGION 2>/dev/null || echo "Warning: Failed to tag volume $VOLUME"
  done
  echo "Volume tagging completed"
else
  echo "Warning: No volumes found to tag"
fi

echo "=== GHA Runner Setup Completed at $(date) ==="
