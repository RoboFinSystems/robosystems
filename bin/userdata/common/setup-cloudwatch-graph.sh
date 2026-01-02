#!/bin/bash
# Universal CloudWatch Agent Setup for Graph Databases
# Supports both LadybugDB and Neo4j with standardized monitoring
#
# Note: Environment is included in the namespace (e.g., RoboSystems/Graph/prod)
# rather than as a dimension, since CloudWatch Agent's append_dimensions
# doesn't reliably propagate custom dimensions to the OTEL-based config.

set -e

# Validate required environment variables
: ${DATABASE_TYPE:?"DATABASE_TYPE must be set (ladybug|neo4j)"}
: ${CLOUDWATCH_NAMESPACE:?"CLOUDWATCH_NAMESPACE must be set"}
: ${DATA_DIR:?"DATA_DIR must be set"}

# Extract environment from namespace for log group (e.g., RoboSystems/Graph/prod -> prod)
ENVIRONMENT="${CLOUDWATCH_NAMESPACE##*/}"

# Use unified log group from CloudFormation
UNIFIED_LOG_GROUP="/robosystems/${ENVIRONMENT}/graph-api"

echo "=== Configuring CloudWatch Agent for ${DATABASE_TYPE} ==="
echo "Namespace: ${CLOUDWATCH_NAMESPACE}"
echo "Log group: ${UNIFIED_LOG_GROUP}"
echo "Data directory: ${DATA_DIR}"

# Verify CloudWatch agent installation
if [ ! -d "/opt/aws/amazon-cloudwatch-agent" ]; then
  echo "Installing CloudWatch agent..."
  yum update -y
  yum install -y amazon-cloudwatch-agent
fi

# Create CloudWatch agent configuration
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
          "${DATA_DIR}"
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
      "InstanceType": "\${aws:InstanceType}"
    }
  },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "${DATA_DIR}/logs/*.log",
            "log_group_name": "${UNIFIED_LOG_GROUP}",
            "log_stream_name": "{instance_id}/application",
            "retention_in_days": 30
          },
          {
            "file_path": "/var/log/${DATABASE_TYPE}-writer-setup.log",
            "log_group_name": "${UNIFIED_LOG_GROUP}",
            "log_stream_name": "{instance_id}/setup",
            "retention_in_days": 30
          }
        ]
      }
    }
  }
}
EOF

# Set proper permissions for config file
chmod 0644 /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
chown root:root /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

# Start CloudWatch agent
echo "Starting CloudWatch Agent..."
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json || {
    echo "WARNING: CloudWatch Agent failed to start, but continuing with setup"
    echo "Error was: $?"
}

# Enable CloudWatch agent to start on boot
systemctl enable amazon-cloudwatch-agent || true

echo "✅ CloudWatch Agent configured successfully"
exit 0
