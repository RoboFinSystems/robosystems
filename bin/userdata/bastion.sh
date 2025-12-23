#!/bin/bash
# Redirect output to a log file and console for debugging
exec > >(tee /var/log/user-data.log)
exec 2>&1

# Log environment for debugging
echo "=== Bastion initialization started at $(date) ==="
echo "Environment: ${Environment}"
echo "Stack Name: ${AWS_STACK_NAME}"
echo "Region: ${AWS_REGION}"
echo "Account ID: ${AWS_ACCOUNT_ID}"

# Update and install packages
dnf update -y
dnf install -y amazon-ssm-agent amazon-cloudwatch-agent htop tmux postgresql15 docker git jq

# Ensure SSM agent is running
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent

# Set the CloudWatch namespace based on the environment
if [ "${Environment}" = "prod" ]; then
  CW_NAMESPACE="BastionHostProd"
elif [ "${Environment}" = "staging" ]; then
  CW_NAMESPACE="BastionHostStaging"
elif [ "${Environment}" = "dev" ]; then
  CW_NAMESPACE="BastionHostDev"
else
  CW_NAMESPACE="BastionHost"
fi

# Get instance ID for log stream naming
INSTANCE_ID=$(ec2-metadata --instance-id 2>/dev/null | cut -d: -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' || echo "unknown")

# Configure CloudWatch agent
cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << EOF
{
  "agent": {
    "metrics_collection_interval": 60,
    "run_as_user": "cwagent"
  },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/user-data.log",
            "log_group_name": "/robosystems/${Environment}/bastion-host",
            "log_stream_name": "${INSTANCE_ID}/user-data",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/secure",
            "log_group_name": "/robosystems/${Environment}/bastion-host",
            "log_stream_name": "${INSTANCE_ID}/secure",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/messages",
            "log_group_name": "/robosystems/${Environment}/bastion-host",
            "log_stream_name": "${INSTANCE_ID}/messages",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/audit/audit.log",
            "log_group_name": "/robosystems/${Environment}/bastion-host",
            "log_stream_name": "${INSTANCE_ID}/audit",
            "timezone": "UTC"
          }
        ]
      }
    }
  },
  "metrics": {
    "namespace": "${CW_NAMESPACE}",
    "metrics_collected": {
      "disk": {
        "measurement": ["used_percent"],
        "metrics_collection_interval": 60,
        "resources": ["/"],
        "drop_device": true
      },
      "mem": {
        "measurement": [
          "used_percent",
          "mem_used",
          "mem_available"
        ],
        "metrics_collection_interval": 60
      },
      "swap": {
        "measurement": ["used_percent"],
        "metrics_collection_interval": 60
      },
      "cpu": {
        "measurement": [
          "cpu_usage_idle",
          "cpu_usage_iowait",
          "cpu_usage_user",
          "cpu_usage_system"
        ],
        "metrics_collection_interval": 60
      }
    }
  }
}
EOF

# Enable SSH session logging in audit
dnf install -y audit
systemctl enable auditd
systemctl start auditd

# Add SSH session tracking rules
cat >> /etc/audit/rules.d/ssh.rules << 'EOF'
-w /var/log/secure -p wa -k ssh_logs
-w /etc/ssh/ -p wa -k ssh_config
-a always,exit -F arch=b64 -S execve -F euid=0 -k root_commands
EOF

# Restart auditd to apply rules
systemctl restart auditd

# Start CloudWatch agent
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config \
  -m ec2 \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json \
  -s

# Verify CloudWatch agent is running
systemctl status amazon-cloudwatch-agent

# Create SSH config for easier tunneling
mkdir -p /home/ec2-user/.ssh

# Note: Use the tunnels script from your local repository for dynamic endpoint discovery
cat > /home/ec2-user/.ssh/config << 'EOF'
Host localhost
    HostName localhost
    User ec2-user
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
EOF

chown ec2-user:ec2-user /home/ec2-user/.ssh/config
chmod 600 /home/ec2-user/.ssh/config

# Send success signal to CloudFormation
echo "Sending CloudFormation signal..."
/opt/aws/bin/cfn-signal --exit-code $? --stack "${AWS_STACK_NAME}" --resource BastionHostEC2Instance --region "${AWS_REGION}"

# Install Docker and setup migration container
echo "Setting up Docker for database migrations..."

# Check instance type to ensure we have enough resources
INSTANCE_TYPE=$(ec2-metadata --instance-type | cut -d: -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
echo "Instance type: $INSTANCE_TYPE"

# Warn if instance is too small for migrations
case "$INSTANCE_TYPE" in
  t4g.nano)
    echo "WARNING: t4g.nano may be too small for running migrations. Consider using t4g.small or larger."
    ;;
  t4g.micro)
    echo "Note: t4g.micro has limited resources. Migration performance may be slower."
    ;;
esac

systemctl enable docker
systemctl start docker
usermod -a -G docker ec2-user

# Login to ECR
ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin $ECR_URI

# Determine image tag based on environment
if [ "${Environment}" = "prod" ]; then
  IMAGE_TAG="latest"
else
  IMAGE_TAG="${Environment}"
fi

# Pull the RoboSystems image for migrations
echo "Pulling RoboSystems image with tag: $IMAGE_TAG"
docker pull $ECR_URI/robosystems:$IMAGE_TAG

# Create a helper script for running migrations
cat > /usr/local/bin/run-migrations.sh << SCRIPT_EOF
#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_error() { echo -e "\${RED}Error: \$1\${NC}" >&2; }
print_info() { echo -e "\${BLUE}Info: \$1\${NC}"; }
print_success() { echo -e "\${GREEN}Success: \$1\${NC}"; }
print_warning() { echo -e "\${YELLOW}Warning: \$1\${NC}"; }

# Default values
COMMAND="upgrade head"
DRY_RUN=false
ENVIRONMENT="${Environment}"

# Parse arguments
while [[ \$# -gt 0 ]]; do
  case \$1 in
    --command)
      COMMAND="\$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --help)
      echo "Usage: \$0 [options]"
      echo "Options:"
      echo "  --command COMMAND    Alembic command to run (default: 'upgrade head')"
      echo "  --dry-run           Show what would be done without applying changes"
      echo "  --help              Show this help message"
      echo ""
      echo "Examples:"
      echo "  \$0                           # Run all pending migrations"
      echo "  \$0 --command current         # Show current migration version"
      echo "  \$0 --command 'downgrade -1'  # Rollback one migration"
      echo "  \$0 --dry-run                 # Preview pending migrations"
      exit 0
      ;;
    *)
      print_error "Unknown option: \$1"
      exit 1
      ;;
  esac
done

print_info "Starting database migration process..."
print_info "Environment: \$ENVIRONMENT"
print_info "Command: alembic \$COMMAND"

# Log migration start to CloudWatch
logger -t database-migration "Starting migration: Environment=\$ENVIRONMENT, Command=\$COMMAND"

# Refresh Docker image - ALWAYS pull latest version before migrations
print_info "Pulling latest Docker image for environment: \$ENVIRONMENT..."
ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# Login to ECR
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin \$ECR_URI 2>/dev/null || {
    print_error "Failed to login to ECR. Check IAM permissions."
    exit 1
}

# Determine correct image tag based on environment
if [ "\$ENVIRONMENT" = "prod" ]; then
  IMAGE_TAG="latest"
else
  IMAGE_TAG="\$ENVIRONMENT"
fi

print_info "Using image tag: \$IMAGE_TAG"

# Force pull to ensure we have the latest version
docker pull \$ECR_URI/robosystems:\$IMAGE_TAG || {
    print_error "Failed to pull Docker image: \$ECR_URI/robosystems:\$IMAGE_TAG"
    exit 1
}

print_success "Successfully pulled latest image"

# Get database connection details
print_info "Retrieving database connection details..."

# Get RDS endpoint from CloudFormation
if [ "\$ENVIRONMENT" = "prod" ]; then
  RDS_ENDPOINT=\$(aws cloudformation describe-stacks \
    --stack-name RoboSystemsPostgresProd \
    --query "Stacks[0].Outputs[?OutputKey=='DatabaseEndpoint'].OutputValue" \
    --output text 2>/dev/null)
  SECRET_ID="robosystems/prod/postgres"
else
  RDS_ENDPOINT=\$(aws cloudformation describe-stacks \
    --stack-name RoboSystemsPostgresStaging \
    --query "Stacks[0].Outputs[?OutputKey=='DatabaseEndpoint'].OutputValue" \
    --output text 2>/dev/null)
  SECRET_ID="robosystems/staging/postgres"
fi

# Validate RDS endpoint was retrieved
if [[ -z "\$RDS_ENDPOINT" || "\$RDS_ENDPOINT" == "None" ]]; then
  print_error "Failed to retrieve RDS endpoint from CloudFormation"
  print_error "Stack: RoboSystemsPostgres\${ENVIRONMENT^}"
  exit 1
fi

# Get database credentials from Secrets Manager
DB_SECRET=\$(aws secretsmanager get-secret-value \
  --secret-id "\$SECRET_ID" \
  --query SecretString \
  --output text)

DB_PASSWORD=\$(echo "\$DB_SECRET" | jq -r '.POSTGRES_PASSWORD // .password')
DB_NAME=\$(echo "\$DB_SECRET" | jq -r '.POSTGRES_DB // .database // "robosystems"')
DB_USER=\$(echo "\$DB_SECRET" | jq -r '.POSTGRES_USER // .username // "postgres"')

# Run migrations in container with timeout
print_info "Running migrations in Docker container..."

# Set a 30-minute timeout for migrations
export MIGRATION_TIMEOUT=1800

if [ "\$DRY_RUN" = true ]; then
  print_warning "DRY RUN MODE - No changes will be applied"

  # Show current status
  docker run --rm --entrypoint /usr/bin/env \
    -e DATABASE_URL="postgresql://\${DB_USER}:\${DB_PASSWORD}@\${RDS_ENDPOINT}:5432/\${DB_NAME}?sslmode=require" \
    -e ENVIRONMENT="\$ENVIRONMENT" \
    \$ECR_URI/robosystems:\$IMAGE_TAG \
    uv run alembic current

  echo ""

  # Show migration plan
  if [[ "\$COMMAND" == "upgrade"* ]]; then
    print_info "Pending migrations:"
    docker run --rm --entrypoint /usr/bin/env \
      -e DATABASE_URL="postgresql://\${DB_USER}:\${DB_PASSWORD}@\${RDS_ENDPOINT}:5432/\${DB_NAME}?sslmode=require" \
      -e ENVIRONMENT="\$ENVIRONMENT" \
      \$ECR_URI/robosystems:\$IMAGE_TAG \
      uv run alembic history | head -20
  fi

  print_success "DRY RUN completed - no changes were made"
else
  # Run the actual migration with timeout
  timeout \$MIGRATION_TIMEOUT docker run --rm --entrypoint /usr/bin/env \
    -e DATABASE_URL="postgresql://\${DB_USER}:\${DB_PASSWORD}@\${RDS_ENDPOINT}:5432/\${DB_NAME}?sslmode=require" \
    -e ENVIRONMENT="\$ENVIRONMENT" \
    \$ECR_URI/robosystems:\$IMAGE_TAG \
    uv run alembic \$COMMAND || {
    print_error "Migration timed out after 30 minutes or failed"
    exit 1
  }

  # Show new status
  echo ""
  print_info "New migration status:"
  docker run --rm --entrypoint /usr/bin/env \
    -e DATABASE_URL="postgresql://\${DB_USER}:\${DB_PASSWORD}@\${RDS_ENDPOINT}:5432/\${DB_NAME}?sslmode=require" \
    -e ENVIRONMENT="\$ENVIRONMENT" \
    \$ECR_URI/robosystems:\$IMAGE_TAG \
    uv run alembic current

  print_success "Migration completed successfully"
fi

# Log migration completion
logger -t database-migration "Migration completed: Environment=\$ENVIRONMENT, Status=SUCCESS"
SCRIPT_EOF

chmod +x /usr/local/bin/run-migrations.sh
chown root:root /usr/local/bin/run-migrations.sh

# Note: Infrastructure operations will use the Docker image from ECR
# This ensures consistency with deployed code and eliminates git clone authentication issues
echo "Infrastructure operations configured to use Docker image from ECR"

# Create directory for RoboSystems configuration
mkdir -p /etc/robosystems

# Store environment configuration
cat > /etc/robosystems/.env << ENV_EOF
ENVIRONMENT=${Environment}
AWS_REGION=${AWS_REGION}
AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID}

# Database configuration (populated at runtime)
# DATABASE_URL will be set by infrastructure scripts when needed
# VALKEY_URL will be set by infrastructure scripts when needed
ENV_EOF

chmod 600 /etc/robosystems/.env
chown ec2-user:ec2-user /etc/robosystems/.env

# Create log directory for operations
mkdir -p /var/log/robosystems
chown ec2-user:ec2-user /var/log/robosystems

# Add CloudWatch log monitoring for infrastructure operations using jq to properly merge JSON
jq --arg env "$Environment" --arg instance "$INSTANCE_ID" \
  '.logs.logs_collected.files.collect_list += [{
    "file_path": "/var/log/robosystems/bastion-operations.log",
    "log_group_name": ("/robosystems/" + $env + "/bastion-host"),
    "log_stream_name": ($instance + "/bastion-operations"),
    "timezone": "UTC"
  }]' /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json > /tmp/cw-config.json

mv /tmp/cw-config.json /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

# Restart CloudWatch agent to pick up new log configuration
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config \
  -m ec2 \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json \
  -s

# Script completed successfully
echo "Bastion host initialization completed successfully"
