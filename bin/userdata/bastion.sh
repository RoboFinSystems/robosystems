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
dnf install -y amazon-ssm-agent amazon-cloudwatch-agent htop tmux postgresql15 docker git

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

# Add additional SSH public keys if provided
if [ -n "${AdditionalSSHKeys}" ]; then
  echo "Adding additional SSH public keys..."
  cat >> /home/ec2-user/.ssh/authorized_keys << EOF
${AdditionalSSHKeys}
EOF

  # Ensure proper ownership and permissions
  chown ec2-user:ec2-user /home/ec2-user/.ssh/authorized_keys
  chmod 600 /home/ec2-user/.ssh/authorized_keys
  echo "Additional SSH keys added successfully"
fi

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
    --stack-name RoboSystemsPostgresIAMProd \
    --query "Stacks[0].Outputs[?OutputKey=='DatabaseEndpoint'].OutputValue" \
    --output text)
  SECRET_ID="robosystems/prod/postgres"
else
  RDS_ENDPOINT=\$(aws cloudformation describe-stacks \
    --stack-name RoboSystemsPostgresIAMStaging \
    --query "Stacks[0].Outputs[?OutputKey=='DatabaseEndpoint'].OutputValue" \
    --output text)
  SECRET_ID="robosystems/staging/postgres"
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

# Note: Admin operations will use the Docker image from ECR
# This ensures consistency with deployed code and eliminates git clone authentication issues
echo "Admin operations configured to use Docker image from ECR"

# Create directory for RoboSystems configuration
mkdir -p /etc/robosystems

# Store environment configuration
cat > /etc/robosystems/.env << ENV_EOF
ENVIRONMENT=${Environment}
AWS_REGION=${AWS_REGION}
AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID}

# Database configuration (populated at runtime)
# DATABASE_URL will be set by admin scripts when needed
# CELERY_BROKER_URL will be set by admin scripts when needed
ENV_EOF

chmod 600 /etc/robosystems/.env
chown ec2-user:ec2-user /etc/robosystems/.env

# Create the admin operations script
cat > /usr/local/bin/run-admin-operation.sh << 'ADMIN_SCRIPT_EOF'
#!/bin/bash
# Admin Operations Script for Bastion Host
# Runs admin operations using Docker image from ECR (same pattern as migrations)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_error() { echo -e "${RED}Error: $1${NC}" >&2; }
print_info() { echo -e "${BLUE}Info: $1${NC}"; }
print_success() { echo -e "${GREEN}Success: $1${NC}"; }
print_warning() { echo -e "${YELLOW}Warning: $1${NC}"; }

# Configuration
ENV_FILE="/etc/robosystems/.env"
LOG_DIR="/var/log/robosystems"
AUDIT_LOG="$LOG_DIR/admin-operations.log"

# Function to log operations
log_operation() {
    local operation="$1"
    local parameters="$2"
    local result="$3"
    local timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    echo "[$timestamp] Operation: $operation | Parameters: $parameters | User: $USER | Result: $result" >> "$AUDIT_LOG"
}

# Function to load environment and get connection details
load_environment() {
    if [ ! -f "$ENV_FILE" ]; then
        print_error "Environment file not found: $ENV_FILE"
        exit 1
    fi

    # Export environment variables
    set -a
    source "$ENV_FILE"
    set +a

    print_info "Loading environment configuration..."

    # Get database connection details dynamically
    if [ "$ENVIRONMENT" = "prod" ]; then
        RDS_ENDPOINT=$(aws cloudformation describe-stacks \
            --stack-name RoboSystemsPostgresIAMProd \
            --query "Stacks[0].Outputs[?OutputKey=='DatabaseEndpoint'].OutputValue" \
            --output text 2>/dev/null)
        SECRET_ID="robosystems/prod/postgres"
        VALKEY_ENDPOINT=$(aws cloudformation describe-stacks \
            --stack-name RoboSystemsValkey \
            --query "Stacks[0].Outputs[?OutputKey=='ValkeyPrimaryEndpoint'].OutputValue" \
            --output text 2>/dev/null)
    else
        RDS_ENDPOINT=$(aws cloudformation describe-stacks \
            --stack-name RoboSystemsPostgresIAMStaging \
            --query "Stacks[0].Outputs[?OutputKey=='DatabaseEndpoint'].OutputValue" \
            --output text 2>/dev/null)
        SECRET_ID="robosystems/staging/postgres"
        VALKEY_ENDPOINT=$(aws cloudformation describe-stacks \
            --stack-name RoboSystemsValkeyStaging \
            --query "Stacks[0].Outputs[?OutputKey=='ValkeyPrimaryEndpoint'].OutputValue" \
            --output text 2>/dev/null)
    fi

    # Get database credentials from Secrets Manager
    DB_SECRET=$(aws secretsmanager get-secret-value \
        --secret-id "$SECRET_ID" \
        --query SecretString \
        --output text 2>/dev/null)

    if [ -n "$DB_SECRET" ]; then
        DB_PASSWORD=$(echo "$DB_SECRET" | jq -r '.POSTGRES_PASSWORD // .password')
        DB_NAME=$(echo "$DB_SECRET" | jq -r '.POSTGRES_DB // .database // "robosystems"')
        DB_USER=$(echo "$DB_SECRET" | jq -r '.POSTGRES_USER // .username // "postgres"')

        export DATABASE_URL="postgresql://${DB_USER}:${DB_PASSWORD}@${RDS_ENDPOINT}:5432/${DB_NAME}?sslmode=require"
    fi

    if [ -n "$VALKEY_ENDPOINT" ]; then
        export CELERY_BROKER_URL="redis://${VALKEY_ENDPOINT}:6379/0"
        export CELERY_RESULT_BACKEND="redis://${VALKEY_ENDPOINT}:6379/1"
    fi

    print_success "Environment loaded successfully"
}

# Function to refresh Docker image
refresh_docker_image() {
    print_info "Pulling latest Docker image for environment: $ENVIRONMENT..."

    ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

    # Login to ECR
    aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin $ECR_URI 2>/dev/null || {
        print_error "Failed to login to ECR. Check IAM permissions."
        exit 1
    }

    # Determine correct image tag based on environment
    if [ "$ENVIRONMENT" = "prod" ]; then
        IMAGE_TAG="latest"
    else
        IMAGE_TAG="$ENVIRONMENT"
    fi

    print_info "Using image tag: $IMAGE_TAG"

    # Force pull to ensure we have the latest version
    docker pull $ECR_URI/robosystems:$IMAGE_TAG || {
        print_error "Failed to pull Docker image: $ECR_URI/robosystems:$IMAGE_TAG"
        exit 1
    }

    export DOCKER_IMAGE="$ECR_URI/robosystems:$IMAGE_TAG"
    print_success "Successfully pulled latest image"
}

# Function to run command in Docker container
run_in_docker() {
    local command="$1"

    docker run --rm --entrypoint /usr/bin/env \
        -e DATABASE_URL="$DATABASE_URL" \
        -e CELERY_BROKER_URL="$CELERY_BROKER_URL" \
        -e CELERY_RESULT_BACKEND="$CELERY_RESULT_BACKEND" \
        -e ENVIRONMENT="$ENVIRONMENT" \
        -e AWS_REGION="$AWS_REGION" \
        -e AWS_ACCOUNT_ID="$AWS_ACCOUNT_ID" \
        "$DOCKER_IMAGE" \
        bash -c "$command"
}

# Main execution
main() {
    local operation="$1"
    shift
    local parameters="$@"

    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}🛠️  RoboSystems Admin Operations${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    # Load environment
    load_environment

    # Refresh Docker image
    refresh_docker_image

    # Log the operation start
    log_operation "$operation" "$parameters" "STARTED"

    print_info "Operation: $operation"
    print_info "Parameters: $parameters"
    echo ""

    # Execute the operation
    case "$operation" in
        # Credit operations
        credit-allocate-user)
            run_in_docker "uv run python -m robosystems.scripts.credit_admin allocate-user $parameters"
            ;;

        credit-allocate-graph)
            run_in_docker "uv run python -m robosystems.scripts.credit_admin allocate-graph $parameters"
            ;;

        credit-allocate-all)
            run_in_docker "uv run python -m robosystems.scripts.credit_admin allocate-all $parameters"
            ;;

        credit-bonus)
            run_in_docker "uv run python -m robosystems.scripts.credit_admin bonus $parameters"
            ;;

        credit-health)
            run_in_docker "uv run python -m robosystems.scripts.credit_admin health"
            ;;

        # Repository access operations
        repo-grant)
            run_in_docker "uv run python robosystems/scripts/repository_access_manager.py grant $parameters"
            ;;

        repo-revoke)
            run_in_docker "uv run python robosystems/scripts/repository_access_manager.py revoke $parameters"
            ;;

        repo-list)
            run_in_docker "uv run python robosystems/scripts/repository_access_manager.py list $parameters"
            ;;

        repo-check)
            run_in_docker "uv run python robosystems/scripts/repository_access_manager.py check $parameters"
            ;;

        # DLQ operations
        dlq-stats)
            run_in_docker "uv run python -m robosystems.scripts.manage_dlq stats"
            ;;

        dlq-health)
            run_in_docker "uv run python -m robosystems.scripts.manage_dlq health"
            ;;

        dlq-list)
            run_in_docker "uv run python -m robosystems.scripts.manage_dlq list $parameters"
            ;;

        dlq-reprocess)
            run_in_docker "uv run python -m robosystems.scripts.manage_dlq reprocess $parameters"
            ;;

        dlq-purge)
            print_warning "WARNING: This will purge all DLQ messages!"
            read -p "Type 'CONFIRM' to proceed: " confirmation
            if [ "$confirmation" == "CONFIRM" ]; then
                run_in_docker "uv run python -m robosystems.scripts.manage_dlq purge --confirm"
            else
                print_error "Operation cancelled"
                log_operation "$operation" "$parameters" "CANCELLED"
                exit 1
            fi
            ;;

        # SEC pipeline operations
        sec-reset)
            print_warning "WARNING: This will reset the SEC database!"
            read -p "Type 'CONFIRM' to proceed: " confirmation
            if [ "$confirmation" == "CONFIRM" ]; then
                run_in_docker "uv run python robosystems/scripts/reset_sec_pipeline.py $parameters"
            else
                print_error "Operation cancelled"
                log_operation "$operation" "$parameters" "CANCELLED"
                exit 1
            fi
            ;;

        sec-load)
            run_in_docker "uv run python robosystems/scripts/sec_pipeline.py full $parameters"
            ;;

        sec-collect)
            run_in_docker "uv run python robosystems/scripts/sec_pipeline.py collect $parameters"
            ;;

        sec-process)
            run_in_docker "uv run python robosystems/scripts/sec_pipeline.py process $parameters"
            ;;

        sec-ingest)
            run_in_docker "uv run python robosystems/scripts/sec_pipeline.py ingest $parameters"
            ;;

        # Help
        help|--help|-h|"")
            echo "Available operations:"
            echo ""
            echo "Credit Management:"
            echo "  credit-allocate-user USER_ID [--dry-run]"
            echo "  credit-allocate-graph GRAPH_ID [--dry-run]"
            echo "  credit-allocate-all [--dry-run]"
            echo "  credit-bonus GRAPH_ID --amount AMOUNT --description TEXT [--dry-run]"
            echo "  credit-health"
            echo ""
            echo "Repository Access:"
            echo "  repo-grant USER_ID REPOSITORY ACCESS_LEVEL [--expires-days N]"
            echo "  repo-revoke USER_ID REPOSITORY"
            echo "  repo-list [--repository REPO]"
            echo "  repo-check USER_ID [--repository REPO]"
            echo ""
            echo "Dead Letter Queue:"
            echo "  dlq-stats"
            echo "  dlq-health"
            echo "  dlq-list [--limit N]"
            echo "  dlq-reprocess TASK_ID"
            echo "  dlq-purge"
            echo ""
            echo "SEC Pipeline:"
            echo "  sec-reset [--soft]"
            echo "  sec-load --year YEAR --companies N --filings N [--refresh] [--parallel]"
            echo "  sec-collect --year YEAR --companies N --filings N"
            echo "  sec-process --year YEAR [--refresh]"
            echo "  sec-ingest --year YEAR"
            echo ""
            echo "Examples:"
            echo "  admin credit-allocate-user user_123"
            echo "  admin repo-grant user_123 sec admin"
            echo "  admin sec-load --year 2024 --companies 10 --filings 5"
            ;;

        *)
            print_error "Unknown operation: $operation"
            echo "Use 'help' to see available operations"
            log_operation "$operation" "$parameters" "INVALID"
            exit 1
            ;;
    esac

    # Check exit code
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo ""
        print_success "Operation completed successfully"
        log_operation "$operation" "$parameters" "SUCCESS"
    else
        echo ""
        print_error "Operation failed with exit code: $EXIT_CODE"
        log_operation "$operation" "$parameters" "FAILED:$EXIT_CODE"
        exit $EXIT_CODE
    fi
}

# Run main function
main "$@"
ADMIN_SCRIPT_EOF

chmod +x /usr/local/bin/run-admin-operation.sh
chown root:root /usr/local/bin/run-admin-operation.sh
echo "Admin operations script installed successfully"

# Create convenient aliases for ec2-user
echo 'alias admin="sudo /usr/local/bin/run-admin-operation.sh"' >> /home/ec2-user/.bashrc
echo 'alias radmin="cd /home/ec2-user/robosystems-service && git pull && sudo /usr/local/bin/run-admin-operation.sh"' >> /home/ec2-user/.bashrc

# Create log directory for admin operations
mkdir -p /var/log/robosystems
chown ec2-user:ec2-user /var/log/robosystems

# Add CloudWatch log monitoring for admin operations
cat >> /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << 'EOF_LOGS'
          ,{
            "file_path": "/var/log/robosystems/admin-operations.log",
            "log_group_name": "/robosystems/${Environment}/bastion-host",
            "log_stream_name": "${INSTANCE_ID}/admin-operations",
            "timezone": "UTC"
          }
EOF_LOGS

# Restart CloudWatch agent to pick up new log configuration
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config \
  -m ec2 \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json \
  -s

# Script completed successfully
echo "Bastion host initialization completed successfully"