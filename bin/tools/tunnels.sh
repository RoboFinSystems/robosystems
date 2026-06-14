#!/bin/bash

# RoboSystems AWS SSM Tunnels
# Usage: ./bin/tools/tunnels.sh [environment] [service]
# Environments: prod, staging, dev
# Services: postgres, valkey, dagster, api, api-internal, all

set -euo pipefail

# Default configuration
DEFAULT_ENVIRONMENT="prod"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Dynamic configuration (populated by discover_infrastructure)
BASTION_INSTANCE_ID=""
POSTGRES_ENDPOINT=""
VALKEY_ENDPOINT=""
DAGSTER_ENDPOINT=""
API_ENDPOINT=""
API_INTERNAL_ENDPOINT=""  # Service Discovery endpoint (bypasses ALB)
API_ACCESS_MODE=""        # internal or public

# Dagster webserver scale-on-demand tracking. The webserver defaults to 0 tasks
# in CloudFormation (it's UI-only, reachable solely through this tunnel) so an
# idle UI doesn't cost an on-demand Fargate task. We scale it to 1 while a
# dagster tunnel is open, then back to 0 on exit.
ENVIRONMENT=""
WEBSERVER_SCALED_UP="false"
WEBSERVER_WAIT_PID=""  # background `ecs wait services-stable` PID (overlaps with bastion boot)


# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Cleanup function
cleanup_on_exit() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"

    # Kill any background SSM sessions
    pkill -f "session-manager-plugin" 2>/dev/null || true

    # Stop the background readiness wait if it's still running (e.g. aborted mid-boot)
    if [[ -n "${WEBSERVER_WAIT_PID:-}" ]]; then
        kill "$WEBSERVER_WAIT_PID" 2>/dev/null || true
    fi

    # Scale the Dagster webserver back down if this session brought it up
    if [[ "$WEBSERVER_SCALED_UP" == "true" && -n "${ENVIRONMENT:-}" ]]; then
        scale_webserver_down "$ENVIRONMENT"
    fi

    # Prompt user to optionally stop the bastion
    if [[ -n "${BASTION_INSTANCE_ID:-}" ]] && [[ -t 0 ]]; then
        echo ""
        read -t 10 -p "Stop bastion instance? (y/N): " -n 1 -r stop_choice || true
        echo ""

        if [[ "${stop_choice:-}" =~ ^[Yy]$ ]]; then
            echo -e "${YELLOW}Stopping bastion instance...${NC}"
            aws ec2 stop-instances --instance-ids "$BASTION_INSTANCE_ID" --region "${AWS_REGION:-us-east-1}" >/dev/null 2>&1 || true
            echo -e "${GREEN}✓ Bastion stopped${NC}"
        fi
    fi

    echo -e "${GREEN}Done.${NC}"
}

# Set up trap to cleanup on exit (only EXIT - INT/TERM will trigger EXIT anyway)
trap cleanup_on_exit EXIT

# Validate dependencies
check_dependencies() {
    local missing_deps=()

    # Check for required commands
    for cmd in aws jq; do
        if ! command -v "$cmd" &> /dev/null; then
            missing_deps+=("$cmd")
        fi
    done

    if [ ${#missing_deps[@]} -ne 0 ]; then
        echo -e "${RED}Error: Missing required dependencies: ${missing_deps[*]}${NC}"
        echo "Please install missing dependencies and try again."
        exit 1
    fi

    # Check AWS CLI version (SSM requires v2)
    local aws_version=$(aws --version 2>&1 | cut -d' ' -f1 | cut -d'/' -f2)
    local major_version=$(echo "$aws_version" | cut -d'.' -f1)

    if [ "$major_version" -lt 2 ]; then
        echo -e "${RED}Error: AWS CLI v2 is required for SSM port forwarding (found v${aws_version})${NC}"
        echo "Please upgrade to AWS CLI v2: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        exit 1
    fi

    # Check for SSM plugin
    if ! command -v session-manager-plugin &> /dev/null; then
        echo -e "${RED}Error: AWS Session Manager Plugin not found${NC}"
        echo "Install it from: https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html"
        exit 1
    fi

    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        echo -e "${RED}Error: AWS credentials not configured or expired${NC}"
        echo "Please run 'aws configure' or set AWS credentials"
        exit 1
    fi
}

print_usage() {
    echo -e "${BLUE}Usage: $0 [environment] [service]${NC}"
    echo ""
    echo -e "${YELLOW}Available environments:${NC}"
    echo "  prod     - Production environment (default)"
    echo "  staging  - Staging environment"
    echo "  dev      - Development environment"
    echo ""
    echo -e "${GREEN}======================================================================"
    echo "SSM Tunnels - Access internal services via AWS Systems Manager"
    echo "======================================================================${NC}"
    echo "  postgres      - PostgreSQL tunnel (localhost:5432)"
    echo "  valkey        - Valkey ElastiCache tunnel (localhost:6379)"
    echo "  dagster       - Dagster webserver tunnel (localhost:8002)"
    echo "  api           - API ALB tunnel (localhost:8000)"
    echo "  api-internal  - API direct tunnel (localhost:8000, bypasses ALB)"
    echo "  all           - All service tunnels (runs in background)"
    echo ""
    echo -e "${GREEN}======================================================================"
    echo "Database Operations"
    echo "======================================================================${NC}"
    echo "  migrate       - Run database migrations via bastion"
    echo "  shell         - Open interactive shell on bastion"
    echo ""
    echo -e "${YELLOW}Note: No SSH keys required - uses AWS IAM authentication${NC}"
    echo ""
}

discover_infrastructure() {
    local environment=$1
    local service=${2:-all}

    # Validate environment
    if [[ ! "$environment" =~ ^(prod|staging|dev)$ ]]; then
        echo -e "${RED}Error: Invalid environment '$environment'. Must be prod, staging, or dev.${NC}"
        return 1
    fi

    echo -e "${BLUE}Discovering infrastructure for $environment environment...${NC}"

    # Discover bastion instance ID
    local env_capitalized="$(echo ${environment:0:1} | tr 'a-z' 'A-Z')${environment:1}"

    echo -e "${YELLOW}Looking for bastion instance...${NC}"

    BASTION_INSTANCE_ID=$(aws ec2 describe-instances \
        --filters "Name=tag:aws:cloudformation:stack-name,Values=RoboSystemsBastion${env_capitalized}" \
                  "Name=instance-state-name,Values=running,stopped,pending" \
        --query 'Reservations[0].Instances[0].InstanceId' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null || echo "")

    if [[ -z "$BASTION_INSTANCE_ID" || "$BASTION_INSTANCE_ID" == "None" ]]; then
        echo -e "${RED}Error: Could not find bastion instance for $environment${NC}"
        echo "Make sure the bastion stack 'RoboSystemsBastion${env_capitalized}' is deployed."
        exit 1
    fi

    # Discover PostgreSQL endpoint
    local postgres_stack=""
    if [[ "$environment" == "prod" ]]; then
        postgres_stack="RoboSystemsPostgresProd"
    elif [[ "$environment" == "staging" ]]; then
        postgres_stack="RoboSystemsPostgresStaging"
    else
        echo -e "${YELLOW}Skipping postgres discovery for dev environment${NC}"
        POSTGRES_ENDPOINT="NOT_FOUND"
    fi

    if [[ -n "$postgres_stack" ]]; then
        echo -e "${YELLOW}Looking for postgres stack: $postgres_stack${NC}"

        POSTGRES_ENDPOINT=$(aws cloudformation describe-stacks \
            --stack-name "$postgres_stack" \
            --query 'Stacks[0].Outputs[?OutputKey==`RDSInstanceEndpoint`].OutputValue' \
            --output text \
            --region "$AWS_REGION" 2>/dev/null || echo "")

        if [[ -z "$POSTGRES_ENDPOINT" ]]; then
            echo -e "${YELLOW}Warning: Could not find PostgreSQL endpoint for $environment${NC}"
            POSTGRES_ENDPOINT="NOT_FOUND"
        fi
    fi

    # Discover Valkey ElastiCache endpoint
    echo -e "${YELLOW}Looking for Valkey ElastiCache cluster...${NC}"

    local valkey_stack="RoboSystemsValkey${env_capitalized}"
    VALKEY_ENDPOINT=$(aws cloudformation describe-stacks \
        --stack-name "$valkey_stack" \
        --query 'Stacks[0].Outputs[?OutputKey==`ValkeyEndpoint`].OutputValue' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null || echo "")

    if [[ -z "$VALKEY_ENDPOINT" || "$VALKEY_ENDPOINT" == "None" ]]; then
        echo -e "${YELLOW}Warning: Could not find Valkey cluster for $environment${NC}"
        VALKEY_ENDPOINT="NOT_FOUND"
    fi

    # Discover Dagster internal endpoint (Service Discovery)
    echo -e "${YELLOW}Looking for Dagster webserver endpoint...${NC}"

    local dagster_stack="RoboSystemsDagster${env_capitalized}"
    DAGSTER_ENDPOINT=$(aws cloudformation describe-stacks \
        --stack-name "$dagster_stack" \
        --query 'Stacks[0].Outputs[?OutputKey==`DagsterInternalEndpoint`].OutputValue' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null || echo "")

    if [[ -z "$DAGSTER_ENDPOINT" || "$DAGSTER_ENDPOINT" == "None" ]]; then
        # Fallback to standard Service Discovery naming
        DAGSTER_ENDPOINT="webserver.dagster.${environment}.robosystems.local"
        echo -e "${YELLOW}Using default Dagster endpoint: $DAGSTER_ENDPOINT${NC}"
    fi

    # Discover API ALB endpoint
    echo -e "${YELLOW}Looking for API ALB endpoint...${NC}"

    local api_stack="RoboSystemsAPI${env_capitalized}"
    API_ENDPOINT=$(aws cloudformation describe-stacks \
        --stack-name "$api_stack" \
        --query 'Stacks[0].Outputs[?OutputKey==`LoadBalancerDNS`].OutputValue' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null || echo "")

    if [[ -z "$API_ENDPOINT" || "$API_ENDPOINT" == "None" ]]; then
        echo -e "${YELLOW}Warning: Could not find API ALB endpoint for $environment${NC}"
        API_ENDPOINT="NOT_FOUND"
    fi

    # Discover API internal endpoint (Service Discovery - bypasses ALB)
    echo -e "${YELLOW}Looking for API internal endpoint (Service Discovery)...${NC}"

    API_INTERNAL_ENDPOINT=$(aws cloudformation describe-stacks \
        --stack-name "$api_stack" \
        --query 'Stacks[0].Outputs[?OutputKey==`ApiServiceDiscoveryEndpoint`].OutputValue' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null || echo "")

    if [[ -z "$API_INTERNAL_ENDPOINT" || "$API_INTERNAL_ENDPOINT" == "None" ]]; then
        # Fallback to standard Service Discovery naming
        API_INTERNAL_ENDPOINT="api.${environment}.robosystems.local"
        echo -e "${YELLOW}Using default API internal endpoint: $API_INTERNAL_ENDPOINT${NC}"
    fi

    # Discover API access mode (internal or public)
    echo -e "${YELLOW}Looking for API access mode...${NC}"

    API_ACCESS_MODE=$(aws cloudformation describe-stacks \
        --stack-name "$api_stack" \
        --query 'Stacks[0].Parameters[?ParameterKey==`ApiAccessMode`].ParameterValue' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null || echo "")

    if [[ -z "$API_ACCESS_MODE" || "$API_ACCESS_MODE" == "None" ]]; then
        API_ACCESS_MODE="unknown"
        echo -e "${YELLOW}Could not determine API access mode${NC}"
    fi

    # Show discovered endpoints
    echo -e "${GREEN}✓ Infrastructure discovered:${NC}"
    echo -e "  Bastion ID:   ${GREEN}$BASTION_INSTANCE_ID${NC}"
    echo -e "  PostgreSQL:   ${GREEN}$POSTGRES_ENDPOINT${NC}"
    echo -e "  Valkey:       ${GREEN}$VALKEY_ENDPOINT${NC}"
    echo -e "  Dagster:      ${GREEN}$DAGSTER_ENDPOINT${NC}"
    echo -e "  API (ALB):    ${GREEN}$API_ENDPOINT${NC}"
    echo -e "  API (Direct): ${GREEN}$API_INTERNAL_ENDPOINT${NC}"
    echo -e "  API Mode:     ${GREEN}$API_ACCESS_MODE${NC}"
    echo ""
}

check_bastion_status() {
    local environment=$1

    echo -e "${BLUE}Checking bastion host status...${NC}"

    # Check instance state
    local instance_state=$(aws ec2 describe-instances \
        --instance-ids "$BASTION_INSTANCE_ID" \
        --query 'Reservations[0].Instances[0].State.Name' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null || echo "")

    echo -e "${YELLOW}Bastion instance state: $instance_state${NC}"

    if [[ "$instance_state" == "stopped" ]]; then
        echo -e "${YELLOW}Starting bastion instance...${NC}"
        aws ec2 start-instances --instance-ids "$BASTION_INSTANCE_ID" --region "$AWS_REGION" >/dev/null

        # Wait for instance to be running
        echo -e "${BLUE}Waiting for instance to reach running state...${NC}"
        aws ec2 wait instance-running --instance-ids "$BASTION_INSTANCE_ID" --region "$AWS_REGION"

        echo -e "${GREEN}✓ Bastion instance is now running${NC}"

        # Wait for SSM agent to initialize
        echo -e "${BLUE}Waiting for SSM agent to initialize (30 seconds)...${NC}"
        sleep 30

    elif [[ "$instance_state" == "running" ]]; then
        echo -e "${GREEN}✓ Bastion instance is already running${NC}"
    elif [[ "$instance_state" == "pending" ]]; then
        echo -e "${BLUE}Waiting for instance to reach running state...${NC}"
        aws ec2 wait instance-running --instance-ids "$BASTION_INSTANCE_ID" --region "$AWS_REGION"
        echo -e "${GREEN}✓ Bastion instance is now running${NC}"
        sleep 15  # Wait for SSM agent
    else
        echo -e "${RED}Error: Bastion instance is in unexpected state: $instance_state${NC}"
        exit 1
    fi

    # Verify SSM connectivity
    echo -e "${BLUE}Verifying SSM connectivity...${NC}"
    local ssm_status=$(aws ssm describe-instance-information \
        --filters "Key=InstanceIds,Values=$BASTION_INSTANCE_ID" \
        --query 'InstanceInformationList[0].PingStatus' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null || echo "")

    if [[ "$ssm_status" != "Online" ]]; then
        echo -e "${YELLOW}SSM agent not yet online, waiting...${NC}"
        for i in {1..12}; do
            sleep 5
            ssm_status=$(aws ssm describe-instance-information \
                --filters "Key=InstanceIds,Values=$BASTION_INSTANCE_ID" \
                --query 'InstanceInformationList[0].PingStatus' \
                --output text \
                --region "$AWS_REGION" 2>/dev/null || echo "")
            if [[ "$ssm_status" == "Online" ]]; then
                break
            fi
            echo -e "${YELLOW}Still waiting for SSM agent... (attempt $i/12)${NC}"
        done
    fi

    if [[ "$ssm_status" == "Online" ]]; then
        echo -e "${GREEN}✓ SSM agent is online${NC}"
    else
        echo -e "${RED}Error: SSM agent not responding. Status: $ssm_status${NC}"
        echo "The instance may not have the SSM agent installed or proper IAM permissions."
        exit 1
    fi
}

# Fire the webserver scale-up (fast: describe + update-service) and kick off the
# readiness wait in the background, so the ~1-2 min cold start overlaps with the
# bastion boot instead of running after it. The describe/update run synchronously
# in the parent shell so WEBSERVER_SCALED_UP propagates to the cleanup trap;
# only the slow `wait services-stable` is backgrounded. Join with wait_webserver_ready.
start_webserver_scale_up() {
    local environment=$1
    local cluster="robosystems-dagster-${environment}-cluster"
    local service="robosystems-dagster-webserver-${environment}"

    echo -e "${BLUE}Ensuring Dagster webserver is running...${NC}"

    local desired
    desired=$(aws ecs describe-services \
        --cluster "$cluster" --services "$service" \
        --query 'services[0].desiredCount' --output text \
        --region "$AWS_REGION" 2>/dev/null || echo "")

    if [[ -z "$desired" || "$desired" == "None" ]]; then
        echo -e "${YELLOW}Warning: Dagster webserver service not found; skipping scale-up${NC}"
        return 0
    fi

    if [[ "$desired" -ge 1 ]]; then
        echo -e "${GREEN}✓ Dagster webserver already running${NC}"
        return 0
    fi

    echo -e "${YELLOW}Starting Dagster webserver (scale 0 -> 1)...${NC}"
    aws ecs update-service \
        --cluster "$cluster" --service "$service" \
        --desired-count 1 --region "$AWS_REGION" >/dev/null
    WEBSERVER_SCALED_UP="true"

    # Background the readiness wait so it runs concurrently with the bastion boot.
    aws ecs wait services-stable \
        --cluster "$cluster" --services "$service" \
        --region "$AWS_REGION" &
    WEBSERVER_WAIT_PID=$!
    echo -e "${BLUE}Dagster webserver starting in background (cold start ~1-2 min)...${NC}"
}

# Join the background readiness wait before opening the tunnel.
wait_webserver_ready() {
    if [[ -z "$WEBSERVER_WAIT_PID" ]]; then
        return 0
    fi
    echo -e "${BLUE}Waiting for Dagster webserver to become healthy...${NC}"
    wait "$WEBSERVER_WAIT_PID" || true
    WEBSERVER_WAIT_PID=""
    echo -e "${GREEN}✓ Dagster webserver is running${NC}"
}

scale_webserver_down() {
    local environment=$1
    echo -e "${YELLOW}Scaling Dagster webserver back down (-> 0)...${NC}"
    aws ecs update-service \
        --cluster "robosystems-dagster-${environment}-cluster" \
        --service "robosystems-dagster-webserver-${environment}" \
        --desired-count 0 --region "${AWS_REGION:-us-east-1}" >/dev/null 2>&1 || true
    echo -e "${GREEN}✓ Dagster webserver scaled to 0${NC}"
}

start_ssm_tunnel() {
    local remote_host=$1
    local remote_port=$2
    local local_port=$3
    local service_name=$4

    echo -e "${GREEN}Starting $service_name tunnel via SSM...${NC}"
    echo -e "${BLUE}Local: localhost:$local_port -> Remote: $remote_host:$remote_port${NC}"

    aws ssm start-session \
        --target "$BASTION_INSTANCE_ID" \
        --document-name AWS-StartPortForwardingSessionToRemoteHost \
        --parameters "{\"host\":[\"$remote_host\"],\"portNumber\":[\"$remote_port\"],\"localPortNumber\":[\"$local_port\"]}" \
        --region "$AWS_REGION"
}

start_ssm_tunnel_background() {
    local remote_host=$1
    local remote_port=$2
    local local_port=$3
    local service_name=$4

    echo -e "${GREEN}Starting $service_name tunnel in background...${NC}"
    echo -e "${BLUE}Local: localhost:$local_port -> Remote: $remote_host:$remote_port${NC}"

    aws ssm start-session \
        --target "$BASTION_INSTANCE_ID" \
        --document-name AWS-StartPortForwardingSessionToRemoteHost \
        --parameters "{\"host\":[\"$remote_host\"],\"portNumber\":[\"$remote_port\"],\"localPortNumber\":[\"$local_port\"]}" \
        --region "$AWS_REGION" &
}

setup_postgres_tunnel() {
    if [[ "$POSTGRES_ENDPOINT" == "NOT_FOUND" ]]; then
        echo -e "${RED}Error: PostgreSQL endpoint not found${NC}"
        exit 1
    fi

    echo ""
    echo -e "${YELLOW}Connect to PostgreSQL with:${NC}"
    echo "psql -h localhost -p 5432 -U postgres -d robosystems"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop the tunnel${NC}"
    echo ""

    start_ssm_tunnel "$POSTGRES_ENDPOINT" "5432" "5432" "PostgreSQL"
}

setup_valkey_tunnel() {
    if [[ "$VALKEY_ENDPOINT" == "NOT_FOUND" ]]; then
        echo -e "${RED}Error: Valkey endpoint not found${NC}"
        exit 1
    fi

    echo ""
    echo -e "${YELLOW}Connect to Valkey with:${NC}"
    echo "redis-cli -h localhost -p 6379"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop the tunnel${NC}"
    echo ""

    start_ssm_tunnel "$VALKEY_ENDPOINT" "6379" "6379" "Valkey"
}

setup_dagster_tunnel() {
    if [[ -z "$DAGSTER_ENDPOINT" || "$DAGSTER_ENDPOINT" == "NOT_FOUND" ]]; then
        echo -e "${RED}Error: Dagster endpoint not found${NC}"
        exit 1
    fi

    echo ""
    echo -e "${YELLOW}Access Dagster UI:${NC}"
    echo "Open http://127.0.0.1:8002 in your browser"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop the tunnel${NC}"
    echo ""

    start_ssm_tunnel "$DAGSTER_ENDPOINT" "3000" "8002" "Dagster"
}

setup_api_tunnel() {
    if [[ -z "$API_ENDPOINT" || "$API_ENDPOINT" == "NOT_FOUND" ]]; then
        echo -e "${RED}Error: API ALB endpoint not found${NC}"
        exit 1
    fi

    # Warn if API is in public mode - ALB tunnel has limitations
    if [[ "$API_ACCESS_MODE" == "public" ]]; then
        echo ""
        echo -e "${YELLOW}⚠️  WARNING: API is in '$API_ACCESS_MODE' mode${NC}"
        echo -e "${YELLOW}The ALB tunnel has limitations:${NC}"
        echo -e "${YELLOW}  - Admin endpoints (/admin/v1/*) are blocked by ALB rules${NC}"
        echo -e "${YELLOW}  - JWT/domain validation may cause issues${NC}"
        echo ""
        echo -e "${GREEN}Recommended: Use 'api-internal' instead for full access:${NC}"
        echo "  ./bin/tools/tunnels.sh $environment api-internal"
        echo ""
        echo -e "${BLUE}Continue with ALB tunnel anyway? (y/N): ${NC}"
        read -t 10 -r continue_choice || true
        if [[ ! "${continue_choice:-}" =~ ^[Yy]$ ]]; then
            echo -e "${YELLOW}Cancelled.${NC}"
            exit 0
        fi
    fi

    echo ""
    echo -e "${YELLOW}Access API (via ALB):${NC}"
    echo "curl http://localhost:8000/v1/status"
    echo "Open http://127.0.0.1:8000/docs in your browser for API docs"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop the tunnel${NC}"
    echo ""

    start_ssm_tunnel "$API_ENDPOINT" "80" "8000" "API"
}

setup_api_internal_tunnel() {
    # This tunnel goes directly to ECS tasks via Service Discovery
    # Bypasses ALB and its IP restrictions - ideal for admin access
    if [[ -z "$API_INTERNAL_ENDPOINT" ]]; then
        echo -e "${RED}Error: API internal endpoint not found${NC}"
        exit 1
    fi

    echo ""
    echo -e "${GREEN}======================================================================"
    echo "API Internal Tunnel (Service Discovery)"
    echo "======================================================================${NC}"
    echo ""
    echo -e "${YELLOW}This tunnel bypasses the ALB and connects directly to ECS tasks.${NC}"
    echo -e "${YELLOW}Use this for admin operations when ALB IP restrictions apply.${NC}"
    echo ""
    echo -e "${YELLOW}Access API:${NC}"
    echo "curl http://localhost:8000/v1/status"
    echo ""
    echo -e "${YELLOW}Admin API (requires this tunnel):${NC}"
    echo "just admin $environment stats"
    echo "Open http://127.0.0.1:8000/docs in your browser for API docs"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop the tunnel${NC}"
    echo ""

    start_ssm_tunnel "$API_INTERNAL_ENDPOINT" "8000" "8000" "API-Internal"
}

setup_all_tunnels() {
    local environment=$1
    local tunnel_count=0

    echo -e "${GREEN}Starting all available tunnels...${NC}"
    echo ""

    # Start tunnels in background
    if [[ "$POSTGRES_ENDPOINT" != "NOT_FOUND" ]]; then
        start_ssm_tunnel_background "$POSTGRES_ENDPOINT" "5432" "5432" "PostgreSQL"
        ((tunnel_count++))
        sleep 1
    fi

    if [[ "$VALKEY_ENDPOINT" != "NOT_FOUND" ]]; then
        start_ssm_tunnel_background "$VALKEY_ENDPOINT" "6379" "6379" "Valkey"
        ((tunnel_count++))
        sleep 1
    fi

    if [[ -n "$DAGSTER_ENDPOINT" && "$DAGSTER_ENDPOINT" != "NOT_FOUND" ]]; then
        start_ssm_tunnel_background "$DAGSTER_ENDPOINT" "3000" "8002" "Dagster"
        ((tunnel_count++))
        sleep 1
    fi

    # API tunnel: use ALB for internal mode, Service Discovery for public modes
    if [[ -n "$API_ENDPOINT" && "$API_ENDPOINT" != "NOT_FOUND" && "$API_ACCESS_MODE" == "internal" ]]; then
        start_ssm_tunnel_background "$API_ENDPOINT" "80" "8000" "API"
        ((tunnel_count++))
        sleep 1
    elif [[ "$API_ACCESS_MODE" == "public" ]]; then
        # Public mode: use api-internal (Service Discovery) to bypass ALB restrictions
        # This allows admin CLI access since ALB blocks /admin/v1/* in public mode
        if [[ -n "$API_INTERNAL_ENDPOINT" ]]; then
            echo -e "${YELLOW}API is in $API_ACCESS_MODE mode - using api-internal tunnel (bypasses ALB)${NC}"
            start_ssm_tunnel_background "$API_INTERNAL_ENDPOINT" "8000" "8000" "API-Internal"
            ((tunnel_count++))
            sleep 1
        fi
    fi

    if [[ $tunnel_count -eq 0 ]]; then
        echo -e "${RED}Error: No services found to tunnel${NC}"
        exit 1
    fi

    echo ""
    echo -e "${GREEN}✓ Started $tunnel_count tunnels${NC}"
    echo ""
    echo -e "${YELLOW}Connection commands:${NC}"

    if [[ "$POSTGRES_ENDPOINT" != "NOT_FOUND" ]]; then
        echo "PostgreSQL: psql -h localhost -p 5432 -U postgres -d robosystems"
    fi

    if [[ "$VALKEY_ENDPOINT" != "NOT_FOUND" ]]; then
        echo "Valkey:     redis-cli -h localhost -p 6379"
    fi

    if [[ -n "$DAGSTER_ENDPOINT" && "$DAGSTER_ENDPOINT" != "NOT_FOUND" ]]; then
        echo "Dagster:    Open http://127.0.0.1:8002 in your browser"
    fi

    if [[ "$API_ACCESS_MODE" == "internal" ]]; then
        echo "API:        curl http://localhost:8000/v1/status"
    elif [[ "$API_ACCESS_MODE" == "public" ]]; then
        echo "API:        curl http://localhost:8000/v1/status (via api-internal)"
        echo "Admin CLI:  just admin $environment stats"
    fi

    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop all tunnels${NC}"
    echo ""

    # Wait for background jobs
    wait
}

run_database_migration() {
    local environment=$1

    echo -e "${GREEN}Running database migrations on $environment environment...${NC}"
    echo ""

    # Default migration command
    local migration_command="upgrade head"
    local dry_run="false"

    # Show migration options
    echo -e "${YELLOW}Migration Options:${NC}"
    echo "1) Run all pending migrations (upgrade head)"
    echo "2) Show current migration version (current)"
    echo "3) Show migration history (history)"
    echo "4) Rollback one migration (downgrade -1)"
    echo "5) Dry run - preview migrations (upgrade head --sql)"
    echo ""
    echo -e "${BLUE}Select option (1-5, default=1): ${NC}"
    read -t 30 -r migration_choice
    echo ""

    case $migration_choice in
        2)
            migration_command="current"
            ;;
        3)
            migration_command="history"
            ;;
        4)
            echo -e "${YELLOW}⚠️  WARNING: This will rollback the last migration${NC}"
            echo -e "${YELLOW}Are you sure? (yes/no): ${NC}"
            read -r confirm
            if [[ "$confirm" != "yes" ]]; then
                echo -e "${RED}Migration cancelled${NC}"
                exit 0
            fi
            migration_command="downgrade -1"
            ;;
        5)
            dry_run="true"
            ;;
        1|"")
            # Default - run migrations
            ;;
        *)
            echo -e "${RED}Invalid selection${NC}"
            exit 1
            ;;
    esac

    echo -e "${BLUE}Executing migration command: alembic $migration_command${NC}"
    if [[ "$dry_run" == "true" ]]; then
        echo -e "${YELLOW}(DRY RUN MODE - No changes will be applied)${NC}"
    fi
    echo ""

    # Build the command arguments
    local migration_args="--command \"$migration_command\""
    if [[ "$dry_run" == "true" ]]; then
        migration_args="$migration_args --dry-run"
    fi

    echo -e "${YELLOW}Connecting to bastion via SSM and running migrations...${NC}"
    echo -e "${BLUE}Note: The latest Docker image ($environment tag) will be pulled automatically${NC}"
    echo "----------------------------------------"

    # Execute the migration command via SSM
    aws ssm start-session \
        --target "$BASTION_INSTANCE_ID" \
        --document-name AWS-StartInteractiveCommand \
        --parameters "{\"command\":[\"sudo -u ec2-user /usr/local/bin/run-migrations.sh $migration_args\"]}" \
        --region "$AWS_REGION"

    local exit_code=$?

    echo "----------------------------------------"

    if [[ "$exit_code" == "0" ]]; then
        echo -e "${GREEN}✓ Migration completed successfully${NC}"
    else
        echo -e "${RED}✗ Migration failed with exit code: $exit_code${NC}"
        exit 1
    fi
}

open_shell() {
    echo -e "${GREEN}Opening interactive shell on bastion...${NC}"
    echo -e "${YELLOW}Type 'exit' to close the session${NC}"
    echo ""

    aws ssm start-session \
        --target "$BASTION_INSTANCE_ID" \
        --region "$AWS_REGION"
}


# Main script
main() {
    # Check dependencies first
    check_dependencies

    local environment=""
    local service=""

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help|help)
                print_usage
                exit 0
                ;;
            --region|-r)
                AWS_REGION="$2"
                shift 2
                ;;
            prod|staging|dev)
                if [[ -z "$environment" ]]; then
                    environment="$1"
                else
                    echo -e "${RED}Error: Environment already specified as '$environment'${NC}"
                    print_usage
                    exit 1
                fi
                shift
                ;;
            postgres|valkey|dagster|api|api-internal|migrate|shell|all)
                if [[ -z "$service" ]]; then
                    service="$1"
                else
                    echo -e "${RED}Error: Service already specified as '$service'${NC}"
                    print_usage
                    exit 1
                fi
                shift
                ;;
            *)
                echo -e "${RED}Error: Unknown argument '$1'${NC}"
                print_usage
                exit 1
                ;;
        esac
    done

    # Set defaults if not specified
    if [[ -z "$environment" ]]; then
        environment="$DEFAULT_ENVIRONMENT"
    fi

    if [[ -z "$service" ]]; then
        service="all"
    fi

    # Validate environment
    case $environment in
        prod|staging|dev)
            ;;
        *)
            echo -e "${RED}Error: Invalid environment '$environment'${NC}"
            print_usage
            exit 1
            ;;
    esac

    # Discover infrastructure
    discover_infrastructure "$environment" "$service"

    # Remember the environment so cleanup_on_exit can scale the webserver back down
    ENVIRONMENT="$environment"

    # The Dagster webserver defaults to 0 tasks (cost); bring it up on demand
    # only for the services that actually need it. Fire this BEFORE the bastion
    # boot so the webserver cold start overlaps with it (both are independent).
    case $service in
        dagster|all)
            start_webserver_scale_up "$environment"
            ;;
    esac

    # Check and start bastion if needed (runs concurrently with the webserver cold start)
    check_bastion_status "$environment"

    # Join the webserver readiness wait before opening the tunnel
    case $service in
        dagster|all)
            wait_webserver_ready
            ;;
    esac

    # Set up tunnels based on service
    case $service in
        postgres)
            setup_postgres_tunnel
            ;;
        valkey)
            setup_valkey_tunnel
            ;;
        dagster)
            setup_dagster_tunnel
            ;;
        api)
            setup_api_tunnel
            ;;
        api-internal)
            setup_api_internal_tunnel
            ;;
        migrate)
            run_database_migration "$environment"
            ;;
        shell)
            open_shell
            ;;
        all|"")
            setup_all_tunnels "$environment"
            ;;
        *)
            echo -e "${RED}Error: Unknown service '$service'${NC}"
            print_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
