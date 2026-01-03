#!/bin/bash

# RoboSystems Bastion Host SSH Tunnels
# Usage: ./bin/tunnels.sh [environment] [service]
# Environments: prod, staging, dev
# Services: postgres, valkey, all

set -euo pipefail

# Default configuration
DEFAULT_ENVIRONMENT="prod"
SSH_KEY="~/.ssh/id_rsa"

# Dynamic configuration (populated by discover_infrastructure)
BASTION_HOST=""
POSTGRES_ENDPOINT=""
VALKEY_ENDPOINT=""
DAGSTER_ENDPOINT=""

# Validate dependencies
check_dependencies() {
    local missing_deps=()

    # Check for required commands
    for cmd in aws jq ssh; do
        if ! command -v "$cmd" &> /dev/null; then
            missing_deps+=("$cmd")
        fi
    done

    if [ ${#missing_deps[@]} -ne 0 ]; then
        echo -e "${RED}Error: Missing required dependencies: ${missing_deps[*]}${NC}"
        echo "Please install missing dependencies and try again."
        exit 1
    fi

    # Check AWS CLI version
    local aws_version=$(aws --version 2>&1 | cut -d' ' -f1 | cut -d'/' -f2)
    local major_version=$(echo "$aws_version" | cut -d'.' -f1)

    if [ "$major_version" -lt 2 ]; then
        echo -e "${YELLOW}Warning: AWS CLI v2 is recommended (found v${aws_version})${NC}"
    fi

    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        echo -e "${RED}Error: AWS credentials not configured or expired${NC}"
        echo "Please run 'aws configure' or set AWS credentials"
        exit 1
    fi
}

# Bastion management variables
BASTION_INSTANCE_ID=""
BASTION_WAS_STARTED="false"

# Cleanup function to stop bastion if we started it
cleanup_on_exit() {
    if [[ "$BASTION_WAS_STARTED" == "true" ]] && [[ -n "$BASTION_INSTANCE_ID" ]]; then
        echo ""
        echo -e "${YELLOW}Stopping bastion instance (was originally stopped)...${NC}"
        aws ec2 stop-instances --instance-ids "$BASTION_INSTANCE_ID" >/dev/null 2>&1 || true
        echo -e "${GREEN}✓ Bastion instance stop command sent${NC}"
    fi
}

# Set up trap to cleanup on exit
trap cleanup_on_exit EXIT INT TERM

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_usage() {
    echo -e "${BLUE}Usage: $0 [environment] [service] [--key|-k <ssh_key_path>]${NC}"
    echo ""
    echo -e "${YELLOW}Available environments:${NC}"
    echo "  prod     - Production environment (default)"
    echo "  staging  - Staging environment"
    echo "  dev      - Development environment"
    echo ""
    echo -e "${GREEN}======================================================================"
    echo "SSH Tunnels - Access internal services via bastion host"
    echo "======================================================================${NC}"
    echo "  postgres      - PostgreSQL tunnel (localhost:5432)"
    echo "  valkey        - Valkey ElastiCache tunnel (localhost:6379)"
    echo "  dagster       - Dagster webserver tunnel (localhost:3003)"
    echo "  all           - All database tunnels (postgres + valkey)"
    echo ""
    echo -e "${GREEN}======================================================================"
    echo "Database Operations"
    echo "======================================================================${NC}"
    echo "  migrate       - Run database migrations via bastion"
    echo ""
    echo -e "${YELLOW}SSH Key Options:${NC}"
    echo "  --key, -k <path>  - Path to SSH private key"
    echo "                      Default: ~/.ssh/id_rsa"
    echo ""
}

check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        echo -e "${RED}Error: AWS CLI not found${NC}"
        echo "Please install AWS CLI and configure your credentials."
        exit 1
    fi

    # Test AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        echo -e "${RED}Error: AWS credentials not configured${NC}"
        echo "Please run 'aws configure' to set up your credentials."
        exit 1
    fi
}

discover_infrastructure() {
    local environment=$1

    # Validate environment
    if [[ ! "$environment" =~ ^(prod|staging|dev)$ ]]; then
        echo -e "${RED}Error: Invalid environment '$environment'. Must be prod, staging, or dev.${NC}"
        return 1
    fi

    echo -e "${BLUE}Discovering infrastructure for $environment environment...${NC}"

    # Discover bastion host
    local env_capitalized="$(echo ${environment:0:1} | tr 'a-z' 'A-Z')${environment:1}"
    local bastion_stack="RoboSystemsBastion${env_capitalized}"
    echo -e "${YELLOW}Looking for bastion stack: $bastion_stack${NC}"

    BASTION_HOST=$(aws cloudformation describe-stacks \
        --stack-name "$bastion_stack" \
        --query 'Stacks[0].Outputs[?OutputKey==`BastionPublicIP`].OutputValue' \
        --output text 2>/dev/null || echo "")

    if [[ -z "$BASTION_HOST" ]]; then
        echo -e "${RED}Error: Could not find bastion host for $environment${NC}"
        echo "Make sure the bastion stack '$bastion_stack' is deployed."
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
        return
    fi

    echo -e "${YELLOW}Looking for postgres stack: $postgres_stack${NC}"

    POSTGRES_ENDPOINT=$(aws cloudformation describe-stacks \
        --stack-name "$postgres_stack" \
        --query 'Stacks[0].Outputs[?OutputKey==`RDSInstanceEndpoint`].OutputValue' \
        --output text 2>/dev/null || echo "")

    if [[ -z "$POSTGRES_ENDPOINT" ]]; then
        echo -e "${YELLOW}Warning: Could not find PostgreSQL endpoint for $environment${NC}"
        POSTGRES_ENDPOINT="NOT_FOUND"
    fi

    # Discover Valkey ElastiCache endpoint
    echo -e "${YELLOW}Looking for Valkey ElastiCache cluster...${NC}"

    local valkey_stack="RoboSystemsValkey${env_capitalized}"
    VALKEY_ENDPOINT=$(aws cloudformation describe-stacks \
        --stack-name "$valkey_stack" \
        --query 'Stacks[0].Outputs[?OutputKey==`ValkeyEndpoint`].OutputValue' \
        --output text 2>/dev/null || echo "")

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
        --output text 2>/dev/null || echo "")

    if [[ -z "$DAGSTER_ENDPOINT" || "$DAGSTER_ENDPOINT" == "None" ]]; then
        # Fallback to standard Service Discovery naming
        DAGSTER_ENDPOINT="webserver.dagster.${environment}.robosystems.local"
        echo -e "${YELLOW}Using default Dagster endpoint: $DAGSTER_ENDPOINT${NC}"
    fi

    # Show discovered endpoints
    echo -e "${GREEN}✓ Infrastructure discovered:${NC}"
    echo -e "  Bastion Host: ${GREEN}$BASTION_HOST${NC}"
    echo -e "  PostgreSQL:   ${GREEN}$POSTGRES_ENDPOINT${NC}"
    echo -e "  Valkey:       ${GREEN}$VALKEY_ENDPOINT${NC}"
    echo -e "  Dagster:      ${GREEN}$DAGSTER_ENDPOINT${NC}"
    echo ""
}

check_ssh_key() {
    local key_path=$(eval echo $SSH_KEY)
    if [[ ! -f "$key_path" ]]; then
        echo -e "${RED}Error: SSH key not found at $key_path${NC}"
        echo "Make sure your SSH key is in the correct location."
        exit 1
    fi

    # Check permissions (detect OS for correct stat command)
    local perms
    if [[ "$OSTYPE" == "darwin"* ]]; then
        perms=$(stat -f "%OLp" "$key_path")
    else
        perms=$(stat -c "%a" "$key_path")
    fi

    if [[ "$perms" != "600" ]]; then
        echo -e "${YELLOW}Warning: SSH key has wrong permissions ($perms). Setting to 600...${NC}"
        chmod 600 "$key_path"
    fi
}

check_bastion_status() {
    local environment=$1

    echo -e "${BLUE}Checking bastion host status...${NC}"

    # Get bastion instance ID
    local env_capitalized="$(echo ${environment:0:1} | tr 'a-z' 'A-Z')${environment:1}"
    BASTION_INSTANCE_ID=$(aws ec2 describe-instances \
        --filters "Name=tag:aws:cloudformation:stack-name,Values=RoboSystemsBastion${env_capitalized}" \
                  "Name=instance-state-name,Values=running,stopped" \
        --query 'Reservations[0].Instances[0].InstanceId' \
        --output text 2>/dev/null || echo "")

    if [[ -z "$BASTION_INSTANCE_ID" || "$BASTION_INSTANCE_ID" == "None" ]]; then
        echo -e "${RED}Error: Could not find bastion instance for $environment${NC}"
        exit 1
    fi

    # Check instance state
    local instance_state=$(aws ec2 describe-instances \
        --instance-ids "$BASTION_INSTANCE_ID" \
        --query 'Reservations[0].Instances[0].State.Name' \
        --output text 2>/dev/null || echo "")

    echo -e "${YELLOW}Bastion instance state: $instance_state${NC}"

    if [[ "$instance_state" == "stopped" ]]; then
        echo -e "${YELLOW}Starting bastion instance...${NC}"
        aws ec2 start-instances --instance-ids "$BASTION_INSTANCE_ID" >/dev/null

        # Mark that we started the bastion
        BASTION_WAS_STARTED="true"

        # Wait for instance to be running
        echo -e "${BLUE}Waiting for instance to reach running state...${NC}"
        aws ec2 wait instance-running --instance-ids "$BASTION_INSTANCE_ID"

        echo -e "${GREEN}✓ Bastion instance is now running${NC}"

        # Wait additional time for SSH service to start
        echo -e "${BLUE}Waiting 30 seconds for SSH service to initialize...${NC}"
        sleep 30

        # Update the bastion host IP after starting
        echo -e "${BLUE}Updating bastion host IP address...${NC}"
        local bastion_stack="RoboSystemsBastion${env_capitalized}"
        BASTION_HOST=$(aws cloudformation describe-stacks \
            --stack-name "$bastion_stack" \
            --query 'Stacks[0].Outputs[?OutputKey==`BastionPublicIP`].OutputValue' \
            --output text 2>/dev/null || echo "")

        echo -e "${GREEN}Updated bastion host IP: $BASTION_HOST${NC}"

    elif [[ "$instance_state" == "running" ]]; then
        echo -e "${GREEN}✓ Bastion instance is already running${NC}"
    else
        echo -e "${RED}Error: Bastion instance is in unexpected state: $instance_state${NC}"
        exit 1
    fi
}

test_connectivity() {
    echo -e "${BLUE}Testing connection to bastion host...${NC}"
    if ! nc -z -w30 $BASTION_HOST 22 2>/dev/null; then
        echo -e "${RED}Error: Cannot connect to bastion host $BASTION_HOST:22${NC}"
        echo "Check your internet connection and bastion host status."
        exit 1
    fi
    echo -e "${GREEN}✓ Bastion host is reachable${NC}"
}

setup_postgres_tunnel() {
    if [[ "$POSTGRES_ENDPOINT" == "NOT_FOUND" ]]; then
        echo -e "${RED}Error: PostgreSQL endpoint not found${NC}"
        exit 1
    fi

    echo -e "${GREEN}Setting up PostgreSQL tunnel...${NC}"
    echo -e "${BLUE}Local: localhost:5432 -> Remote: $POSTGRES_ENDPOINT:5432${NC}"
    echo ""
    echo -e "${YELLOW}Connect to PostgreSQL with:${NC}"
    echo "psql -h localhost -p 5432 -U postgres -d robosystems"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop the tunnel${NC}"
    echo ""

    ssh -i $SSH_KEY -N -L 5432:$POSTGRES_ENDPOINT:5432 ec2-user@$BASTION_HOST
}

setup_valkey_tunnel() {
    if [[ "$VALKEY_ENDPOINT" == "NOT_FOUND" ]]; then
        echo -e "${RED}Error: Valkey endpoint not found${NC}"
        exit 1
    fi

    echo -e "${GREEN}Setting up Valkey tunnel...${NC}"
    echo -e "${BLUE}Local: localhost:6379 -> Remote: $VALKEY_ENDPOINT:6379${NC}"
    echo ""
    echo -e "${YELLOW}Connect to Valkey with:${NC}"
    echo "redis-cli -h localhost -p 6379"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop the tunnel${NC}"
    echo ""

    ssh -i $SSH_KEY -N -L 6379:$VALKEY_ENDPOINT:6379 ec2-user@$BASTION_HOST
}

setup_dagster_tunnel() {
    if [[ -z "$DAGSTER_ENDPOINT" || "$DAGSTER_ENDPOINT" == "NOT_FOUND" ]]; then
        echo -e "${RED}Error: Dagster endpoint not found${NC}"
        exit 1
    fi

    echo -e "${GREEN}Setting up Dagster webserver tunnel...${NC}"
    echo -e "${BLUE}Local: localhost:3003 -> Remote: $DAGSTER_ENDPOINT:3000${NC}"
    echo ""
    echo -e "${YELLOW}Access Dagster UI:${NC}"
    echo "Open http://localhost:3003 in your browser"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop the tunnel${NC}"
    echo ""

    ssh -i $SSH_KEY -N -L 3003:$DAGSTER_ENDPOINT:3000 ec2-user@$BASTION_HOST
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

    # Build the command arguments
    local migration_args="--command \"$migration_command\""
    if [[ "$dry_run" == "true" ]]; then
        migration_args="$migration_args --dry-run"
    fi

    echo -e "${BLUE}Executing migration command: alembic $migration_command${NC}"
    if [[ "$dry_run" == "true" ]]; then
        echo -e "${YELLOW}(DRY RUN MODE - No changes will be applied)${NC}"
    fi
    echo ""

    # Run the migration script directly via SSH
    echo -e "${YELLOW}Connecting to bastion and running migrations...${NC}"
    echo -e "${BLUE}Note: The latest Docker image ($environment tag) will be pulled automatically${NC}"
    echo "----------------------------------------"

    # Execute the migration command via SSH
    ssh -i "$SSH_KEY" \
        -o "StrictHostKeyChecking=no" \
        -o "UserKnownHostsFile=/dev/null" \
        -t ec2-user@$BASTION_HOST \
        "sudo -u ec2-user /usr/local/bin/run-migrations.sh $migration_args"

    local exit_code=$?

    echo "----------------------------------------"

    if [[ "$exit_code" == "0" ]]; then
        echo -e "${GREEN}✓ Migration completed successfully${NC}"
    else
        echo -e "${RED}✗ Migration failed with exit code: $exit_code${NC}"
        exit 1
    fi
}

setup_all_tunnels() {
    local environment=$1
    local available_services=()
    local tunnel_args=""

    # Check which services are available
    if [[ "$POSTGRES_ENDPOINT" != "NOT_FOUND" ]]; then
        available_services+=("PostgreSQL")
        tunnel_args="$tunnel_args -L 5432:$POSTGRES_ENDPOINT:5432"
    fi

    if [[ "$VALKEY_ENDPOINT" != "NOT_FOUND" ]]; then
        available_services+=("Valkey")
        tunnel_args="$tunnel_args -L 6379:$VALKEY_ENDPOINT:6379"
    fi

    if [[ -n "$DAGSTER_ENDPOINT" && "$DAGSTER_ENDPOINT" != "NOT_FOUND" ]]; then
        available_services+=("Dagster")
        tunnel_args="$tunnel_args -L 3003:$DAGSTER_ENDPOINT:3000"
    fi

    if [[ ${#available_services[@]} -eq 0 ]]; then
        echo -e "${RED}Error: No services found to tunnel${NC}"
        exit 1
    fi

    echo -e "${GREEN}Setting up tunnels for: ${available_services[*]}${NC}"

    if [[ "$POSTGRES_ENDPOINT" != "NOT_FOUND" ]]; then
        echo -e "${BLUE}PostgreSQL: localhost:5432 -> $POSTGRES_ENDPOINT:5432${NC}"
    fi

    if [[ "$VALKEY_ENDPOINT" != "NOT_FOUND" ]]; then
        echo -e "${BLUE}Valkey:     localhost:6379 -> $VALKEY_ENDPOINT:6379${NC}"
    fi

    if [[ -n "$DAGSTER_ENDPOINT" && "$DAGSTER_ENDPOINT" != "NOT_FOUND" ]]; then
        echo -e "${BLUE}Dagster:    localhost:3003 -> $DAGSTER_ENDPOINT:3000${NC}"
    fi

    echo ""
    echo -e "${YELLOW}Connection commands:${NC}"

    if [[ "$POSTGRES_ENDPOINT" != "NOT_FOUND" ]]; then
        echo "PostgreSQL: psql -h localhost -p 5432 -U postgres -d robosystems"
    fi

    if [[ "$VALKEY_ENDPOINT" != "NOT_FOUND" ]]; then
        echo "Valkey:     redis-cli -h localhost -p 6379"
        echo "            Note: Valkey uses AUTH. Get token with:"
        echo "            aws secretsmanager get-secret-value --secret-id robosystems/$environment/valkey --query 'SecretString' | jq -r '.VALKEY_AUTH_TOKEN'"
        echo "            Then connect: redis-cli -h localhost -p 6379 -a <AUTH_TOKEN> --tls"
    fi

    if [[ -n "$DAGSTER_ENDPOINT" && "$DAGSTER_ENDPOINT" != "NOT_FOUND" ]]; then
        echo "Dagster:    Open http://localhost:3003 in your browser"
    fi

    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop all tunnels${NC}"
    echo ""

    # Execute SSH with dynamic tunnel arguments
    ssh -i $SSH_KEY -N $tunnel_args ec2-user@$BASTION_HOST
}

cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up tunnels...${NC}"

    # Stop bastion instance if we started it
    if [[ "$BASTION_WAS_STARTED" == "true" && -n "$BASTION_INSTANCE_ID" ]]; then
        echo -e "${YELLOW}Stopping bastion instance...${NC}"
        aws ec2 stop-instances --instance-ids "$BASTION_INSTANCE_ID" >/dev/null 2>&1
        echo -e "${GREEN}✓ Bastion instance stop command sent${NC}"
    fi

    echo -e "${GREEN}Tunnels stopped.${NC}"
}

# Set trap for cleanup on script exit
trap cleanup EXIT INT TERM

# Main script
main() {
    # Check dependencies first
    check_dependencies

    local environment=""
    local service=""
    local custom_ssh_key=""

    # Parse arguments with support for SSH key parameter
    while [[ $# -gt 0 ]]; do
        case $1 in
            --key|-k)
                custom_ssh_key="$2"
                shift 2
                ;;
            -h|--help|help)
                print_usage
                exit 0
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
            postgres|valkey|dagster|migrate|all)
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

    # Update SSH_KEY if custom key provided
    if [[ -n "$custom_ssh_key" ]]; then
        SSH_KEY="$custom_ssh_key"
        echo -e "${BLUE}Using custom SSH key: $SSH_KEY${NC}"
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

    # Check prerequisites
    check_aws_cli
    check_ssh_key

    # Discover infrastructure
    discover_infrastructure "$environment"

    # Check and start bastion if needed
    check_bastion_status "$environment"

    # Test connectivity
    test_connectivity

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
        migrate)
            run_database_migration "$environment"
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
