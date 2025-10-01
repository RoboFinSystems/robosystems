#!/bin/bash

# RoboSystems Bastion Host SSH Tunnels
# Usage: ./bin/tunnels.sh [environment] [service]
# Environments: prod, staging, dev
# Services: postgres, kuzu, valkey, all

set -euo pipefail

# Default configuration
DEFAULT_ENVIRONMENT="prod"
SSH_KEY="~/.ssh/id_rsa"

# Dynamic configuration (populated by discover_infrastructure)
BASTION_HOST=""
POSTGRES_ENDPOINT=""
KUZU_ENDPOINT=""
VALKEY_ENDPOINT=""
KUZU_INSTANCES_JSON=""  # JSON array of all Kuzu instances

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
    echo -e "${YELLOW}Available services:${NC}"
    echo "  postgres      - PostgreSQL tunnel (localhost:5432)"
    echo "  kuzu          - Kuzu API tunnel to default instance (localhost:8001)"
    echo "  kuzu-select   - Select specific Kuzu instance to tunnel"
    echo "  kuzu-forward  - Port forward ALL Kuzu instances (8001, 8002, ...)"
    echo "  kuzu-direct   - Direct SSH access to Kuzu instance filesystem"
    echo "  kuzu-list     - List all available Kuzu instances"
    echo "  kuzu-master   - Tunnel to shared master (port 8002)"
    echo "  kuzu-replica  - Tunnel to shared replica (port 8002)"
    echo "  valkey        - Valkey ElastiCache tunnel (localhost:6379)"
    echo "  migrate       - Run database migrations via bastion"
    echo "  admin         - Run admin operations via bastion"
    echo "  admin-help    - Show available admin operations"
    echo "  all           - All services (default)"
    echo ""
    echo -e "${YELLOW}SSH Key Options:${NC}"
    echo "  --key, -k <path>  - Path to SSH private key"
    echo "                      Default: ~/.ssh/id_rsa"
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo "  $0 prod postgres"
    echo "  $0 staging all --key ~/.ssh/my-key.pem"
    echo "  $0 prod kuzu-list        # List all Kuzu instances"
    echo "  $0 prod kuzu-select      # Tunnel to specific instance"
    echo "  $0 prod kuzu-forward     # Forward ALL instances (8001, 8002, ...)"
    echo "  $0 prod kuzu-direct      # SSH to Kuzu filesystem"
    echo "  $0 prod migrate          # Run database migrations"
    echo "  $0 staging migrate       # Run migrations on staging"
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
    # Stack names vary by environment - production uses Prod suffix, others use Staging
    local postgres_stack=""
    if [[ "$environment" == "prod" ]]; then
        postgres_stack="RoboSystemsPostgresIAMProd"
    elif [[ "$environment" == "staging" ]]; then
        postgres_stack="RoboSystemsPostgresIAMStaging"
    else
        # Dev environment would use local postgres
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

    # Discover Kuzu endpoints from DynamoDB registry
    echo -e "${YELLOW}Looking for Kuzu instances from registry...${NC}"

    # Get all active Kuzu instances from DynamoDB
    local instance_registry_table="robosystems-kuzu-${environment}-instance-registry"

    # Query DynamoDB for all healthy instances
    local kuzu_instances=$(aws dynamodb scan \
        --table-name "$instance_registry_table" \
        --filter-expression "#status = :healthy" \
        --expression-attribute-names '{"#status": "status"}' \
        --expression-attribute-values '{":healthy": {"S": "healthy"}}' \
        --query 'Items[*].[instance_id.S, private_ip.S, node_type.S, repository_type.S, database_count.N]' \
        --output json 2>/dev/null || echo "[]")

    if [[ "$kuzu_instances" == "[]" || -z "$kuzu_instances" ]]; then
        echo -e "${YELLOW}Warning: Could not find any Kuzu instances in registry${NC}"
        # Fallback to EC2 tag-based discovery
        KUZU_ENDPOINT=$(aws ec2 describe-instances \
            --filters "Name=tag:Service,Values=RoboSystems" \
                      "Name=tag:Component,Values=Kuzu" \
                      "Name=tag:Environment,Values=$environment" \
                      "Name=instance-state-name,Values=running" \
            --query 'Reservations[0].Instances[0].PrivateIpAddress' \
            --output text 2>/dev/null || echo "")

        if [[ -z "$KUZU_ENDPOINT" || "$KUZU_ENDPOINT" == "None" ]]; then
            KUZU_ENDPOINT="NOT_FOUND"
        fi
    else
        # Store all instances for later selection
        KUZU_INSTANCES_JSON="$kuzu_instances"
        # Use first instance as default
        KUZU_ENDPOINT=$(echo "$kuzu_instances" | jq -r '.[0][1] // "NOT_FOUND"')
        local instance_count=$(echo "$kuzu_instances" | jq '. | length')
        echo -e "${GREEN}Found $instance_count Kuzu instance(s) in registry${NC}"
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


    # Show discovered endpoints
    echo -e "${GREEN}✓ Infrastructure discovered:${NC}"
    echo -e "  Bastion Host: ${GREEN}$BASTION_HOST${NC}"
    echo -e "  PostgreSQL:   ${GREEN}$POSTGRES_ENDPOINT${NC}"
    echo -e "  Kuzu:         ${GREEN}$KUZU_ENDPOINT${NC}"
    echo -e "  Valkey:       ${GREEN}$VALKEY_ENDPOINT${NC}"
    echo ""
}

check_ssh_key() {
    local key_path=$(eval echo $SSH_KEY)
    if [[ ! -f "$key_path" ]]; then
        echo -e "${RED}Error: SSH key not found at $key_path${NC}"
        echo "Make sure your SSH key is in the correct location."
        exit 1
    fi

    # Check permissions
    local perms=$(stat -f "%OLp" "$key_path" 2>/dev/null || stat -c "%a" "$key_path" 2>/dev/null)
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

        echo -e "${BLUE}Waiting 30 seconds for bastion to boot...${NC}"
        sleep 30

        # Wait for instance to be running
        echo -e "${BLUE}Waiting for instance to reach running state...${NC}"
        aws ec2 wait instance-running --instance-ids "$BASTION_INSTANCE_ID"

        echo -e "${GREEN}✓ Bastion instance is now running${NC}"

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
    if ! nc -z -w5 $BASTION_HOST 22 2>/dev/null; then
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

setup_kuzu_tunnel() {
    if [[ "$KUZU_ENDPOINT" == "NOT_FOUND" ]]; then
        echo -e "${RED}Error: Kuzu endpoint not found${NC}"
        exit 1
    fi

    echo -e "${GREEN}Setting up Kuzu API tunnel...${NC}"
    echo -e "${BLUE}Local: localhost:8001 -> Remote: $KUZU_ENDPOINT:8001${NC}"
    echo ""
    echo -e "${YELLOW}Access Kuzu API:${NC}"
    echo "  API Docs:    http://localhost:8001/docs"
    echo "  OpenAPI:     http://localhost:8001/openapi.json"
    echo "  Health:      http://localhost:8001/health"
    echo ""
    echo -e "${YELLOW}Example query:${NC}"
    echo "  curl -X POST http://localhost:8001/v1/kg1a2b3c/query \\"
    echo "    -H 'Content-Type: application/json' \\"
    echo "    -d '{\"query\": \"MATCH (e:Entity) RETURN e LIMIT 5\"}'"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop the tunnel${NC}"
    echo ""

    ssh -i $SSH_KEY -N -L 8001:$KUZU_ENDPOINT:8001 ec2-user@$BASTION_HOST
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

setup_kuzu_direct() {
    # Check if we have multiple instances
    if [[ -n "$KUZU_INSTANCES_JSON" && "$KUZU_INSTANCES_JSON" != "[]" ]]; then
        local instance_count=$(echo "$KUZU_INSTANCES_JSON" | jq '. | length')
        if [[ "$instance_count" -gt 1 ]]; then
            echo -e "${YELLOW}Multiple Kuzu instances found. Please select one:${NC}"
            setup_kuzu_select_direct
            return
        fi
    fi

    if [[ "$KUZU_ENDPOINT" == "NOT_FOUND" ]]; then
        echo -e "${RED}Error: Kuzu endpoint not found${NC}"
        exit 1
    fi

    echo -e "${GREEN}Connecting to Kuzu instance via AWS Systems Manager...${NC}"
    echo ""

    # Try to get instance ID from registry if available
    local selected_instance_id=""
    if [[ -n "$KUZU_INSTANCES_JSON" && "$KUZU_INSTANCES_JSON" != "[]" ]]; then
        selected_instance_id=$(echo "$KUZU_INSTANCES_JSON" | jq -r --arg ip "$KUZU_ENDPOINT" '.[] | select(.[1] == $ip) | .[0]')
    fi

    # If not found in registry, try SSM
    if [[ -z "$selected_instance_id" ]]; then
        echo -e "${YELLOW}Finding Kuzu instance ID via SSM...${NC}"
        selected_instance_id=$(aws ssm describe-instance-information \
            --filters "Key=tag:Service,Values=RoboSystems" \
                      "Key=tag:Component,Values=Kuzu" \
            --query 'InstanceInformationList[0].InstanceId' \
            --output text 2>/dev/null)
    fi

    if [[ -z "$selected_instance_id" || "$selected_instance_id" == "None" ]]; then
        echo -e "${YELLOW}SSM not available, falling back to SSH via bastion...${NC}"
        echo ""

        # Fallback to SSH method
        echo -e "${GREEN}Setting up SSH to Kuzu instance via bastion...${NC}"
        echo -e "${BLUE}Bastion: $BASTION_HOST -> Kuzu: $KUZU_ENDPOINT${NC}"
        echo ""

        # Add key to SSH agent
        ssh-add $SSH_KEY 2>/dev/null || {
            echo -e "${RED}Failed to add SSH key to agent. Make sure ssh-agent is running:${NC}"
            echo "eval \$(ssh-agent)"
            echo "ssh-add $SSH_KEY"
            exit 1
        }

        echo -e "${GREEN}✓ SSH key added to agent${NC}"
        echo -e "${BLUE}Connecting to Kuzu instance via bastion with SSH...${NC}"
        echo ""

        # Connect with agent forwarding, then SSH to Kuzu instance
        ssh -A -i $SSH_KEY ec2-user@$BASTION_HOST -t "ssh -o StrictHostKeyChecking=no ec2-user@$KUZU_ENDPOINT"
        return
    fi

    echo -e "${GREEN}✓ Found Kuzu instance: $selected_instance_id${NC}"
    echo -e "${BLUE}Starting Systems Manager session...${NC}"
    echo ""
    echo -e "${YELLOW}You'll have shell access to the Kuzu instance filesystem.${NC}"
    echo -e "${YELLOW}Useful commands:${NC}"
    echo "  docker ps                          # List running containers"
    echo "  docker logs kuzu-writer -f         # View Kuzu logs"
    echo "  curl localhost:8001/health         # Check API health"
    echo "  ls -la /mnt/kuzu-data/databases/   # List databases"
    echo "  df -h /mnt/kuzu-data/              # Check disk usage"
    echo "  du -sh /mnt/kuzu-data/databases/*  # Check database sizes"
    echo ""
    echo -e "${YELLOW}Database file access:${NC}"
    echo "  cd /mnt/kuzu-data/databases/kg1a2b3c45  # Enter database directory"
    echo "  ls -la                                      # List database files"
    echo ""

    # Connect via SSM
    aws ssm start-session --target "$selected_instance_id" --document-name "AWS-StartInteractiveCommand" --parameters "command=sudo -u ec2-user -i bash"
}


list_kuzu_instances() {
    echo -e "${GREEN}Available Kuzu Instances:${NC}"
    echo ""

    if [[ -z "$KUZU_INSTANCES_JSON" || "$KUZU_INSTANCES_JSON" == "[]" ]]; then
        echo -e "${YELLOW}No Kuzu instances found in registry${NC}"
        return
    fi

    # Parse and display instances
    local index=1
    echo "$KUZU_INSTANCES_JSON" | jq -r '.[] | @tsv' | while IFS=$'\t' read -r instance_id private_ip node_type repo_type db_count; do
        local instance_name=""
        if [[ "$node_type" == "shared_master" ]]; then
            instance_name="$(echo $repo_type | tr '[:lower:]' '[:upper:]') Repository Master"
        elif [[ "$node_type" == "shared_replica" ]]; then
            instance_name="$(echo $repo_type | tr '[:lower:]' '[:upper:]') Repository Replica"
        else
            instance_name="Entity Writer (${db_count} DBs)"
        fi

        echo -e "${BLUE}$index)${NC} $instance_name"
        echo "   Instance ID: $instance_id"
        echo "   Private IP:  $private_ip"
        echo "   Type:        $node_type"
        if [[ "$node_type" == "shared_master" ]] || [[ "$node_type" == "shared_replica" ]]; then
            echo "   Repository:  $repo_type"
        fi
        echo ""
        ((index++))
    done
}

setup_kuzu_master_tunnel() {
    if [[ -z "$KUZU_INSTANCES_JSON" || "$KUZU_INSTANCES_JSON" == "[]" ]]; then
        echo -e "${RED}No Kuzu instances available${NC}"
        exit 1
    fi

    # Find shared master instance
    local master_instance=$(echo "$KUZU_INSTANCES_JSON" | jq -r '.[] | select(.[2] == "shared_master")')

    if [[ -z "$master_instance" ]]; then
        echo -e "${RED}No shared master instance found${NC}"
        echo -e "${YELLOW}Make sure Kuzu shared writers are deployed${NC}"
        exit 1
    fi

    local master_ip=$(echo "$master_instance" | jq -r '.[1]')
    local instance_id=$(echo "$master_instance" | jq -r '.[0]')
    local repo_type=$(echo "$master_instance" | jq -r '.[3]')

    echo -e "${GREEN}Connecting to shared master instance:${NC}"
    echo "  Instance ID: $instance_id"
    echo "  Repository:  $repo_type"
    echo "  Endpoint:    $master_ip:8002"
    echo ""
    echo -e "${YELLOW}Setting up SSH tunnel...${NC}"
    echo -e "${BLUE}Shared Master: localhost:8002 -> $master_ip:8002${NC}"
    echo ""
    echo -e "${YELLOW}Connection commands:${NC}"
    echo "  curl http://localhost:8002/status"
    echo "  curl http://localhost:8002/health"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to close the tunnel${NC}"
    echo ""

    # Create tunnel on port 8002
    ssh -i "$SSH_KEY" \
        -o "StrictHostKeyChecking=no" \
        -o "UserKnownHostsFile=/dev/null" \
        -N -L 8002:$master_ip:8002 \
        ec2-user@$BASTION_HOST
}

setup_kuzu_replica_tunnel() {
    if [[ -z "$KUZU_INSTANCES_JSON" || "$KUZU_INSTANCES_JSON" == "[]" ]]; then
        echo -e "${RED}No Kuzu instances available${NC}"
        exit 1
    fi

    # Find shared replica instances
    local replica_instances=$(echo "$KUZU_INSTANCES_JSON" | jq -r '.[] | select(.[2] == "shared_replica")')

    if [[ -z "$replica_instances" ]]; then
        echo -e "${RED}No shared replica instances found${NC}"
        echo -e "${YELLOW}Make sure Kuzu shared writers with replicas are deployed${NC}"
        exit 1
    fi

    # Count replicas
    local replica_count=$(echo "$KUZU_INSTANCES_JSON" | jq '[.[] | select(.[2] == "shared_replica")] | length')

    if [[ "$replica_count" -gt 1 ]]; then
        # Multiple replicas - let user select
        echo -e "${GREEN}Available Shared Replica Instances:${NC}"
        echo ""

        local index=1
        echo "$KUZU_INSTANCES_JSON" | jq -r '.[] | select(.[2] == "shared_replica") | @tsv' | while IFS=$'\t' read -r instance_id private_ip node_type repo_type db_count; do
            echo -e "${BLUE}$index)${NC} $(echo $repo_type | tr '[:lower:]' '[:upper:]') Repository Replica"
            echo "   Instance ID: $instance_id"
            echo "   Private IP:  $private_ip"
            echo ""
            ((index++))
        done

        echo -e "${BLUE}Select replica (1-$replica_count): ${NC}"
        read -r selection

        if ! [[ "$selection" =~ ^[0-9]+$ ]] || [ "$selection" -lt 1 ] || [ "$selection" -gt "$replica_count" ]; then
            echo -e "${RED}Invalid selection${NC}"
            exit 1
        fi

        # Get selected replica
        local selected_replica=$(echo "$KUZU_INSTANCES_JSON" | jq -r "[.[] | select(.[2] == \"shared_replica\")][$((selection-1))]")
        local replica_ip=$(echo "$selected_replica" | jq -r '.[1]')
        local instance_id=$(echo "$selected_replica" | jq -r '.[0]')
        local repo_type=$(echo "$selected_replica" | jq -r '.[3]')
    else
        # Single replica
        local replica_ip=$(echo "$replica_instances" | jq -r '.[1]')
        local instance_id=$(echo "$replica_instances" | jq -r '.[0]')
        local repo_type=$(echo "$replica_instances" | jq -r '.[3]')
    fi

    echo -e "${GREEN}Connecting to shared replica instance:${NC}"
    echo "  Instance ID: $instance_id"
    echo "  Repository:  $repo_type"
    echo "  Endpoint:    $replica_ip:8002"
    echo ""
    echo -e "${YELLOW}Setting up SSH tunnel...${NC}"
    echo -e "${BLUE}Shared Replica: localhost:8002 -> $replica_ip:8002${NC}"
    echo ""
    echo -e "${YELLOW}Connection commands:${NC}"
    echo "  curl http://localhost:8002/status"
    echo "  curl http://localhost:8002/health"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to close the tunnel${NC}"
    echo ""

    # Create tunnel on port 8002
    ssh -i "$SSH_KEY" \
        -o "StrictHostKeyChecking=no" \
        -o "UserKnownHostsFile=/dev/null" \
        -N -L 8002:$replica_ip:8002 \
        ec2-user@$BASTION_HOST
}

select_kuzu_instance() {
    if [[ -z "$KUZU_INSTANCES_JSON" || "$KUZU_INSTANCES_JSON" == "[]" ]]; then
        echo -e "${RED}No Kuzu instances available to select${NC}"
        exit 1
    fi

    list_kuzu_instances

    local instance_count=$(echo "$KUZU_INSTANCES_JSON" | jq '. | length')
    echo -e "${BLUE}Select instance (1-$instance_count): ${NC}"
    read -r selection

    if ! [[ "$selection" =~ ^[0-9]+$ ]] || [ "$selection" -lt 1 ] || [ "$selection" -gt "$instance_count" ]; then
        echo -e "${RED}Invalid selection${NC}"
        exit 1
    fi

    # Get selected instance (array is 0-indexed)
    local selected_index=$((selection - 1))
    local selected_instance=$(echo "$KUZU_INSTANCES_JSON" | jq -r ".[$selected_index]")

    KUZU_ENDPOINT=$(echo "$selected_instance" | jq -r '.[1]')
    local instance_id=$(echo "$selected_instance" | jq -r '.[0]')
    local node_type=$(echo "$selected_instance" | jq -r '.[2]')
    local repo_type=$(echo "$selected_instance" | jq -r '.[3]')

    echo ""
    echo -e "${GREEN}Selected instance:${NC}"
    echo "  Instance ID: $instance_id"
    echo "  Endpoint:    $KUZU_ENDPOINT"
    if [[ "$node_type" == "shared_master" ]] || [[ "$node_type" == "shared_replica" ]]; then
        echo "  Repository:  $repo_type"
    fi
    echo ""

    return 0
}

setup_kuzu_select_tunnel() {
    if [[ -z "$KUZU_INSTANCES_JSON" || "$KUZU_INSTANCES_JSON" == "[]" ]]; then
        echo -e "${RED}No Kuzu instances available${NC}"
        echo -e "${YELLOW}Make sure Kuzu writers are deployed and healthy${NC}"
        exit 1
    fi

    select_kuzu_instance
    setup_kuzu_tunnel
}

setup_kuzu_forward() {
    if [[ -z "$KUZU_INSTANCES_JSON" || "$KUZU_INSTANCES_JSON" == "[]" ]]; then
        echo -e "${RED}No Kuzu instances available${NC}"
        echo -e "${YELLOW}Make sure Kuzu writers are deployed and healthy${NC}"
        exit 1
    fi

    echo -e "${GREEN}Setting up port forwarding for Kuzu instances...${NC}"
    echo ""

    # Find available ports starting from 8001
    local port_base=8001
    local tunnel_commands=""
    local instance_info=""

    # Build tunnel commands and display info
    local index=1
    while IFS=$'\t' read -r instance_id private_ip node_type repo_type db_count; do
        local port=$((port_base + index - 1))

        # Check if port is available
        while lsof -i :$port >/dev/null 2>&1; do
            ((port++))
        done

        local instance_name=""
        local remote_port=8001
        if [[ "$node_type" == "shared_master" ]]; then
            instance_name="${repo_type^^} Repository Master"
            remote_port=8002
        elif [[ "$node_type" == "shared_replica" ]]; then
            instance_name="${repo_type^^} Repository Replica"
            remote_port=8002
        else
            instance_name="Entity Writer (${db_count} DBs)"
        fi

        instance_info="${instance_info}${BLUE}Instance $index:${NC} $instance_name\n"
        instance_info="${instance_info}  Instance ID: $instance_id\n"
        instance_info="${instance_info}  Local port:  localhost:$port\n"
        instance_info="${instance_info}  Remote:      $private_ip:$remote_port\n"
        instance_info="${instance_info}  API Docs:    http://localhost:$port/docs\n"
        instance_info="${instance_info}  OpenAPI:     http://localhost:$port/openapi.json\n"
        instance_info="${instance_info}\n"

        tunnel_commands="$tunnel_commands -L $port:$private_ip:$remote_port"
        ((index++))
    done < <(echo "$KUZU_INSTANCES_JSON" | jq -r '.[] | @tsv')

    # Display all instance info
    echo -e "$instance_info"

    echo -e "${YELLOW}Example queries:${NC}"
    echo "  # Query entity database on port 8001"
    echo "  curl -X POST http://localhost:8001/v1/kg1a2b3c/query \\"
    echo "    -H 'Content-Type: application/json' \\"
    echo "    -d '{\"query\": \"MATCH (e:Entity) RETURN e LIMIT 5\"}'"
    echo ""
    echo "  # Query SEC repository on port 8002 (if available)"
    echo "  curl -X POST http://localhost:8002/v1/sec/query \\"
    echo "    -H 'Content-Type: application/json' \\"
    echo "    -d '{\"query\": \"MATCH (e:Entity) WHERE e.ein = '\\''12-3456789'\\'' RETURN e\"}'"
    echo ""

    echo -e "${YELLOW}Starting all tunnels...${NC}"
    echo -e "${YELLOW}Press Ctrl+C to stop all tunnels${NC}"
    echo ""

    # Start SSH with all port forwards
    ssh -i $SSH_KEY -N $tunnel_commands ec2-user@$BASTION_HOST
}

setup_kuzu_select_direct() {
    if [[ -z "$KUZU_INSTANCES_JSON" || "$KUZU_INSTANCES_JSON" == "[]" ]]; then
        echo -e "${RED}No Kuzu instances available${NC}"
        echo -e "${YELLOW}Make sure Kuzu writers are deployed and healthy${NC}"
        exit 1
    fi

    select_kuzu_instance

    # Get instance ID for selected endpoint using jq
    local selected_instance_id=$(echo "$KUZU_INSTANCES_JSON" | jq -r --arg ip "$KUZU_ENDPOINT" '.[] | select(.[1] == $ip) | .[0]')

    if [[ -z "$selected_instance_id" ]]; then
        echo -e "${RED}Could not find instance ID for selected endpoint${NC}"
        exit 1
    fi

    echo -e "${GREEN}Connecting to Kuzu instance $selected_instance_id via AWS Systems Manager...${NC}"
    echo ""

    # Check if instance has SSM agent
    local ssm_instance=$(aws ssm describe-instance-information \
        --filters "Key=InstanceIds,Values=$selected_instance_id" \
        --query 'InstanceInformationList[0].InstanceId' \
        --output text 2>/dev/null)

    if [[ -z "$ssm_instance" || "$ssm_instance" == "None" ]]; then
        echo -e "${YELLOW}SSM not available, falling back to SSH via bastion...${NC}"
        echo ""

        # Fallback to SSH method
        echo -e "${GREEN}Setting up SSH to Kuzu instance via bastion...${NC}"
        echo -e "${BLUE}Bastion: $BASTION_HOST -> Kuzu: $KUZU_ENDPOINT${NC}"
        echo ""

        # Add key to SSH agent
        ssh-add $SSH_KEY 2>/dev/null || {
            echo -e "${RED}Failed to add SSH key to agent. Make sure ssh-agent is running:${NC}"
            echo "eval \$(ssh-agent)"
            echo "ssh-add $SSH_KEY"
            exit 1
        }

        echo -e "${GREEN}✓ SSH key added to agent${NC}"
        echo -e "${BLUE}Connecting to Kuzu instance via bastion with SSH...${NC}"
        echo ""

        # Connect with agent forwarding, then SSH to Kuzu instance
        ssh -A -i $SSH_KEY ec2-user@$BASTION_HOST -t "ssh -o StrictHostKeyChecking=no ec2-user@$KUZU_ENDPOINT"
        return
    fi

    echo -e "${GREEN}✓ Found Kuzu instance: $selected_instance_id${NC}"
    echo -e "${BLUE}Starting Systems Manager session...${NC}"
    echo ""
    echo -e "${YELLOW}You'll have shell access to the Kuzu instance filesystem.${NC}"
    echo -e "${YELLOW}Useful commands:${NC}"
    echo "  docker ps                          # List running containers"
    echo "  docker logs kuzu-writer -f         # View Kuzu logs"
    echo "  curl localhost:8001/health         # Check API health"
    echo "  ls -la /mnt/kuzu-data/databases/   # List databases"
    echo "  df -h /mnt/kuzu-data/              # Check disk usage"
    echo "  du -sh /mnt/kuzu-data/databases/*  # Check database sizes"
    echo ""
    echo -e "${YELLOW}Database file access:${NC}"
    echo "  cd /mnt/kuzu-data/databases/kg1a2b3c45  # Enter database directory"
    echo "  ls -la                                      # List database files"
    echo ""

    # Connect via SSM
    aws ssm start-session --target "$selected_instance_id" --document-name "AWS-StartInteractiveCommand" --parameters "command=sudo -u ec2-user -i bash"
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

run_admin_operation() {
    local environment=$1
    shift  # Remove environment from arguments
    local admin_args="$@"

    echo -e "${GREEN}Running admin operations on $environment environment...${NC}"
    echo ""

    # If no arguments provided, show help
    if [[ -z "$admin_args" ]]; then
        admin_args="help"
    fi

    echo -e "${BLUE}Executing admin operation: $admin_args${NC}"
    echo "----------------------------------------"

    # Execute the admin command via SSH
    ssh -i "$SSH_KEY" \
        -o "StrictHostKeyChecking=no" \
        -o "UserKnownHostsFile=/dev/null" \
        -t ec2-user@$BASTION_HOST \
        "sudo /usr/local/bin/run-admin-operation.sh $admin_args"

    local exit_code=$?

    echo "----------------------------------------"

    if [[ "$exit_code" == "0" ]]; then
        echo -e "${GREEN}✓ Admin operation completed successfully${NC}"
    else
        echo -e "${RED}✗ Admin operation failed with exit code: $exit_code${NC}"
        exit 1
    fi
}

show_admin_help() {
    echo -e "${BLUE}Available Admin Operations:${NC}"
    echo ""
    echo -e "${YELLOW}Credit Management:${NC}"
    echo "  credit-allocate-user USER_ID [--dry-run]"
    echo "  credit-allocate-graph GRAPH_ID [--dry-run]"
    echo "  credit-allocate-all [--dry-run]"
    echo "  credit-bonus GRAPH_ID --amount AMOUNT --description TEXT [--dry-run]"
    echo "  credit-health"
    echo ""
    echo -e "${YELLOW}Repository Access:${NC}"
    echo "  repo-grant USER_ID REPOSITORY ACCESS_LEVEL [--expires-days N]"
    echo "  repo-revoke USER_ID REPOSITORY"
    echo "  repo-list [--repository REPO]"
    echo "  repo-check USER_ID [--repository REPO]"
    echo ""
    echo -e "${YELLOW}Dead Letter Queue:${NC}"
    echo "  dlq-stats"
    echo "  dlq-health"
    echo "  dlq-list [--limit N]"
    echo "  dlq-reprocess TASK_ID"
    echo "  dlq-purge"
    echo ""
    echo -e "${YELLOW}SEC Pipeline:${NC}"
    echo "  sec-reset [--soft]"
    echo "  sec-load --year YEAR --companies N --filings N [--refresh] [--parallel]"
    echo "  sec-collect --year YEAR --companies N --filings N"
    echo "  sec-process --year YEAR [--refresh]"
    echo "  sec-ingest --year YEAR"
    echo ""
    echo -e "${GREEN}Examples:${NC}"
    echo "  # Allocate credits for a user"
    echo "  ./bin/tools/tunnels.sh prod admin credit-allocate-user user_123"
    echo ""
    echo "  # Grant SEC access"
    echo "  ./bin/tools/tunnels.sh prod admin repo-grant user_123 sec admin"
    echo ""
    echo "  # Load SEC data"
    echo "  ./bin/tools/tunnels.sh prod admin sec-load --year 2024 --companies 10 --filings 5"
    echo ""
    echo "  # Check DLQ health"
    echo "  ./bin/tools/tunnels.sh staging admin dlq-health"
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

    if [[ "$KUZU_ENDPOINT" != "NOT_FOUND" ]]; then
        available_services+=("Kuzu")
        tunnel_args="$tunnel_args -L 8001:$KUZU_ENDPOINT:8001"
    fi

    if [[ "$VALKEY_ENDPOINT" != "NOT_FOUND" ]]; then
        available_services+=("Valkey")
        tunnel_args="$tunnel_args -L 6379:$VALKEY_ENDPOINT:6379"
    fi

    if [[ ${#available_services[@]} -eq 0 ]]; then
        echo -e "${RED}Error: No services found to tunnel${NC}"
        exit 1
    fi

    echo -e "${GREEN}Setting up tunnels for: ${available_services[*]}${NC}"

    if [[ "$POSTGRES_ENDPOINT" != "NOT_FOUND" ]]; then
        echo -e "${BLUE}PostgreSQL: localhost:5432 -> $POSTGRES_ENDPOINT:5432${NC}"
    fi

    if [[ "$KUZU_ENDPOINT" != "NOT_FOUND" ]]; then
        local kuzu_instance_count=1
        if [[ -n "$KUZU_INSTANCES_JSON" && "$KUZU_INSTANCES_JSON" != "[]" ]]; then
            kuzu_instance_count=$(echo "$KUZU_INSTANCES_JSON" | jq '. | length')
        fi

        if [[ "$kuzu_instance_count" -gt 1 ]]; then
            echo -e "${BLUE}Kuzu:       localhost:8001 -> $KUZU_ENDPOINT:8001 (1 of $kuzu_instance_count instances)${NC}"
        else
            echo -e "${BLUE}Kuzu:       localhost:8001 -> $KUZU_ENDPOINT:8001${NC}"
        fi
    fi

    if [[ "$VALKEY_ENDPOINT" != "NOT_FOUND" ]]; then
        echo -e "${BLUE}Valkey:     localhost:6379 -> $VALKEY_ENDPOINT:6379${NC}"
    fi

    echo ""
    echo -e "${YELLOW}Connection commands:${NC}"

    if [[ "$POSTGRES_ENDPOINT" != "NOT_FOUND" ]]; then
        echo "PostgreSQL: psql -h localhost -p 5432 -U postgres -d robosystems"
    fi

    if [[ "$KUZU_ENDPOINT" != "NOT_FOUND" ]]; then
        echo "Kuzu:       curl http://localhost:8001/health"
        if [[ -n "$KUZU_INSTANCES_JSON" && "$KUZU_INSTANCES_JSON" != "[]" ]]; then
            local kuzu_instance_count=$(echo "$KUZU_INSTANCES_JSON" | jq '. | length')
            if [[ "$kuzu_instance_count" -gt 1 ]]; then
                echo "            For other instances: ./bin/tunnels.sh $environment kuzu-select"
                echo "            List all instances:  ./bin/tunnels.sh $environment kuzu-list"
            fi
        fi
        echo "            Direct SSH access:   ./bin/tunnels.sh $environment kuzu-direct"
    fi

    if [[ "$VALKEY_ENDPOINT" != "NOT_FOUND" ]]; then
        echo "Valkey:     redis-cli -h localhost -p 6379"
    fi


    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop all tunnels${NC}"
    echo ""

    # Ask user if they want direct access instead of tunnels
    local has_direct_options=false
    if [[ "$KUZU_ENDPOINT" != "NOT_FOUND" ]]; then
        has_direct_options=true

        # Clear any pending input and ensure clean terminal state
        while read -r -t 1 -n 1; do :; done

        # Force output to terminal
        exec < /dev/tty

        echo -e "${BLUE}Direct access options available:${NC}"
        echo "1) Kuzu instance (file system access via AWS SSM)"
        echo "2) Just use tunnels (default)"
        echo ""
        echo -e -n "${BLUE}Choose option (1-2, default=2): ${NC}"

        # Read from terminal directly
        read -r direct_choice < /dev/tty
        echo ""

        case $direct_choice in
            1)
                if [[ "$KUZU_ENDPOINT" != "NOT_FOUND" ]]; then
                    echo -e "${GREEN}Switching to direct Kuzu access...${NC}"
                    # Check if multiple instances exist
                    if [[ -n "$KUZU_INSTANCES_JSON" && "$KUZU_INSTANCES_JSON" != "[]" ]]; then
                        local kuzu_instance_count=$(echo "$KUZU_INSTANCES_JSON" | jq '. | length')
                        if [[ "$kuzu_instance_count" -gt 1 ]]; then
                            setup_kuzu_select_direct
                        else
                            setup_kuzu_direct
                        fi
                    else
                        setup_kuzu_direct
                    fi
                    return
                else
                    echo -e "${YELLOW}Kuzu not available, continuing with tunnels...${NC}"
                fi
                ;;
            *)
                echo -e "${GREEN}Using tunnels...${NC}"
                ;;
        esac
        echo ""
    fi

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
            postgres|kuzu|kuzu-select|kuzu-forward|kuzu-direct|kuzu-list|kuzu-master|kuzu-replica|valkey|migrate|all)
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
        kuzu)
            setup_kuzu_tunnel
            ;;
        kuzu-select)
            setup_kuzu_select_tunnel
            ;;
        kuzu-forward)
            setup_kuzu_forward
            ;;
        kuzu-direct)
            setup_kuzu_direct
            ;;
        kuzu-list)
            list_kuzu_instances
            ;;
        kuzu-master)
            setup_kuzu_master_tunnel
            ;;
        kuzu-replica)
            setup_kuzu_replica_tunnel
            ;;
        valkey)
            setup_valkey_tunnel
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
