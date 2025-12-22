#!/bin/bash
# =============================================================================
# ROBOSYSTEMS SERVICE GITHUB REPOSITORY SETUP SCRIPT
# =============================================================================
#
# This script configures GitHub repository secrets and variables used by CI/CD
# pipelines and deployment automation.
#
# Usage:
#   just setup-gha
#   or directly: bin/setup/gha
#
# Required GitHub repository configuration:
# - Repository secrets (sensitive data)
# - Repository variables (non-sensitive configuration)
#
# =============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_error() {
    echo -e "${RED}❌ $1${NC}" >&2
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

echo "=== RoboSystems GitHub Repository Setup ==="
echo ""


# =============================================================================
# GITHUB SETUP FUNCTIONS
# =============================================================================

function check_prerequisites() {
    print_info "Checking prerequisites..."

    # Check GitHub CLI
    if ! command -v gh >/dev/null 2>&1; then
        print_error "GitHub CLI is not installed. Please install it first."
        echo "   Visit: https://cli.github.com/"
        exit 1
    fi

    # Check GitHub authentication
    if ! gh auth status >/dev/null 2>&1; then
        print_error "GitHub CLI not authenticated."
        echo "   Run: gh auth login"
        exit 1
    fi

    # Check if we're in a git repository
    if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        print_error "Not in a git repository"
        exit 1
    fi

    # Get repository name
    REPO_NAME=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || echo "")
    if [ -z "$REPO_NAME" ]; then
        print_error "Could not determine repository name"
        exit 1
    fi

    print_success "Prerequisites check passed"
    print_info "Repository: $REPO_NAME"
    echo ""
}

function setup_secrets() {
    echo "Setting up GitHub repository secrets..."

    # Set GitHub Repository Secrets
    echo "⚠️  NOTE: Update these commands with your actual secret values before running!"
    echo ""
    echo "📋 Required secrets (CI/CD core functionality):"
    echo "gh secret set AWS_ACCESS_KEY_ID --body \"your_aws_access_key_id\""
    echo "gh secret set AWS_SECRET_ACCESS_KEY --body \"your_aws_secret_access_key\""
    echo "gh secret set AWS_GITHUB_TOKEN --body \"your_github_token_for_aws_operations\""
    echo "gh secret set RUNNER_GITHUB_TOKEN --body \"your_github_token_for_runner\""
    echo ""
    echo "📋 Organization-level secrets (may be inherited, set if not present):"
    echo "gh secret set ACTIONS_TOKEN --body \"your_github_token_for_workflow_automation\""
    echo "gh secret set ANTHROPIC_API_KEY --body \"your_anthropic_api_key_here\""
    echo ""
    echo "💡 ACTIONS_TOKEN enables:"
    echo "   - Cross-workflow automation"
    echo "   - Container registry refresh"
    echo "   - Automated PR creation"
    echo ""
    echo "💡 ANTHROPIC_API_KEY enables:"
    echo "   - Claude-powered PR creation (./bin/tools/create-pr)"
    echo "   - Intelligent code analysis and descriptions"
    echo "   - Enhanced GitHub Actions workflows"
    echo "   - AI-assisted development workflows"
    echo ""
    echo "Secrets setup completed!"
}


function setup_minimum_config() {
    echo "Setting up minimal configuration..."
    echo "💡 Only essential variables required - everything else has sensible defaults!"
    echo ""

    # Absolutely essential variables
    echo "📋 Required variables:"
    while true; do
        read -p "Enter Root Domain (e.g., robosystems.ai): " ROOT_DOMAIN
        # Basic domain validation: must contain at least one dot and valid characters
        if [[ "$ROOT_DOMAIN" =~ ^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)+$ ]]; then
            break
        else
            echo "❌ Invalid domain format. Please enter a valid domain (e.g., example.com)"
        fi
    done

    echo ""
    echo "🔧 Optional variables (press Enter to use defaults):"
    read -p "GitHub Organization Name [YourGitHubOrg]: " GITHUB_ORG
    GITHUB_ORG=${GITHUB_ORG:-"YourGitHubOrg"}
    read -p "Repository Name [robosystems]: " REPO_NAME
    REPO_NAME=${REPO_NAME:-"robosystems"}
    REPOSITORY_NAME="${GITHUB_ORG}/${REPO_NAME}"
    read -p "AWS Account ID [123456789012]: " AWS_ACCOUNT_ID
    AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID:-"123456789012"}
    read -p "Bastion EC2 Key Pair Name [your-key-pair-name]: " BASTION_KEY_PAIR_NAME
    BASTION_KEY_PAIR_NAME=${BASTION_KEY_PAIR_NAME:-"your-key-pair-name"}
    read -p "AWS SNS Alert Email [alerts@example.com]: " AWS_SNS_ALERT_EMAIL
    AWS_SNS_ALERT_EMAIL=${AWS_SNS_ALERT_EMAIL:-"alerts@example.com"}
    read -p "ECR Repository Name [robosystems]: " ECR_REPOSITORY
    ECR_REPOSITORY=${ECR_REPOSITORY:-"robosystems"}
    echo ""
    echo "🔑 SSH Keys (optional):"
    echo "Enter additional SSH public keys for bastion access (one per line, press Ctrl+D when done):"
    echo "Leave empty to skip."
    BASTION_SSH_KEYS=""
    while IFS= read -r line; do
        if [[ -n "$line" ]]; then
            if [[ -n "$BASTION_SSH_KEYS" ]]; then
                BASTION_SSH_KEYS="$BASTION_SSH_KEYS"$'\n'"$line"
            else
                BASTION_SSH_KEYS="$line"
            fi
        fi
    done

    # Set the essential variables
    echo ""
    echo "Setting variables..."

    # Core Infrastructure
    gh variable set API_DOMAIN_NAME_ROOT --body "$ROOT_DOMAIN"
    gh variable set REPOSITORY_NAME --body "$REPOSITORY_NAME"
    gh variable set ECR_REPOSITORY --body "$ECR_REPOSITORY"

    # AWS Configuration (typically org-level, set at repo level for forks)
    gh variable set AWS_ACCOUNT_ID --body "$AWS_ACCOUNT_ID"
    gh variable set AWS_REGION --body "us-east-1"
    gh variable set ENVIRONMENT_PROD --body "prod"
    gh variable set ENVIRONMENT_STAGING --body "staging"

    # Domain Configuration
    gh variable set API_DOMAIN_NAME_PROD --body "api.$ROOT_DOMAIN"
    gh variable set API_DOMAIN_NAME_STAGING --body "staging.api.$ROOT_DOMAIN"

    # Application URLs (typically org-level, set at repo level for forks)
    gh variable set ROBOSYSTEMS_API_URL_PROD --body "https://api.$ROOT_DOMAIN"
    gh variable set ROBOSYSTEMS_API_URL_STAGING --body "https://staging.api.$ROOT_DOMAIN"
    gh variable set ROBOSYSTEMS_APP_URL_PROD --body "https://$ROOT_DOMAIN"
    gh variable set ROBOSYSTEMS_APP_URL_STAGING --body "https://staging.$ROOT_DOMAIN"

    # API Scaling Configuration
    gh variable set API_MIN_CAPACITY_PROD --body "1"
    gh variable set API_MAX_CAPACITY_PROD --body "10"
    gh variable set API_MIN_CAPACITY_STAGING --body "1"
    gh variable set API_MAX_CAPACITY_STAGING --body "2"
    gh variable set API_ASG_REFRESH_PROD --body "true"
    gh variable set API_ASG_REFRESH_STAGING --body "true"

    # Dagster Configuration
    gh variable set DAGSTER_CPU_PROD --body "512"
    gh variable set DAGSTER_CPU_STAGING --body "512"
    gh variable set DAGSTER_MEMORY_PROD --body "1024"
    gh variable set DAGSTER_MEMORY_STAGING --body "1024"
    gh variable set DAGSTER_DAEMON_CPU_PROD --body "512"
    gh variable set DAGSTER_DAEMON_CPU_STAGING --body "512"
    gh variable set DAGSTER_DAEMON_MEMORY_PROD --body "1024"
    gh variable set DAGSTER_DAEMON_MEMORY_STAGING --body "1024"
    gh variable set DAGSTER_RUN_WORKER_INSTANCE_TYPE_PROD --body "r7g.large"
    gh variable set DAGSTER_RUN_WORKER_INSTANCE_TYPE_STAGING --body "r7g.large"
    gh variable set DAGSTER_RUN_WORKER_STORAGE_GB_PROD --body "500"
    gh variable set DAGSTER_RUN_WORKER_STORAGE_GB_STAGING --body "500"
    gh variable set DAGSTER_RUN_WORKER_MIN_CAPACITY_PROD --body "0"
    gh variable set DAGSTER_RUN_WORKER_MIN_CAPACITY_STAGING --body "0"
    gh variable set DAGSTER_RUN_WORKER_MAX_CAPACITY_PROD --body "5"
    gh variable set DAGSTER_RUN_WORKER_MAX_CAPACITY_STAGING --body "2"
    gh variable set DAGSTER_MIN_CAPACITY_PROD --body "1"
    gh variable set DAGSTER_MIN_CAPACITY_STAGING --body "1"
    gh variable set DAGSTER_MAX_CAPACITY_PROD --body "3"
    gh variable set DAGSTER_MAX_CAPACITY_STAGING --body "2"
    gh variable set DAGSTER_ASG_REFRESH_PROD --body "true"
    gh variable set DAGSTER_ASG_REFRESH_STAGING --body "true"
    gh variable set RUN_MIGRATIONS_PROD --body "false"
    gh variable set RUN_MIGRATIONS_STAGING --body "true"

    # Database Configuration
    gh variable set DATABASE_ENGINE_PROD --body "postgres"
    gh variable set DATABASE_ENGINE_STAGING --body "postgres"
    gh variable set DATABASE_INSTANCE_SIZE_PROD --body "db.t4g.micro"
    gh variable set DATABASE_INSTANCE_SIZE_STAGING --body "db.t4g.micro"
    gh variable set DATABASE_ALLOCATED_STORAGE_PROD --body "20"
    gh variable set DATABASE_ALLOCATED_STORAGE_STAGING --body "20"
    gh variable set DATABASE_MAX_ALLOCATED_STORAGE_PROD --body "100"
    gh variable set DATABASE_MAX_ALLOCATED_STORAGE_STAGING --body "100"
    gh variable set DATABASE_MULTI_AZ_ENABLED_PROD --body "false"
    gh variable set DATABASE_MULTI_AZ_ENABLED_STAGING --body "false"
    gh variable set DATABASE_SECRETS_ROTATION_DAYS --body "90"

    # VPC Flow Logs Configuration (SOC 2 - VPC-level, not environment-specific)
    gh variable set VPC_FLOW_LOGS_ENABLED --body "true"
    gh variable set VPC_FLOW_LOGS_RETENTION_DAYS --body "30"
    gh variable set VPC_FLOW_LOGS_TRAFFIC_TYPE --body "REJECT"

    # CloudTrail Configuration (SOC 2 - Account-level, not environment-specific)
    gh variable set CLOUDTRAIL_ENABLED --body "true"
    gh variable set CLOUDTRAIL_LOG_RETENTION_DAYS --body "90"
    gh variable set CLOUDTRAIL_DATA_EVENTS_ENABLED --body "false"

    # Valkey Configuration
    gh variable set VALKEY_NODE_TYPE_PROD --body "cache.t4g.micro"
    gh variable set VALKEY_NODE_TYPE_STAGING --body "cache.t4g.micro"
    gh variable set VALKEY_NUM_NODES_PROD --body "1"
    gh variable set VALKEY_NUM_NODES_STAGING --body "1"
    gh variable set VALKEY_ENCRYPTION_ENABLED_PROD --body "true"
    gh variable set VALKEY_ENCRYPTION_ENABLED_STAGING --body "true"
    gh variable set VALKEY_SECRET_ROTATION_ENABLED_PROD --body "true"
    gh variable set VALKEY_SECRET_ROTATION_ENABLED_STAGING --body "true"
    gh variable set VALKEY_ROTATION_SCHEDULE_DAYS_PROD --body "90"
    gh variable set VALKEY_ROTATION_SCHEDULE_DAYS_STAGING --body "90"
    gh variable set VALKEY_SNAPSHOT_RETENTION_DAYS_PROD --body "7"
    gh variable set VALKEY_SNAPSHOT_RETENTION_DAYS_STAGING --body "0"

    # LadybugDB Writer Configuration - Standard Tier
    gh variable set LBUG_STANDARD_ENABLED_PROD --body "true"
    gh variable set LBUG_STANDARD_ENABLED_STAGING --body "true"
    gh variable set LBUG_STANDARD_MIN_INSTANCES_PROD --body "1"
    gh variable set LBUG_STANDARD_MAX_INSTANCES_PROD --body "10"
    gh variable set LBUG_STANDARD_MIN_INSTANCES_STAGING --body "1"
    gh variable set LBUG_STANDARD_MAX_INSTANCES_STAGING --body "5"

    # LadybugDB Writer Configuration - Large Tier
    gh variable set LBUG_LARGE_ENABLED_PROD --body "false"
    gh variable set LBUG_LARGE_ENABLED_STAGING --body "false"
    gh variable set LBUG_LARGE_MIN_INSTANCES_PROD --body "0"
    gh variable set LBUG_LARGE_MAX_INSTANCES_PROD --body "20"
    gh variable set LBUG_LARGE_MIN_INSTANCES_STAGING --body "0"
    gh variable set LBUG_LARGE_MAX_INSTANCES_STAGING --body "5"

    # LadybugDB Writer Configuration - XLarge Tier
    gh variable set LBUG_XLARGE_ENABLED_PROD --body "false"
    gh variable set LBUG_XLARGE_ENABLED_STAGING --body "false"
    gh variable set LBUG_XLARGE_MIN_INSTANCES_PROD --body "0"
    gh variable set LBUG_XLARGE_MAX_INSTANCES_PROD --body "10"
    gh variable set LBUG_XLARGE_MIN_INSTANCES_STAGING --body "0"
    gh variable set LBUG_XLARGE_MAX_INSTANCES_STAGING --body "5"

    # LadybugDB Writer Configuration - Shared Repository
    gh variable set LBUG_SHARED_ENABLED_PROD --body "false"
    gh variable set LBUG_SHARED_ENABLED_STAGING --body "false"
    gh variable set LBUG_SHARED_MIN_INSTANCES_PROD --body "1"
    gh variable set LBUG_SHARED_MAX_INSTANCES_PROD --body "3"
    gh variable set LBUG_SHARED_MIN_INSTANCES_STAGING --body "1"
    gh variable set LBUG_SHARED_MAX_INSTANCES_STAGING --body "2"

    # Neo4j Writer Configuration (optional backend)
    gh variable set NEO4J_COMMUNITY_LARGE_ENABLED_PROD --body "false"
    gh variable set NEO4J_COMMUNITY_LARGE_ENABLED_STAGING --body "false"
    gh variable set NEO4J_ENTERPRISE_XLARGE_ENABLED_PROD --body "false"
    gh variable set NEO4J_ENTERPRISE_XLARGE_ENABLED_STAGING --body "false"

    # Other Graph Settings
    gh variable set GRAPH_API_KEY_ROTATION_DAYS --body "90"
    gh variable set GRAPH_UPDATE_CONTAINERS_PROD --body "true"
    gh variable set GRAPH_UPDATE_CONTAINERS_STAGING --body "true"

    # GHA Runner Configuration
    gh variable set RUNNER_STORAGE_SIZE --body "20"
    gh variable set RUNNER_MIN_INSTANCES --body "1"
    gh variable set RUNNER_MAX_INSTANCES --body "6"
    gh variable set RUNNER_DESIRED_INSTANCES --body "1"
    gh variable set RUNNER_ENVIRONMENT --body "ci"
    gh variable set RUNNER_GITHUB_ORG --body "$GITHUB_ORG"

    # Notification Configuration
    gh variable set AWS_SNS_ALERT_EMAIL --body "$AWS_SNS_ALERT_EMAIL"

    # Bastion Configuration
    gh variable set BASTION_KEY_PAIR_NAME --body "$BASTION_KEY_PAIR_NAME"

    # Bastion Access Configuration (uses placeholder IP by default - as variable)
    gh secret set BASTION_ALLOWED_CIDR_BLOCK --body "0.0.0.0/32"

    # Features Configuration
    gh variable set OBSERVABILITY_ENABLED_PROD --body "true"
    gh variable set OBSERVABILITY_ENABLED_STAGING --body "true"

    # WAF Configuration (environment-specific)
    gh variable set WAF_ENABLED_PROD --body "true"
    gh variable set WAF_ENABLED_STAGING --body "true"
    gh variable set WAF_RATE_LIMIT_PER_IP --body "10000"
    gh variable set WAF_GEO_BLOCKING_ENABLED --body "false"
    gh variable set WAF_AWS_MANAGED_RULES_ENABLED --body "true"

    # Infrastructure Configuration
    gh variable set MAX_AVAILABILITY_ZONES --body "5"

    # Public Domain Configuration (optional for externalizing XBRL data)
    gh variable set PUBLIC_DOMAIN_NAME_PROD --body "public.$ROOT_DOMAIN"
    gh variable set PUBLIC_DOMAIN_NAME_STAGING --body "public-staging.$ROOT_DOMAIN"

    echo ""
    echo "✅ Minimal configuration completed!"
    echo ""
    echo "📋 Variables set:"
    echo "  🌐 Root Domain: $ROOT_DOMAIN"
    echo "  📦 Repository: $REPOSITORY_NAME"
    echo "  🔑 Bastion Key Pair: $BASTION_KEY_PAIR_NAME"
    echo "  🐳 ECR Repository: $ECR_REPOSITORY"
    echo "  📊 Observability: Enabled for both prod & staging"
    echo "  🛡️ WAF Protection: Ready to enable (currently disabled)"
    echo "  🗃️ Database Multi-AZ: Disabled (for cost optimization)"
    echo "  📁 CloudTrail: Disabled (enable for SOC 2, ~\$5-15/month)"
    echo "  🔍 VPC Flow Logs: Disabled (enable for SOC 2, ~\$10-15/month)"
    echo "  🔐 Graph API Key Rotation: Every 90 days"
    echo "  🔐 PostgreSQL Password Rotation: Every 90 days"
    echo "  🔐 Valkey Encryption: Enabled for both environments"
    echo "  🔐 Valkey Secret Rotation: Every 90 days (both environments)"
    echo "  💾 Valkey Snapshots: 7 days (prod), disabled (staging)"
    echo "  🔄 Database Migrations: Disabled (prod), Enabled (staging)"
    echo ""
    echo "🚀 Your deployment is ready to run!"
    echo "💡 All settings use cost-optimized defaults."
    echo ""
    echo "📋 Still need to set secrets:"
    echo "  Required:"
    echo "    - ACTIONS_TOKEN (GitHub token)"
    echo "    - AWS_ACCESS_KEY_ID (AWS credentials)"
    echo "    - AWS_SECRET_ACCESS_KEY (AWS credentials)"
    echo "  Optional:"
    echo "    - ANTHROPIC_API_KEY (enables AI-powered tools)"
}

function setup_full_config() {
    echo "Setting up full configuration with all currently used variables..."
    echo ""

    # Get user input for key variables
    while true; do
        read -p "Enter Root Domain (e.g., robosystems.ai): " ROOT_DOMAIN
        # Basic domain validation: must contain at least one dot and valid characters
        if [[ "$ROOT_DOMAIN" =~ ^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)+$ ]]; then
            break
        else
            echo "❌ Invalid domain format. Please enter a valid domain (e.g., example.com)"
        fi
    done
    read -p "Enter GitHub Organization Name [YourGitHubOrg]: " GITHUB_ORG
    GITHUB_ORG=${GITHUB_ORG:-"YourGitHubOrg"}
    read -p "Enter Repository Name [robosystems-service]: " REPO_NAME
    REPO_NAME=${REPO_NAME:-"robosystems-service"}
    REPOSITORY_NAME="${GITHUB_ORG}/${REPO_NAME}"
    read -p "Enter AWS Account ID: " AWS_ACCOUNT_ID
    read -p "Enter Bastion EC2 Key Pair Name: " BASTION_KEY_PAIR_NAME
    read -p "Enter AWS SNS Alert Email: " AWS_SNS_ALERT_EMAIL
    read -p "Enter ECR Repository Name [robosystems]: " ECR_REPOSITORY
    ECR_REPOSITORY=${ECR_REPOSITORY:-"robosystems"}

    echo ""
    echo "Setting all variables..."

    # Core Infrastructure
    gh variable set API_DOMAIN_NAME_ROOT --body "$ROOT_DOMAIN"
    gh variable set API_DOMAIN_NAME_PROD --body "api.$ROOT_DOMAIN"
    gh variable set API_DOMAIN_NAME_STAGING --body "staging.api.$ROOT_DOMAIN"
    gh variable set REPOSITORY_NAME --body "$REPOSITORY_NAME"
    gh variable set ECR_REPOSITORY --body "$ECR_REPOSITORY"

    # AWS Configuration (typically org-level, set at repo level for forks)
    gh variable set AWS_ACCOUNT_ID --body "$AWS_ACCOUNT_ID"
    gh variable set AWS_REGION --body "us-east-1"
    gh variable set ENVIRONMENT_PROD --body "prod"
    gh variable set ENVIRONMENT_STAGING --body "staging"

    # Application URLs (typically org-level, set at repo level for forks)
    gh variable set ROBOSYSTEMS_API_URL_PROD --body "https://api.$ROOT_DOMAIN"
    gh variable set ROBOSYSTEMS_API_URL_STAGING --body "https://staging.api.$ROOT_DOMAIN"
    gh variable set ROBOSYSTEMS_APP_URL_PROD --body "https://$ROOT_DOMAIN"
    gh variable set ROBOSYSTEMS_APP_URL_STAGING --body "https://staging.$ROOT_DOMAIN"

    # Bastion Configuration
    gh variable set BASTION_KEY_PAIR_NAME --body "$BASTION_KEY_PAIR_NAME"

    # API Scaling Configuration
    gh variable set API_MIN_CAPACITY_PROD --body "1"
    gh variable set API_MAX_CAPACITY_PROD --body "10"
    gh variable set API_MIN_CAPACITY_STAGING --body "1"
    gh variable set API_MAX_CAPACITY_STAGING --body "2"
    gh variable set API_ASG_REFRESH_PROD --body "true"
    gh variable set API_ASG_REFRESH_STAGING --body "true"

    # Dagster Configuration
    gh variable set DAGSTER_CPU_PROD --body "512"
    gh variable set DAGSTER_CPU_STAGING --body "512"
    gh variable set DAGSTER_MEMORY_PROD --body "1024"
    gh variable set DAGSTER_MEMORY_STAGING --body "1024"
    gh variable set DAGSTER_DAEMON_CPU_PROD --body "512"
    gh variable set DAGSTER_DAEMON_CPU_STAGING --body "512"
    gh variable set DAGSTER_DAEMON_MEMORY_PROD --body "1024"
    gh variable set DAGSTER_DAEMON_MEMORY_STAGING --body "1024"
    gh variable set DAGSTER_RUN_WORKER_INSTANCE_TYPE_PROD --body "r7g.large"
    gh variable set DAGSTER_RUN_WORKER_INSTANCE_TYPE_STAGING --body "r7g.large"
    gh variable set DAGSTER_RUN_WORKER_STORAGE_GB_PROD --body "500"
    gh variable set DAGSTER_RUN_WORKER_STORAGE_GB_STAGING --body "500"
    gh variable set DAGSTER_RUN_WORKER_MIN_CAPACITY_PROD --body "0"
    gh variable set DAGSTER_RUN_WORKER_MIN_CAPACITY_STAGING --body "0"
    gh variable set DAGSTER_RUN_WORKER_MAX_CAPACITY_PROD --body "5"
    gh variable set DAGSTER_RUN_WORKER_MAX_CAPACITY_STAGING --body "2"
    gh variable set DAGSTER_MIN_CAPACITY_PROD --body "1"
    gh variable set DAGSTER_MIN_CAPACITY_STAGING --body "1"
    gh variable set DAGSTER_MAX_CAPACITY_PROD --body "3"
    gh variable set DAGSTER_MAX_CAPACITY_STAGING --body "2"
    gh variable set DAGSTER_ASG_REFRESH_PROD --body "true"
    gh variable set DAGSTER_ASG_REFRESH_STAGING --body "true"
    gh variable set RUN_MIGRATIONS_PROD --body "false"
    gh variable set RUN_MIGRATIONS_STAGING --body "true"

    # Database Configuration
    gh variable set DATABASE_ENGINE_PROD --body "postgres"
    gh variable set DATABASE_ENGINE_STAGING --body "postgres"
    gh variable set DATABASE_INSTANCE_SIZE_PROD --body "db.t4g.micro"
    gh variable set DATABASE_INSTANCE_SIZE_STAGING --body "db.t4g.micro"
    gh variable set DATABASE_ALLOCATED_STORAGE_PROD --body "20"
    gh variable set DATABASE_ALLOCATED_STORAGE_STAGING --body "20"
    gh variable set DATABASE_MAX_ALLOCATED_STORAGE_PROD --body "100"
    gh variable set DATABASE_MAX_ALLOCATED_STORAGE_STAGING --body "100"
    gh variable set DATABASE_MULTI_AZ_ENABLED_PROD --body "false"
    gh variable set DATABASE_MULTI_AZ_ENABLED_STAGING --body "false"
    gh variable set DATABASE_SECRETS_ROTATION_DAYS --body "90"

    # VPC Flow Logs Configuration (SOC 2 - VPC-level, not environment-specific)
    gh variable set VPC_FLOW_LOGS_ENABLED --body "true"
    gh variable set VPC_FLOW_LOGS_RETENTION_DAYS --body "30"
    gh variable set VPC_FLOW_LOGS_TRAFFIC_TYPE --body "REJECT"

    # CloudTrail Configuration (SOC 2 - Account-level, not environment-specific)
    gh variable set CLOUDTRAIL_ENABLED --body "true"
    gh variable set CLOUDTRAIL_LOG_RETENTION_DAYS --body "90"
    gh variable set CLOUDTRAIL_DATA_EVENTS_ENABLED --body "false"

    # Valkey Configuration
    gh variable set VALKEY_NODE_TYPE_PROD --body "cache.t4g.micro"
    gh variable set VALKEY_NODE_TYPE_STAGING --body "cache.t4g.micro"
    gh variable set VALKEY_NUM_NODES_PROD --body "1"
    gh variable set VALKEY_NUM_NODES_STAGING --body "1"
    gh variable set VALKEY_ENCRYPTION_ENABLED_PROD --body "true"
    gh variable set VALKEY_ENCRYPTION_ENABLED_STAGING --body "true"
    gh variable set VALKEY_SECRET_ROTATION_ENABLED_PROD --body "true"
    gh variable set VALKEY_SECRET_ROTATION_ENABLED_STAGING --body "true"
    gh variable set VALKEY_ROTATION_SCHEDULE_DAYS_PROD --body "90"
    gh variable set VALKEY_ROTATION_SCHEDULE_DAYS_STAGING --body "90"
    gh variable set VALKEY_SNAPSHOT_RETENTION_DAYS_PROD --body "7"
    gh variable set VALKEY_SNAPSHOT_RETENTION_DAYS_STAGING --body "0"

    # LadybugDB Writer Configuration - Standard Tier
    gh variable set LBUG_STANDARD_ENABLED_PROD --body "true"
    gh variable set LBUG_STANDARD_ENABLED_STAGING --body "true"
    gh variable set LBUG_STANDARD_MIN_INSTANCES_PROD --body "1"
    gh variable set LBUG_STANDARD_MAX_INSTANCES_PROD --body "10"
    gh variable set LBUG_STANDARD_MIN_INSTANCES_STAGING --body "1"
    gh variable set LBUG_STANDARD_MAX_INSTANCES_STAGING --body "5"

    # LadybugDB Writer Configuration - Large Tier
    gh variable set LBUG_LARGE_ENABLED_PROD --body "false"
    gh variable set LBUG_LARGE_ENABLED_STAGING --body "false"
    gh variable set LBUG_LARGE_MIN_INSTANCES_PROD --body "0"
    gh variable set LBUG_LARGE_MAX_INSTANCES_PROD --body "20"
    gh variable set LBUG_LARGE_MIN_INSTANCES_STAGING --body "0"
    gh variable set LBUG_LARGE_MAX_INSTANCES_STAGING --body "5"

    # LadybugDB Writer Configuration - XLarge Tier
    gh variable set LBUG_XLARGE_ENABLED_PROD --body "false"
    gh variable set LBUG_XLARGE_ENABLED_STAGING --body "false"
    gh variable set LBUG_XLARGE_MIN_INSTANCES_PROD --body "0"
    gh variable set LBUG_XLARGE_MAX_INSTANCES_PROD --body "10"
    gh variable set LBUG_XLARGE_MIN_INSTANCES_STAGING --body "0"
    gh variable set LBUG_XLARGE_MAX_INSTANCES_STAGING --body "5"

    # LadybugDB Writer Configuration - Shared Repository
    gh variable set LBUG_SHARED_ENABLED_PROD --body "true"
    gh variable set LBUG_SHARED_ENABLED_STAGING --body "true"
    gh variable set LBUG_SHARED_MIN_INSTANCES_PROD --body "1"
    gh variable set LBUG_SHARED_MAX_INSTANCES_PROD --body "3"
    gh variable set LBUG_SHARED_MIN_INSTANCES_STAGING --body "1"
    gh variable set LBUG_SHARED_MAX_INSTANCES_STAGING --body "2"

    # Neo4j Writer Configuration (optional backend)
    gh variable set NEO4J_COMMUNITY_LARGE_ENABLED_PROD --body "false"
    gh variable set NEO4J_COMMUNITY_LARGE_ENABLED_STAGING --body "false"
    gh variable set NEO4J_ENTERPRISE_XLARGE_ENABLED_PROD --body "false"
    gh variable set NEO4J_ENTERPRISE_XLARGE_ENABLED_STAGING --body "false"

    # Graph Settings
    gh variable set GRAPH_API_KEY_ROTATION_DAYS --body "90"
    gh variable set GRAPH_UPDATE_CONTAINERS_PROD --body "true"
    gh variable set GRAPH_UPDATE_CONTAINERS_STAGING --body "true"

    # GHA Runner Configuration
    gh variable set RUNNER_STORAGE_SIZE --body "20"
    gh variable set RUNNER_MIN_INSTANCES --body "1"
    gh variable set RUNNER_MAX_INSTANCES --body "6"
    gh variable set RUNNER_DESIRED_INSTANCES --body "1"
    gh variable set RUNNER_ENVIRONMENT --body "ci"
    gh variable set RUNNER_GITHUB_ORG --body "$GITHUB_ORG"

    # Notification Configuration
    gh variable set AWS_SNS_ALERT_EMAIL --body "$AWS_SNS_ALERT_EMAIL"

    # Features Configuration
    gh variable set OBSERVABILITY_ENABLED_PROD --body "true"
    gh variable set OBSERVABILITY_ENABLED_STAGING --body "true"

    # WAF Configuration (environment-specific)
    gh variable set WAF_ENABLED_PROD --body "true"
    gh variable set WAF_ENABLED_STAGING --body "true"
    gh variable set WAF_RATE_LIMIT_PER_IP --body "10000"
    gh variable set WAF_GEO_BLOCKING_ENABLED --body "false"
    gh variable set WAF_AWS_MANAGED_RULES_ENABLED --body "true"

    # Infrastructure Configuration
    gh variable set MAX_AVAILABILITY_ZONES --body "5"

    # Public Domain Configuration (optional for frontend apps)
    gh variable set PUBLIC_DOMAIN_NAME_PROD --body "public.$ROOT_DOMAIN"
    gh variable set PUBLIC_DOMAIN_NAME_STAGING --body "public-staging.$ROOT_DOMAIN"

    # Additional Application URLs (optional, for multi-app ecosystems)
    echo ""
    read -p "Configure RoboLedger app URLs? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -p "RoboLedger domain (e.g., roboledger.ai): " ROBOLEDGER_DOMAIN
        gh variable set ROBOLEDGER_APP_URL_PROD --body "https://$ROBOLEDGER_DOMAIN"
        gh variable set ROBOLEDGER_APP_URL_STAGING --body "https://staging.$ROBOLEDGER_DOMAIN"
    fi

    read -p "Configure RoboInvestor app URLs? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -p "RoboInvestor domain (e.g., roboinvestor.ai): " ROBOINVESTOR_DOMAIN
        gh variable set ROBOINVESTOR_APP_URL_PROD --body "https://$ROBOINVESTOR_DOMAIN"
        gh variable set ROBOINVESTOR_APP_URL_STAGING --body "https://staging.$ROBOINVESTOR_DOMAIN"
    fi

    # Bastion Access Configuration (uses placeholder IP by default - as secret)
    gh secret set BASTION_ALLOWED_CIDR_BLOCK --body "0.0.0.0/32"

    echo ""
    echo "✅ Full configuration completed!"
    echo ""
    echo "📋 Summary of configured variables:"
    echo "  🌐 Domains: api.$ROOT_DOMAIN, staging.api.$ROOT_DOMAIN"
    echo "  📦 Repository: $REPOSITORY_NAME"
    echo "  🐳 ECR: $ECR_REPOSITORY"
    echo "  🔧 Total variables configured: 73"
    echo ""
    echo "All variables have been set to their current production defaults."
}

# =============================================================================
# MAIN SCRIPT EXECUTION
# =============================================================================

function main() {
    check_prerequisites

    echo "This script will configure GitHub repository secrets and variables."
    echo ""

    # Show current repository
    local repo_info=$(gh repo view --json nameWithOwner --jq '.nameWithOwner' 2>/dev/null || echo "Unknown")
    echo "Repository: $repo_info"
    echo ""

    echo "Choose what to set up:"
    echo "1) Variables only - Minimum config (essential variables only)"
    echo "2) Variables only - Full config (all variables)"
    echo "3) Show secret commands (requires manual setup)"
    echo "4) Both variables (minimum) + secret commands"
    echo "5) Both variables (full) + secret commands"
    echo ""
    read -p "Enter your choice (1/2/3/4/5): " -n 1 -r
    echo ""

    case $REPLY in
        1)
            echo "Setting up minimum variables..."
            echo ""
            setup_minimum_config
            ;;
        2)
            echo "Setting up full variables..."
            echo ""
            setup_full_config
            ;;
        3)
            echo "Showing secret setup commands..."
            echo ""
            setup_secrets
            ;;
        4)
            echo "Setting up minimum variables and showing secret commands..."
            echo ""
            setup_minimum_config
            echo ""
            setup_secrets
            ;;
        5)
            echo "Setting up full variables and showing secret commands..."
            echo ""
            setup_full_config
            echo ""
            setup_secrets
            ;;
        *)
            echo "Invalid choice. Exiting."
            exit 1
            ;;
    esac

    echo ""
    echo "✅ GitHub repository setup completed!"
    echo ""
    echo "📋 Next steps:"
    echo "1. If secrets weren't set, run the displayed commands with real values"
    echo "2. Verify variables: gh variable list"
    echo "3. Verify secrets: gh secret list"
    echo "4. Test CI/CD pipeline"
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
