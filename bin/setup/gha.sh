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

function show_optional_secrets() {
    echo "📋 Optional Secrets (not required for deployment):"
    echo ""
    echo "   ACTIONS_TOKEN      - GitHub Personal Access Token (PAT)"
    echo "                        Create at: github.com/settings/tokens"
    echo "                        Required scope: repo (full control)"
    echo ""
    echo "   ANTHROPIC_API_KEY  - Enables AI-powered PR summaries and release notes"
    echo ""
    echo "ACTIONS_TOKEN enables:"
    echo "   - Push to protected branches (create-release.yml)"
    echo "   - Push tags and create GitHub releases (tag-release.yml)"
    echo "   - Org-level self-hosted runner checks (claude.yml)"
    echo ""
    echo "Without ACTIONS_TOKEN, workflows fall back to github.token with limitations:"
    echo "   - May fail on protected branches/tags"
    echo "   - PRs won't trigger on:pull_request workflows"
    echo "   - Runner checks limited to repo-level"
    echo ""
    echo "To set secrets:"
    echo "   gh secret set ACTIONS_TOKEN"
    echo "   gh secret set ANTHROPIC_API_KEY"
    echo ""
    echo "Note: AWS credentials are handled via OIDC (no secrets needed)."
}


function setup_full_config() {
    echo "Setting up full configuration with all currently used variables..."
    echo ""

    # Check if environment choice was passed from bootstrap.sh
    local setup_staging=false
    if [ -n "${SETUP_STAGING:-}" ]; then
        # Use the value from bootstrap.sh
        if [ "$SETUP_STAGING" = "true" ]; then
            setup_staging=true
            echo "Environment: Production + Staging (from bootstrap)"
        else
            echo "Environment: Production only (from bootstrap)"
        fi
    else
        # Ask interactively if not set
        echo "Which environments do you want to configure?"
        echo "  1) Production only (recommended for getting started)"
        echo "  2) Production + Staging (full setup)"
        echo ""
        read -p "Select [1]: " env_choice
        env_choice=${env_choice:-1}

        # Validate input
        if [[ ! "$env_choice" =~ ^[12]$ ]]; then
            print_warning "Invalid choice '$env_choice', defaulting to production only"
            env_choice=1
        fi

        if [ "$env_choice" = "2" ]; then
            setup_staging=true
            echo "Configuring: Production + Staging"
        else
            echo "Configuring: Production only"
        fi
    fi
    echo ""

    # Domain configuration (optional for VPC-only deployments)
    echo "📋 Domain Configuration:"
    echo "   Leave empty for VPC-only deployment (access via SSM tunnel)"
    while true; do
        read -p "Enter Root Domain (e.g., robosystems.ai) or press Enter to skip: " ROOT_DOMAIN
        # Allow empty for VPC-only deployment
        if [ -z "$ROOT_DOMAIN" ]; then
            print_info "No domain configured - API will be accessible via SSM tunnel only"
            break
        fi
        # Basic domain validation: must contain at least one dot and valid characters
        if [[ "$ROOT_DOMAIN" =~ ^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)+$ ]]; then
            break
        else
            echo "❌ Invalid domain format. Please enter a valid domain (e.g., example.com) or press Enter to skip"
        fi
    done
    read -p "Enter GitHub Organization Name [YourGitHubOrg]: " GITHUB_ORG
    GITHUB_ORG=${GITHUB_ORG:-"YourGitHubOrg"}
    read -p "Enter Repository Name [robosystems]: " REPO_NAME
    REPO_NAME=${REPO_NAME:-"robosystems"}
    read -p "Enter AWS Account ID: " AWS_ACCOUNT_ID

    # Check if alert email is already available (from bootstrap or GitHub)
    if [ -n "${ALERT_EMAIL:-}" ]; then
        # Passed from bootstrap.sh
        echo "AWS_SNS_ALERT_EMAIL: $ALERT_EMAIL (from bootstrap)"
        AWS_SNS_ALERT_EMAIL="$ALERT_EMAIL"
    else
        EXISTING_EMAIL=$(gh variable get AWS_SNS_ALERT_EMAIL 2>/dev/null || echo "")
        if [ -n "$EXISTING_EMAIL" ]; then
            echo "AWS_SNS_ALERT_EMAIL already set: $EXISTING_EMAIL"
            AWS_SNS_ALERT_EMAIL="$EXISTING_EMAIL"
        else
            read -p "Enter AWS SNS Alert Email: " AWS_SNS_ALERT_EMAIL
        fi
    fi

    read -p "Enter ECR Repository Name [robosystems]: " ECR_REPOSITORY
    ECR_REPOSITORY=${ECR_REPOSITORY:-"robosystems"}

    echo ""
    echo "Setting all variables..."

    # Core Infrastructure
    gh variable set AWS_ECR_REPOSITORY --body "$ECR_REPOSITORY"

    # AWS Configuration (typically org-level, set at repo level for forks)
    gh variable set AWS_ACCOUNT_ID --body "$AWS_ACCOUNT_ID"
    gh variable set AWS_REGION --body "us-east-1"
    gh variable set ENVIRONMENT_PROD --body "prod"
    if $setup_staging; then
        gh variable set ENVIRONMENT_STAGING --body "staging"
    else
        # Explicitly remove staging variable to prevent accidental deployments
        gh variable delete ENVIRONMENT_STAGING --yes 2>/dev/null || true
    fi

    # API Access Mode & Domain Configuration
    # Modes: 'public' (HTTPS with domain) or 'internal' (SSM tunnel)
    # Workflows default to 'internal' if not set - explicit setting here for visibility
    # API_ACCESS_MODE may be pre-set by bootstrap.sh
    local access_mode="${API_ACCESS_MODE:-}"
    if [ -n "$ROOT_DOMAIN" ]; then
        # Domain provided - must be public mode
        access_mode="public"
        gh variable set API_DOMAIN_NAME_ROOT --body "$ROOT_DOMAIN"
        gh variable set API_DOMAIN_NAME_PROD --body "api.$ROOT_DOMAIN"
        gh variable set ROBOSYSTEMS_API_URL_PROD --body "https://api.$ROOT_DOMAIN"
        gh variable set ROBOSYSTEMS_APP_URL_PROD --body "https://$ROOT_DOMAIN"
        if $setup_staging; then
            gh variable set API_DOMAIN_NAME_STAGING --body "staging.api.$ROOT_DOMAIN"
            gh variable set ROBOSYSTEMS_API_URL_STAGING --body "https://staging.api.$ROOT_DOMAIN"
            gh variable set ROBOSYSTEMS_APP_URL_STAGING --body "https://staging.$ROOT_DOMAIN"
        fi
    elif [ -z "$access_mode" ]; then
        # No domain and no pre-set mode - default to internal
        access_mode="internal"
    fi
    # Set the access mode variable
    gh variable set API_ACCESS_MODE_PROD --body "$access_mode"
    if $setup_staging; then
        gh variable set API_ACCESS_MODE_STAGING --body "$access_mode"
    fi
    # To use public mode (HTTPS with domain): gh variable set API_ACCESS_MODE_PROD --body "public"

    # API Scaling Configuration
    gh variable set API_MIN_CAPACITY_PROD --body "1"
    gh variable set API_MAX_CAPACITY_PROD --body "10"
    if $setup_staging; then
        gh variable set API_MIN_CAPACITY_STAGING --body "1"
        gh variable set API_MAX_CAPACITY_STAGING --body "2"
    fi

    # API Fargate Task Sizing
    gh variable set API_CPU_PROD --body "512"
    gh variable set API_MEMORY_PROD --body "1024"
    if $setup_staging; then
        gh variable set API_CPU_STAGING --body "512"
        gh variable set API_MEMORY_STAGING --body "1024"
    fi

    # API Auto-scaling Targets
    gh variable set API_CPU_TARGET_PROD --body "70"
    gh variable set API_MEMORY_TARGET_PROD --body "80"
    if $setup_staging; then
        gh variable set API_CPU_TARGET_STAGING --body "70"
        gh variable set API_MEMORY_TARGET_STAGING --body "80"
    fi

    # API Capacity Provider Configuration (Spot vs On-Demand)
    # Single spot_weight variable; on-demand weight derived as (100 - spot_weight) in deploy workflow
    gh variable set API_FARGATE_SPOT_WEIGHT_PROD --body "0"
    if $setup_staging; then
        gh variable set API_FARGATE_SPOT_WEIGHT_STAGING --body "90"
    fi

    # Dagster Daemon Configuration
    gh variable set DAGSTER_DAEMON_CPU_PROD --body "1024"
    gh variable set DAGSTER_DAEMON_MEMORY_PROD --body "2048"
    if $setup_staging; then
        gh variable set DAGSTER_DAEMON_CPU_STAGING --body "1024"
        gh variable set DAGSTER_DAEMON_MEMORY_STAGING --body "2048"
    fi

    # Dagster Webserver Configuration
    # DESIRED_COUNT 0 leaves the UI off; bin/tools/tunnels.sh scales it to 1 on
    # demand. Set to 1 to keep it always available.
    gh variable set DAGSTER_WEBSERVER_CPU_PROD --body "512"
    gh variable set DAGSTER_WEBSERVER_MEMORY_PROD --body "1024"
    gh variable set DAGSTER_WEBSERVER_DESIRED_COUNT_PROD --body "1"
    if $setup_staging; then
        gh variable set DAGSTER_WEBSERVER_CPU_STAGING --body "512"
        gh variable set DAGSTER_WEBSERVER_MEMORY_STAGING --body "1024"
        gh variable set DAGSTER_WEBSERVER_DESIRED_COUNT_STAGING --body "1"
    fi

    # Dagster Run Job Configuration (EcsRunLauncher - Fargate)
    # Note: CPU/Memory configured at Dagster job level, not GHA variables
    gh variable set DAGSTER_MAX_CONCURRENT_RUNS_PROD --body "20"
    if $setup_staging; then
        gh variable set DAGSTER_MAX_CONCURRENT_RUNS_STAGING --body "20"
    fi

    # Dagster Capacity Provider Configuration (Spot vs On-Demand)
    # Single spot_weight variable; on-demand weight derived as (100 - spot_weight) in deploy workflow
    gh variable set DAGSTER_DAEMON_FARGATE_SPOT_WEIGHT_PROD --body "0"
    gh variable set DAGSTER_WEBSERVER_FARGATE_SPOT_WEIGHT_PROD --body "0"
    # Base: Minimum tasks guaranteed on On-Demand (set >0 when using Savings Plans)
    gh variable set API_FARGATE_BASE_PROD --body "0"
    gh variable set DAGSTER_DAEMON_FARGATE_BASE_PROD --body "0"
    gh variable set DAGSTER_WEBSERVER_FARGATE_BASE_PROD --body "0"
    if $setup_staging; then
        gh variable set DAGSTER_DAEMON_FARGATE_SPOT_WEIGHT_STAGING --body "80"
        gh variable set DAGSTER_WEBSERVER_FARGATE_SPOT_WEIGHT_STAGING --body "80"
        gh variable set API_FARGATE_BASE_STAGING --body "0"
        gh variable set DAGSTER_DAEMON_FARGATE_BASE_STAGING --body "0"
        gh variable set DAGSTER_WEBSERVER_FARGATE_BASE_STAGING --body "0"
    fi

    # Dagster Monitoring Configuration
    gh variable set DAGSTER_CONTAINER_INSIGHTS_PROD --body "disabled"
    if $setup_staging; then
        gh variable set DAGSTER_CONTAINER_INSIGHTS_STAGING --body "disabled"
    fi

    # Background Worker Configuration
    gh variable set WORKER_ENABLED_PROD --body "false"
    gh variable set WORKER_CPU_PROD --body "512"
    gh variable set WORKER_MEMORY_PROD --body "1024"
    gh variable set WORKER_DESIRED_COUNT_PROD --body "1"
    gh variable set WORKER_MIN_COUNT_PROD --body "1"
    gh variable set WORKER_MAX_COUNT_PROD --body "5"
    gh variable set WORKER_AUTOSCALING_ENABLED_PROD --body "false"
    if $setup_staging; then
        gh variable set WORKER_ENABLED_STAGING --body "false"
        gh variable set WORKER_CPU_STAGING --body "512"
        gh variable set WORKER_MEMORY_STAGING --body "1024"
        gh variable set WORKER_DESIRED_COUNT_STAGING --body "1"
        gh variable set WORKER_MIN_COUNT_STAGING --body "1"
        gh variable set WORKER_MAX_COUNT_STAGING --body "5"
        gh variable set WORKER_AUTOSCALING_ENABLED_STAGING --body "false"
    fi

    # Database Configuration
    # Performance Insights is on in prod only; 7 days is the free retention tier.
    gh variable set DATABASE_INSTANCE_SIZE_PROD --body "db.t4g.small"
    gh variable set DATABASE_ALLOCATED_STORAGE_PROD --body "20"
    gh variable set DATABASE_MAX_ALLOCATED_STORAGE_PROD --body "100"
    gh variable set DATABASE_MULTI_AZ_ENABLED_PROD --body "false"
    gh variable set DATABASE_POSTGRES_VERSION_PROD --body "16"
    gh variable set DATABASE_PI_ENABLED_PROD --body "true"
    gh variable set DATABASE_PI_RETENTION_DAYS_PROD --body "7"
    # RDS Proxy (off by default - flip RDS_PROXY_ENABLED_PROD to "true" to turn on)
    gh variable set RDS_PROXY_ENABLED_PROD --body "false"
    gh variable set RDS_PROXY_MAX_CONNECTIONS_PERCENT_PROD --body "100"
    gh variable set RDS_PROXY_CONNECTION_BORROW_TIMEOUT_PROD --body "120"
    if $setup_staging; then
        gh variable set DATABASE_INSTANCE_SIZE_STAGING --body "db.t4g.small"
        gh variable set DATABASE_ALLOCATED_STORAGE_STAGING --body "20"
        gh variable set DATABASE_MAX_ALLOCATED_STORAGE_STAGING --body "100"
        gh variable set DATABASE_MULTI_AZ_ENABLED_STAGING --body "false"
        gh variable set DATABASE_POSTGRES_VERSION_STAGING --body "16"
        gh variable set DATABASE_PI_ENABLED_STAGING --body "false"
        gh variable set DATABASE_PI_RETENTION_DAYS_STAGING --body "7"
        gh variable set RDS_PROXY_ENABLED_STAGING --body "false"
        gh variable set RDS_PROXY_MAX_CONNECTIONS_PERCENT_STAGING --body "100"
        gh variable set RDS_PROXY_CONNECTION_BORROW_TIMEOUT_STAGING --body "120"
    fi

    # VPC Flow Logs Configuration (SOC 2 - VPC-level, not environment-specific)
    # Disabled by default - enable for compliance requirements
    gh variable set VPC_FLOW_LOGS_ENABLED --body "false"
    gh variable set VPC_FLOW_LOGS_RETENTION_DAYS --body "90"
    gh variable set VPC_FLOW_LOGS_TRAFFIC_TYPE --body "REJECT"

    # CloudTrail Configuration (SOC 2 - Account-level, not environment-specific)
    # Disabled by default - S3 bucket cleanup is painful if you need to tear down
    gh variable set CLOUDTRAIL_ENABLED --body "false"
    gh variable set CLOUDTRAIL_LOG_RETENTION_DAYS --body "90"
    gh variable set CLOUDTRAIL_DATA_EVENTS_ENABLED --body "false"

    # Security Baseline (SOC 2 - Account-level detective controls, not environment-specific)
    # Disabled by default - enables GuardDuty, Security Hub + FSBP, Access Analyzer, Inspector.
    # AWS Config is a separate switch (SECURITY_CONFIG_ENABLED) because its cost scales
    # with resource count. Requires the deploy role grants from bootstrap-oidc (re-run bootstrap).
    gh variable set SECURITY_ENABLED --body "false"
    gh variable set SECURITY_CONFIG_ENABLED --body "false"

    # Secrets Rotation Configuration (monthly automatic rotation via secrets-rotation.yml)
    # Disabled by default - enable after testing in staging
    gh variable set SECRETS_ROTATION_ENABLED_PROD --body "false"
    if $setup_staging; then
        gh variable set SECRETS_ROTATION_ENABLED_STAGING --body "false"
    fi

    # Valkey Configuration
    gh variable set VALKEY_NODE_TYPE_PROD --body "cache.t4g.micro"
    gh variable set VALKEY_NUM_NODES_PROD --body "1"
    gh variable set VALKEY_VERSION_PROD --body "8.1"
    gh variable set VALKEY_ENCRYPTION_ENABLED_PROD --body "true"
    gh variable set VALKEY_SNAPSHOT_RETENTION_DAYS_PROD --body "7"
    if $setup_staging; then
        gh variable set VALKEY_NODE_TYPE_STAGING --body "cache.t4g.micro"
        gh variable set VALKEY_NUM_NODES_STAGING --body "1"
        gh variable set VALKEY_VERSION_STAGING --body "8.1"
        gh variable set VALKEY_ENCRYPTION_ENABLED_STAGING --body "true"
        gh variable set VALKEY_SNAPSHOT_RETENTION_DAYS_STAGING --body "0"
    fi

    # OpenSearch Configuration (text search for filing narratives)
    # Disabled by default - enable when OpenSearch infrastructure is needed
    gh variable set OPENSEARCH_ENABLED_PROD --body "false"
    gh variable set OPENSEARCH_INSTANCE_TYPE_PROD --body "t3.medium.search"
    gh variable set OPENSEARCH_EBS_SIZE_PROD --body "100"
    gh variable set OPENSEARCH_VERSION_PROD --body "2.19"
    # Scaling / HA — minimal single-node install. AZ count rides the workflow default;
    # set the switches (count/zone) to roll out Multi-AZ later (see specs/opensearch-scaling-ha).
    gh variable set OPENSEARCH_INSTANCE_COUNT_PROD --body "1"
    gh variable set OPENSEARCH_ZONE_AWARENESS_PROD --body "false"
    if $setup_staging; then
        gh variable set OPENSEARCH_ENABLED_STAGING --body "false"
        gh variable set OPENSEARCH_INSTANCE_TYPE_STAGING --body "t3.medium.search"
        gh variable set OPENSEARCH_EBS_SIZE_STAGING --body "100"
        gh variable set OPENSEARCH_VERSION_STAGING --body "2.19"
        gh variable set OPENSEARCH_INSTANCE_COUNT_STAGING --body "1"
        gh variable set OPENSEARCH_ZONE_AWARENESS_STAGING --body "false"
    fi

    # LadybugDB Writer Configuration - Standard Tier
    # Note: LBUG_STANDARD_ENABLED is not needed - standard tier is always deployed (always_enabled: true in graph.yml)
    gh variable set LBUG_STANDARD_MIN_INSTANCES_PROD --body "1"
    gh variable set LBUG_STANDARD_MAX_INSTANCES_PROD --body "10"
    if $setup_staging; then
        gh variable set LBUG_STANDARD_MIN_INSTANCES_STAGING --body "1"
        gh variable set LBUG_STANDARD_MAX_INSTANCES_STAGING --body "5"
    fi

    # LadybugDB Writer Configuration - Large Tier
    gh variable set LBUG_LARGE_ENABLED_PROD --body "false"
    gh variable set LBUG_LARGE_MIN_INSTANCES_PROD --body "0"
    gh variable set LBUG_LARGE_MAX_INSTANCES_PROD --body "20"
    if $setup_staging; then
        gh variable set LBUG_LARGE_ENABLED_STAGING --body "false"
        gh variable set LBUG_LARGE_MIN_INSTANCES_STAGING --body "0"
        gh variable set LBUG_LARGE_MAX_INSTANCES_STAGING --body "5"
    fi

    # LadybugDB Writer Configuration - XLarge Tier
    gh variable set LBUG_XLARGE_ENABLED_PROD --body "false"
    gh variable set LBUG_XLARGE_MIN_INSTANCES_PROD --body "0"
    gh variable set LBUG_XLARGE_MAX_INSTANCES_PROD --body "10"
    if $setup_staging; then
        gh variable set LBUG_XLARGE_ENABLED_STAGING --body "false"
        gh variable set LBUG_XLARGE_MIN_INSTANCES_STAGING --body "0"
        gh variable set LBUG_XLARGE_MAX_INSTANCES_STAGING --body "5"
    fi

    # LadybugDB Writer Configuration - Shared Repository (opt-in)
    # Note: Shared master must be exactly 1 instance (single source of truth for snapshots)
    gh variable set LBUG_SHARED_ENABLED_PROD --body "false"
    gh variable set LBUG_SHARED_MIN_INSTANCES_PROD --body "1"
    gh variable set LBUG_SHARED_MAX_INSTANCES_PROD --body "1"
    if $setup_staging; then
        gh variable set LBUG_SHARED_ENABLED_STAGING --body "false"
        gh variable set LBUG_SHARED_MIN_INSTANCES_STAGING --body "1"
        gh variable set LBUG_SHARED_MAX_INSTANCES_STAGING --body "1"
    fi

    # Shared Replicas Configuration - Read-Only Fleet
    # Replicas download shared databases from S3 at startup, then serve queries locally
    # Materialize sec_lbug_s3_published asset in Dagster to upload database, then enable replicas
    gh variable set SHARED_REPLICAS_ENABLED_PROD --body "false"
    gh variable set SHARED_REPLICAS_MIN_INSTANCES_PROD --body "1"
    gh variable set SHARED_REPLICAS_MAX_INSTANCES_PROD --body "3"
    gh variable set SHARED_REPLICAS_DESIRED_CAPACITY_PROD --body "1"
    gh variable set SHARED_REPLICAS_ROOT_VOLUME_SIZE_PROD --body "150"
    gh variable set SHARED_REPLICAS_CPU_TARGET_PROD --body "70"
    gh variable set SHARED_REPLICAS_MEMORY_TARGET_PROD --body "80"
    gh variable set SHARED_REPLICAS_ENABLE_RESPONSE_TIME_SCALING_PROD --body "false"
    gh variable set SHARED_REPLICAS_RESPONSE_TIME_TARGET_PROD --body "5"
    gh variable set SHARED_REPLICAS_INSTANCE_WARMUP_PROD --body "900"
    gh variable set SHARED_REPLICAS_HEALTH_CHECK_GRACE_PERIOD_PROD --body "900"
    gh variable set SHARED_REPOSITORIES_PROD --body "sec"
    # Shared Replicas Spot Configuration (disabled by default for safety)
    gh variable set SHARED_REPLICAS_SPOT_ENABLED_PROD --body "false"
    gh variable set SHARED_REPLICAS_OD_BASE_PROD --body "0"
    gh variable set SHARED_REPLICAS_SPOT_WEIGHT_PROD --body "0"
    if $setup_staging; then
        gh variable set SHARED_REPLICAS_ENABLED_STAGING --body "false"
        gh variable set SHARED_REPLICAS_MIN_INSTANCES_STAGING --body "1"
        gh variable set SHARED_REPLICAS_MAX_INSTANCES_STAGING --body "3"
        gh variable set SHARED_REPLICAS_DESIRED_CAPACITY_STAGING --body "1"
        gh variable set SHARED_REPLICAS_ROOT_VOLUME_SIZE_STAGING --body "150"
        gh variable set SHARED_REPLICAS_CPU_TARGET_STAGING --body "70"
        gh variable set SHARED_REPLICAS_MEMORY_TARGET_STAGING --body "80"
        gh variable set SHARED_REPLICAS_ENABLE_RESPONSE_TIME_SCALING_STAGING --body "false"
        gh variable set SHARED_REPLICAS_RESPONSE_TIME_TARGET_STAGING --body "5"
        gh variable set SHARED_REPLICAS_INSTANCE_WARMUP_STAGING --body "900"
        gh variable set SHARED_REPLICAS_HEALTH_CHECK_GRACE_PERIOD_STAGING --body "900"
        gh variable set SHARED_REPOSITORIES_STAGING --body "sec"
        # Shared Replicas Spot Configuration (disabled by default for safety)
        gh variable set SHARED_REPLICAS_SPOT_ENABLED_STAGING --body "false"
        gh variable set SHARED_REPLICAS_OD_BASE_STAGING --body "0"
        gh variable set SHARED_REPLICAS_SPOT_WEIGHT_STAGING --body "0"
    fi

    # Note: legacy graph backend variables were removed with the Ladybug-only deployment path
    # If another backend is introduced in future, add its *_ENABLED_* variables here

    # Graph AMI: Auto-initialized by get-graph-ami action on first deploy
    # Stored in SSM: /robosystems/{env}/graph/ami-id
    # Updated via: graph-maintenance.yml workflow
    # AUTO_UPDATE: monthly scheduled refresh of the AMI variable (staging 1st, prod 2nd)
    # AUTO_DEPLOY: whether that scheduled run also triggers an ASG refresh
    # Both are read without a fallback in graph-maintenance.yml, so an unset
    # value reads as "not true" and the scheduled work stays off.
    gh variable set GRAPH_AMI_AUTO_UPDATE --body "true"
    gh variable set GRAPH_AMI_AUTO_DEPLOY --body "false"

    # Graph container refresh is now controlled via workflow_dispatch input
    # (graph_container_refresh parameter, defaults to true)

    # GitHub Actions Runner Configuration
    # Default: "github-hosted" uses GitHub-hosted runners (ubuntu-latest)
    # For self-hosted: set RUNNER_LABELS to e.g. "self-hosted,Linux,X64"
    # RUNNER_SCOPE: "repo" (check repo only), "org" (org only), "both" (repo then org)
    gh variable set RUNNER_LABELS --body "github-hosted"
    gh variable set RUNNER_SCOPE --body "both"

    # Notification Configuration
    gh variable set AWS_SNS_ALERT_EMAIL --body "$AWS_SNS_ALERT_EMAIL"

    # Sustained application 5xx over 5 minutes before the API error alarm
    # fires (security stack, both environments). Starts deliberately low —
    # there is no traffic baseline to tune against, and a noisy alarm in a
    # folder checked several times a day is recoverable where a missing one
    # is not. Retune with `just gha-set API_TARGET_ERROR_THRESHOLD <n>` and
    # redeploy the security stack; no code change needed.
    gh variable set API_TARGET_ERROR_THRESHOLD --body "5"

    # Features Configuration
    gh variable set OBSERVABILITY_ENABLED_PROD --body "true"
    if $setup_staging; then
        gh variable set OBSERVABILITY_ENABLED_STAGING --body "true"
    fi

    # WAF Configuration (environment-specific) - disabled by default to save ~$5+/month
    gh variable set WAF_ENABLED_PROD --body "false"
    gh variable set WAF_RATE_LIMIT_PER_IP --body "3000"
    gh variable set WAF_GEO_BLOCKING_ENABLED --body "false"
    gh variable set WAF_AWS_MANAGED_RULES_ENABLED --body "true"
    if $setup_staging; then
        gh variable set WAF_ENABLED_STAGING --body "false"
    fi

    # Audit Trail Retention (environment-specific) - disabled by default
    # Forwards security-audit logs to a long-retention S3 bucket when enabled.
    gh variable set AUDIT_ENABLED_PROD --body "false"
    gh variable set AUDIT_RETENTION_DAYS --body "400"
    if $setup_staging; then
        gh variable set AUDIT_ENABLED_STAGING --body "false"
    fi

    # Infrastructure Configuration
    gh variable set VPC_MAX_AVAILABILITY_ZONES --body "2"
    # VPC Endpoint Mode: 'gateway' (free S3+DynamoDB), 'minimal' (~$22/mo), 'full' (~$45/mo)
    # 'minimal' is recommended - includes ECR endpoints to avoid NAT data transfer costs during deployments
    gh variable set VPC_ENDPOINT_MODE --body "minimal"

    # VPC Configuration - Set to non-zero for VPC peering with other 10.x VPCs
    # Default 0 = 10.0.0.0/16, set to 2 = 10.2.0.0/16 to peer with another 10.0.0.0/16
    gh variable set VPC_SECOND_OCTET --body "0"

    # Public Domain Configuration (optional for frontend apps, skip if no domain)
    if [ -n "$ROOT_DOMAIN" ]; then
        gh variable set PUBLIC_DOMAIN_NAME_PROD --body "public.$ROOT_DOMAIN"
        if $setup_staging; then
            gh variable set PUBLIC_DOMAIN_NAME_STAGING --body "public-staging.$ROOT_DOMAIN"
        fi
    fi

    # Additional Application URLs (optional, for multi-app ecosystems)
    # Only offer if domain is configured
    if [ -n "$ROOT_DOMAIN" ]; then
        echo ""
        read -p "Configure RoboLedger app URLs? (y/N): " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            read -p "RoboLedger domain (e.g., roboledger.ai): " ROBOLEDGER_DOMAIN
            gh variable set ROBOLEDGER_APP_URL_PROD --body "https://$ROBOLEDGER_DOMAIN"
            if $setup_staging; then
                gh variable set ROBOLEDGER_APP_URL_STAGING --body "https://staging.$ROBOLEDGER_DOMAIN"
            fi
        fi

        read -p "Configure RoboInvestor app URLs? (y/N): " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            read -p "RoboInvestor domain (e.g., roboinvestor.ai): " ROBOINVESTOR_DOMAIN
            gh variable set ROBOINVESTOR_APP_URL_PROD --body "https://$ROBOINVESTOR_DOMAIN"
            if $setup_staging; then
                gh variable set ROBOINVESTOR_APP_URL_STAGING --body "https://staging.$ROBOINVESTOR_DOMAIN"
            fi
        fi
    fi

    # Publishing Configuration
    gh variable set DOCKERHUB_PUBLISHING_ENABLED --body "false"

    echo ""
    echo "✅ Full configuration completed!"
    echo ""
    echo "📋 Summary of configured variables:"
    if [ -n "$ROOT_DOMAIN" ]; then
        if $setup_staging; then
            echo "  🌐 Domains: api.$ROOT_DOMAIN, staging.api.$ROOT_DOMAIN"
        else
            echo "  🌐 Domain: api.$ROOT_DOMAIN (prod only)"
        fi
    else
        echo "  🌐 Domain: VPC-only (SSM tunnel access)"
    fi
    echo "  📦 Repository: ${GITHUB_ORG}/${REPO_NAME}"
    echo "  🐳 ECR: $ECR_REPOSITORY"
    if $setup_staging; then
        echo "  🔧 Environments: Production + Staging"
    else
        echo "  🔧 Environment: Production only"
    fi
    echo ""
    echo "All variables have been set to their current defaults."
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

    echo "This sets the full GitHub variable set for control over infrastructure."
    echo "Note: Basic deployments work without this (workflows have sensible defaults)."
    echo ""
    read -p "Continue with full variable setup? (Y/n): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "Cancelled."
        exit 0
    fi
    echo ""

    setup_full_config
    echo ""
    show_optional_secrets

    echo ""
    echo "✅ GitHub repository setup completed!"
    echo ""
    echo "📋 Next steps:"
    echo "   1. Deploy: just deploy staging"
    echo "   2. Verify variables: gh variable list"
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
