#!/bin/bash
# =============================================================================
# ROBOSYSTEMS SERVICE AWS SETUP SCRIPT
# =============================================================================
#
# This script sets up AWS resources for RoboSystems:
# 1. Secrets Manager - Sensitive credentials (API keys, encryption keys)
# 2. SSM Parameter Store - Feature flags and runtime configuration
#
# Usage:
#   just setup-aws
#   or directly: bin/setup/aws
#
# Resources created:
# - Secrets: robosystems/{env} (credentials only)
# - SSM Parameters: /robosystems/{env}/features/* (feature flags)
#
# =============================================================================

set -e

echo "=== RoboSystems AWS Setup ==="
echo ""

# =============================================================================
# CONFIGURATION REFERENCE
# =============================================================================
#
# SECRETS MANAGER (robosystems/{env}):
# - Encryption keys: JWT_SECRET_KEY, CONNECTION_CREDENTIALS_KEY
# - External API keys: INTUIT_*, STRIPE_*, OPENFIGI_*, TURNSTILE_*
# - Internal credentials: SEC_GOV_USER_AGENT
#
# SSM PARAMETER STORE (/robosystems/{env}/features/*):
# - Feature flags: RATE_LIMIT_ENABLED, BILLING_ENABLED, etc.
# - Runtime configuration: ORG_GRAPHS_DEFAULT_LIMIT
#
# Feature flags use SSM for cost efficiency (FREE vs $0.40/secret/month).
# =============================================================================

# =============================================================================
# ENVIRONMENT VARIABLES
# =============================================================================
#
# API_ACCESS_MODE   - API access mode: 'internal' (default) or 'public'
#                     When 'internal', sets JWT_ISSUER/JWT_AUDIENCE to 'localhost'
# SETUP_STAGING     - Set to 'true' to also create staging resources
# SKIP_SECRETS      - Set to 'true' to skip Secrets Manager setup
# SKIP_SSM          - Set to 'true' to skip SSM Parameter Store setup
#
# =============================================================================

# =============================================================================
# AWS SECRETS MANAGER SETUP FUNCTIONS
# =============================================================================

function generate_secret_key() {
    # Generate a secure random key using openssl
    openssl rand -base64 32
}

function create_production_secret() {
    echo "Checking production secret..."

    # Check if secret already exists - don't overwrite!
    if aws secretsmanager describe-secret --secret-id "robosystems/prod" >/dev/null 2>&1; then
        echo "  Production secret already exists - skipping (won't overwrite)"
        return 0
    fi

    echo "Creating production secret..."

    # Create Production Secret
    aws secretsmanager create-secret \
        --name "robosystems/prod" \
        --description "RoboSystems production environment secrets (credentials only)" \
        --tags Key=Environment,Value=prod Key=Service,Value=RoboSystems Key=Component,Value=Secrets

    echo "Generating secure keys..."
    PROD_JWT_SECRET=$(generate_secret_key)
    PROD_CONNECTION_KEY=$(generate_secret_key)

    echo "Setting production secret values..."

    # Build JWT entries for internal mode (localhost access via SSM tunnel)
    local jwt_entries=""
    if [ "${API_ACCESS_MODE:-internal}" = "internal" ]; then
        jwt_entries="\"JWT_ISSUER\": \"localhost\", \"JWT_AUDIENCE\": \"localhost\","
        echo "Including JWT config for internal mode (localhost)"
    fi

    # Set Production Secret Values
    # NOTE: Feature flags have been moved to SSM Parameter Store
    aws secretsmanager put-secret-value \
        --secret-id "robosystems/prod" \
        --secret-string '{
        '"${jwt_entries}"'

        "CONNECTION_CREDENTIALS_KEY": "'"$PROD_CONNECTION_KEY"'",
        "JWT_SECRET_KEY": "'"$PROD_JWT_SECRET"'",
        "TURNSTILE_SECRET_KEY": "your_cloudflare_turnstile_secret_key",
        "TURNSTILE_SITE_KEY": "your_cloudflare_turnstile_site_key",

        "INTUIT_CLIENT_ID": "Intuit.ipp.application.your_client_id",
        "INTUIT_CLIENT_SECRET": "your_quickbooks_client_secret_here",
        "INTUIT_ENVIRONMENT": "production",
        "INTUIT_REDIRECT_URI": "https://your-api-domain.example.com/auth/callback",
        "SEC_GOV_USER_AGENT": "YourCompany/1.0 (your-email@example.com)",
        "OPENFIGI_API_KEY": "your_openfigi_api_key_here",
        "STRIPE_SECRET_KEY": "sk_live_your_stripe_secret_key_here",
        "STRIPE_PUBLISHABLE_KEY": "pk_live_your_stripe_publishable_key_here",
        "STRIPE_WEBHOOK_SECRET": "whsec_your_stripe_webhook_secret_here"
    }'

    echo "  Production secret created successfully!"
}

function create_staging_secret() {
    echo "Checking staging secret..."

    # Check if secret already exists - don't overwrite!
    if aws secretsmanager describe-secret --secret-id "robosystems/staging" >/dev/null 2>&1; then
        echo "  Staging secret already exists - skipping (won't overwrite)"
        return 0
    fi

    echo "Creating staging secret..."

    # Create Staging Secret
    aws secretsmanager create-secret \
        --name "robosystems/staging" \
        --description "RoboSystems staging environment secrets (credentials only)" \
        --tags Key=Environment,Value=staging Key=Service,Value=RoboSystems Key=Component,Value=Secrets

    echo "Generating secure keys..."
    STAGING_JWT_SECRET=$(generate_secret_key)
    STAGING_CONNECTION_KEY=$(generate_secret_key)

    echo "Setting staging secret values..."

    # Build JWT entries for internal mode (localhost access via SSM tunnel)
    local jwt_entries=""
    if [ "${API_ACCESS_MODE:-internal}" = "internal" ]; then
        jwt_entries="\"JWT_ISSUER\": \"localhost\", \"JWT_AUDIENCE\": \"localhost\","
        echo "Including JWT config for internal mode (localhost)"
    fi

    # Set Staging Secret Values
    # NOTE: Feature flags have been moved to SSM Parameter Store
    aws secretsmanager put-secret-value \
        --secret-id "robosystems/staging" \
        --secret-string '{
        '"${jwt_entries}"'

        "CONNECTION_CREDENTIALS_KEY": "'"$STAGING_CONNECTION_KEY"'",
        "JWT_SECRET_KEY": "'"$STAGING_JWT_SECRET"'",
        "TURNSTILE_SECRET_KEY": "your_cloudflare_turnstile_secret_key",
        "TURNSTILE_SITE_KEY": "your_cloudflare_turnstile_site_key",

        "INTUIT_CLIENT_ID": "Intuit.ipp.application.your_sandbox_client_id",
        "INTUIT_CLIENT_SECRET": "your_quickbooks_sandbox_client_secret_here",
        "INTUIT_ENVIRONMENT": "sandbox",
        "INTUIT_REDIRECT_URI": "https://your-staging-api-domain.example.com/auth/callback",
        "SEC_GOV_USER_AGENT": "YourCompany-Staging/1.0 (your-email@example.com)",
        "OPENFIGI_API_KEY": "your_openfigi_api_key_here",
        "STRIPE_SECRET_KEY": "sk_test_your_stripe_test_secret_key_here",
        "STRIPE_PUBLISHABLE_KEY": "pk_test_your_stripe_test_publishable_key_here",
        "STRIPE_WEBHOOK_SECRET": "whsec_your_stripe_test_webhook_secret_here"
    }'

    echo "  Staging secret created successfully!"
}

# =============================================================================
# SSM PARAMETER STORE SETUP FUNCTIONS
# =============================================================================

function create_ssm_feature_flags() {
    local env="$1"
    local prefix="/robosystems/${env}/features"

    echo "Creating SSM feature flags for ${env}..."

    # USER_REGISTRATION_ENABLED differs between staging (true) and prod (false)
    local user_reg="false"
    if [ "$env" = "staging" ]; then
        user_reg="true"
    fi

    # Define all feature flag parameters
    # Format: KEY=VALUE (all other flags are same for staging/prod)
    local params=(
        "BACKUP_CREATION_ENABLED=true"
        "BILLING_ENABLED=false"
        "CAPTCHA_ENABLED=false"
        "CONNECTIONS_ENABLED=false"

        "CONNECTION_QUICKBOOKS_ENABLED=false"
        "SEC_PIPELINE_ENABLED=false"
        "DIRECT_GRAPH_MATERIALIZATION_ENABLED=true"
        "EMAIL_VERIFICATION_ENABLED=false"
        "FACT_GRID_ENABLED=false"
        # Extensions surfaces are gated per product domain. There is no
        # settable EXTENSIONS_ENABLED: it is a *derived* property in
        # config/env.py (ROBOLEDGER_ENABLED OR ROBOINVESTOR_ENABLED), so
        # seeding that name — or the retired LEDGER_ENABLED / INVESTOR_ENABLED
        # — produces a parameter nothing reads, and an operator who sets it to
        # false gets no effect while extensions stay on. Seed the three names
        # the code actually reads. Code defaults are true; these start false so
        # a fresh environment opts in per domain.
        "ROBOLEDGER_ENABLED=false"
        "ROBOINVESTOR_ENABLED=false"
        "EXTENSIONS_GRAPHQL_ENABLED=false"
        # Kill switch for graph_usage_monitor_sensor's 80%/100% storage emails.
        # Seeded at its code default purely so it is discoverable in
        # `just ssm-list <env> features` — a kill switch you cannot find is not one.
        "GRAPH_USAGE_ALERTS_ENABLED=true"
        # Identity and access controls. Seeded at their code defaults so an
        # operator can find them — these are the switches you reach for during
        # an incident, and one you cannot see in `just ssm-list` is not a
        # switch. Changing a value here changes nothing until an environment
        # is bootstrapped; the code default already governs today.
        "PASSWORD_AUTH_ENABLED=true"
        "PASSKEYS_ENABLED=false"
        "MFA_ENFORCEMENT_ENABLED=false"
        "SSO_OIDC_ENABLED=false"
        "SCIM_ENABLED=false"
        # Kill switch for the daily storage-reclaim sweep, which deletes data.
        # Read per run rather than at import, so flipping it takes effect on
        # the next run.
        "STORAGE_RECLAIM_ENABLED=true"
        "CONNECTION_EXTERNAL_ENABLED=true"
        "SEMANTIC_MEMORY_ENABLED=false"
        "MCP_GRAPHQL_ENABLED=true"
        "OPERATOR_POST_ENABLED=true"
        "EXTENSIONS_PROMOTION_AUTO_DISPATCH=false"
        "INTUIT_REPORTS_TESTING_MIGRATION=true"
        "LOAD_SHEDDING_ENABLED=true"
        "MCP_AUTO_LIMIT_ENABLED=true"
        "MCP_SEMANTIC_MEMORY_ENABLED=false"
        "MCP_SUBGRAPH_OPS_ENABLED=false"
        "MCP_WORKSPACE_ENABLED=false"
        "ORG_MEMBER_INVITATIONS_ENABLED=false"
        "OTEL_ENABLED=false"
        "OTEL_TRACES_ENABLED=false"
        "RATE_LIMIT_ENABLED=false"
        "SECURITY_AUDIT_ENABLED=false"
        "SHARED_MASTER_READS_ENABLED=true"
        "SHARED_MASTER_PARKING_ENABLED=true"
        "SEMANTIC_SEARCH_ENABLED=false"
        "SUBGRAPH_CREATION_ENABLED=true"
        "TAXONOMY_AUTHORING_ENABLED=false"
        "USER_REGISTRATION_ENABLED=${user_reg}"
    )

    local created=0
    local skipped=0

    for param in "${params[@]}"; do
        local key="${param%%=*}"
        local value="${param#*=}"
        local param_name="${prefix}/${key}"

        # Check if parameter already exists
        if aws ssm get-parameter --name "$param_name" >/dev/null 2>&1; then
            ((skipped++))
            continue
        fi

        # Create the parameter
        aws ssm put-parameter \
            --name "$param_name" \
            --value "$value" \
            --type String \
            --description "Feature flag: ${key}" \
            --tags "Key=Environment,Value=${env}" "Key=Service,Value=RoboSystems" "Key=Component,Value=FeatureFlags" \
            >/dev/null 2>&1

        ((created++))
    done

    echo "  Created: ${created}, Skipped (existing): ${skipped}"
}

function update_ssm_feature_flag() {
    local env="$1"
    local key="$2"
    local value="$3"
    local param_name="/robosystems/${env}/features/${key}"

    aws ssm put-parameter \
        --name "$param_name" \
        --value "$value" \
        --type String \
        --overwrite \
        --description "Feature flag: ${key}" \
        >/dev/null 2>&1

    echo "  Updated: ${param_name} = ${value}"
}

# =============================================================================
# SSM TUNING PARAMETERS SETUP FUNCTIONS
# =============================================================================
# Tuning parameters are runtime-adjustable operational values.
# These differ from feature flags (booleans) and secrets (credentials).
#
# Categories:
#   cache/       - Cache TTL values
#   admission/   - Admission control thresholds
#   database/    - Connection pool sizing
#   queues/      - Queue configuration
#   circuits/    - Circuit breaker settings
#   load_shedding/ - Load shedding thresholds
#   mcp/         - MCP operation limits
#   workers/     - Worker pool settings

function create_ssm_tuning_parameters() {
    local env="$1"
    local prefix="/robosystems/${env}/tuning"

    echo "Creating SSM tuning parameters for ${env}..."

    # Define all tuning parameters with their default values
    # Format: PATH=VALUE
    local params=(
        # Cache TTLs (seconds)
        "cache/BALANCE_TTL=300"
        "cache/SUMMARY_TTL=600"
        "cache/JWT_TTL=1800"
        "cache/API_KEY_TTL=300"
        "cache/SCHEMA_TTL=300"
        "cache/OPERATION_COST_TTL=3600"

        # Admission Control - Main API (all percentages 0-100)
        "admission/MEMORY_THRESHOLD=85.0"
        "admission/CPU_THRESHOLD=90.0"
        "admission/QUEUE_THRESHOLD=80.0"

        # Admission Control - Graph API / LadybugDB (percentages 0-100)
        "lbug_admission/MEMORY_THRESHOLD=85.0"
        "lbug_admission/CPU_THRESHOLD=90.0"

        # Queue Configuration
        "queues/MAX_SIZE=1000"
        "queues/MAX_CONCURRENT=50"
        "queues/MAX_PER_USER=10"
        "queues/TIMEOUT=300"

        # Circuit Breakers
        "circuits/THRESHOLD=5"
        "circuits/TIMEOUT=60"

        # Load Shedding (all percentages 0-100)
        "load_shedding/START_PRESSURE=80.0"
        "load_shedding/STOP_PRESSURE=60.0"

        # MCP Operation Limits
        "mcp/MAX_RESULT_ROWS=1000"
        "mcp/MAX_RESULT_SIZE_MB=5.0"
        "mcp/POOL_IDLE_TIMEOUT=300"
        "mcp/POOL_MAX_LIFETIME=3600"

        # Worker Configuration
        "workers/MAX_WORKERS=10"

        # Timeout Configuration
        "timeouts/GRAPH_HTTP=30"
        "timeouts/GRAPH_QUERY=30"

        # SSE Configuration
        "sse/MAX_CONNECTIONS_PER_USER=5"
        "sse/QUEUE_SIZE=100"

        # Limits
        "limits/ORG_GRAPHS_DEFAULT=10"

        # Database Connection Pool
        "database/POOL_SIZE=5"
        "database/MAX_OVERFLOW=10"
        "database/POOL_TIMEOUT=30"
        "database/POOL_RECYCLE=3600"
    )

    local created=0
    local skipped=0

    for param in "${params[@]}"; do
        local path="${param%%=*}"
        local value="${param#*=}"
        local param_name="${prefix}/${path}"

        # Check if parameter already exists
        if aws ssm get-parameter --name "$param_name" >/dev/null 2>&1; then
            ((skipped++))
            continue
        fi

        # Create the parameter
        aws ssm put-parameter \
            --name "$param_name" \
            --value "$value" \
            --type String \
            --description "Tuning parameter: ${path}" \
            --tags "Key=Environment,Value=${env}" "Key=Service,Value=RoboSystems" "Key=Component,Value=Tuning" \
            >/dev/null 2>&1

        ((created++))
    done

    echo "  Created: ${created}, Skipped (existing): ${skipped}"
}

function update_ssm_tuning_parameter() {
    local env="$1"
    local path="$2"
    local value="$3"
    local param_name="/robosystems/${env}/tuning/${path}"

    aws ssm put-parameter \
        --name "$param_name" \
        --value "$value" \
        --type String \
        --overwrite \
        --description "Tuning parameter: ${path}" \
        >/dev/null 2>&1

    echo "  Updated: ${param_name} = ${value}"
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

function check_prerequisites() {
    echo "Checking prerequisites..."

    # Check AWS CLI
    if ! command -v aws >/dev/null 2>&1; then
        echo "AWS CLI is not installed. Please install it first."
        echo "   Visit: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html"
        exit 1
    fi

    # Check AWS credentials
    if ! aws sts get-caller-identity >/dev/null 2>&1; then
        echo "AWS credentials not configured or invalid."
        echo "   Run: aws configure"
        exit 1
    fi

    echo "Prerequisites check passed"
    echo ""
}

# =============================================================================
# MAIN SCRIPT EXECUTION
# =============================================================================

function main() {
    check_prerequisites

    echo "This script sets up AWS resources for RoboSystems:"
    echo "  - Secrets Manager: Credentials (API keys, encryption keys)"
    echo "  - SSM Parameter Store: Feature flags (FREE tier)"
    echo ""
    echo "Safe to run multiple times - existing resources are NOT overwritten."
    echo ""

    # Show current AWS identity
    local aws_identity=$(aws sts get-caller-identity --query 'Account' --output text 2>/dev/null)
    echo "AWS Account: $aws_identity"
    echo ""

    # Determine environment configuration
    local setup_staging=false
    if [ -n "${SETUP_STAGING:-}" ]; then
        if [ "$SETUP_STAGING" = "true" ]; then
            setup_staging=true
            echo "Environment: Production + Staging (from bootstrap)"
        else
            echo "Environment: Production only (from bootstrap)"
        fi
    else
        echo "Which environments do you want to set up?"
        echo "  1) Production only (recommended for getting started)"
        echo "  2) Production + Staging (full setup)"
        echo ""
        read -p "Select [1]: " env_choice
        env_choice=${env_choice:-1}

        if [[ ! "$env_choice" =~ ^[12]$ ]]; then
            echo "Invalid choice '$env_choice', defaulting to production only"
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

    # Determine what to set up
    local run_secrets=true
    local run_ssm=true

    if [ "${SKIP_SECRETS:-}" = "true" ]; then
        run_secrets=false
    fi
    if [ "${SKIP_SSM:-}" = "true" ]; then
        run_ssm=false
    fi

    # Interactive mode if not skipped via env vars
    if [ -z "${SKIP_SECRETS:-}" ] && [ -z "${SKIP_SSM:-}" ]; then
        echo "What would you like to set up?"
        echo "  1) Both Secrets Manager and SSM Parameters (recommended)"
        echo "  2) Secrets Manager only (credentials)"
        echo "  3) SSM Parameters only (feature flags)"
        echo ""
        read -p "Select [1]: " setup_choice
        setup_choice=${setup_choice:-1}

        case "$setup_choice" in
            2)
                run_ssm=false
                ;;
            3)
                run_secrets=false
                ;;
            *)
                # Default: both
                ;;
        esac
    fi

    echo ""
    echo "Setup plan:"
    if $run_secrets; then
        echo "  - Secrets Manager: Production$(if $setup_staging; then echo " + Staging"; fi)"
    fi
    if $run_ssm; then
        echo "  - SSM Parameters: Production$(if $setup_staging; then echo " + Staging"; fi)"
    fi
    echo ""

    read -p "Continue? (Y/n): " -n 1 -r
    echo ""

    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "Cancelled."
        exit 0
    fi

    echo ""

    # Create Secrets Manager secrets
    if $run_secrets; then
        echo "=== Setting up Secrets Manager ==="
        create_production_secret
        if $setup_staging; then
            create_staging_secret
        fi
        echo ""
    fi

    # Create SSM Parameter Store feature flags
    if $run_ssm; then
        echo "=== Setting up SSM Parameter Store (Feature Flags) ==="
        create_ssm_feature_flags "prod"
        if $setup_staging; then
            create_ssm_feature_flags "staging"
        fi
        echo ""

        echo "=== Setting up SSM Parameter Store (Tuning Parameters) ==="
        create_ssm_tuning_parameters "prod"
        if $setup_staging; then
            create_ssm_tuning_parameters "staging"
        fi
        echo ""
    fi

    echo "AWS setup completed!"
    echo ""
    echo "Next steps:"
    if $run_secrets; then
        echo "  - Update credentials: aws secretsmanager get-secret-value --secret-id robosystems/prod"
    fi
    if $run_ssm; then
        echo "  - List feature flags: just ssm-list prod features"
        echo "  - List tuning params: just ssm-list prod tuning"
        echo "  - Update a flag: just ssm-set prod features/RATE_LIMIT_ENABLED false"
        echo "  - Update tuning: just ssm-set prod tuning/cache/BALANCE_TTL 600"
    fi
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
