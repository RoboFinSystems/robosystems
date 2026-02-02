#!/bin/bash
# =============================================================================
# ROBOSYSTEMS BOOTSTRAP SCRIPT
# =============================================================================
#
# Bootstrap using AWS SSO + GitHub OIDC Federation.
# No long-term AWS credentials stored anywhere.
#
# PREREQUISITES:
#   - AWS CLI v2 installed
#   - AWS IAM Identity Center (SSO) enabled with admin access
#   - GitHub CLI installed and authenticated
#
# WHAT THIS DOES:
#   1. Configures AWS CLI SSO profile (if not exists)
#   2. Deploys GitHub OIDC CloudFormation stack
#   3. Sets GitHub variables (role ARN, etc.)
#   4. Creates AWS Secrets Manager secrets
#
# USAGE:
#   just bootstrap [profile] [region]
#   or: bin/setup/bootstrap.sh [profile] [region]
#
# ARGUMENTS:
#   profile: AWS SSO profile name (default: robosystems-sso)
#   region:  AWS region (default: us-east-1)
#
# EXAMPLES:
#   just bootstrap                           # Use defaults
#   just bootstrap my-fork-sso               # Custom profile
#   just bootstrap my-fork-sso eu-west-1     # Custom profile and region
#
# =============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Parse arguments - these take priority over environment variables
SSO_PROFILE="${1:-${AWS_PROFILE:-robosystems-sso}}"
AWS_REGION="${2:-${AWS_REGION:-us-east-1}}"
OIDC_STACK_NAME="RoboSystemsGitHubOIDC"

# Export immediately so all AWS CLI calls in this script use the correct profile
# This ensures bootstrap works even if .envrc isn't activated yet
export AWS_PROFILE="$SSO_PROFILE"
export AWS_REGION="$AWS_REGION"

# Track if we need to remind user to activate .envrc
ENVRC_NEEDS_ACTIVATION=false

print_header() {
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_step() {
    echo -e "${BLUE}▶ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}" >&2
}

print_info() {
    echo -e "  $1"
}

# =============================================================================
# DIRENV SETUP
# =============================================================================

setup_direnv() {
    print_header "Setting up direnv"

    local target_file=".envrc"
    local expected_profile="$SSO_PROFILE"
    local expected_region="$AWS_REGION"

    print_info "Using profile: ${expected_profile}"
    print_info "Using region:  ${expected_region}"
    echo ""

    if [ -f "$target_file" ]; then
        # Check if existing .envrc has the expected profile
        local current_profile
        current_profile=$(grep -E "^export AWS_PROFILE=" "$target_file" 2>/dev/null | cut -d'=' -f2 | tr -d '"' | tr -d "'" || echo "")

        if [ "$current_profile" = "$expected_profile" ]; then
            print_success "Existing .envrc already configured for profile '${expected_profile}'"

            # Check if region is set, update if missing
            if ! grep -q "^export AWS_REGION=" "$target_file" 2>/dev/null; then
                # Ensure file ends with newline before appending
                [[ -s "$target_file" && $(tail -c1 "$target_file") != $'\n' ]] && echo "" >> "$target_file"
                echo "export AWS_REGION=${expected_region}" >> "$target_file"
                print_info "Added AWS_REGION to existing .envrc"
                ENVRC_NEEDS_ACTIVATION=true
            fi
            return 0
        else
            # Different profile - ask user what to do
            print_warning "Existing .envrc uses different profile: '${current_profile}'"
            echo ""
            cat "$target_file"
            echo ""
            read -p "Update to profile '${expected_profile}'? (Y/n): " -n 1 -r
            echo ""
            if [[ $REPLY =~ ^[Nn]$ ]]; then
                print_error "Cannot continue with mismatched profile"
                print_info "Either update .envrc manually or run: just bootstrap ${current_profile}"
                exit 1
            fi
        fi
    fi

    # Generate .envrc with the configured profile and region
    cat > "$target_file" << EOF
# Automatically set AWS profile and region for this project
export AWS_PROFILE=${expected_profile}
export AWS_REGION=${expected_region}
EOF

    print_success "Created .envrc with AWS_PROFILE=${expected_profile} and AWS_REGION=${expected_region}"
    ENVRC_NEEDS_ACTIVATION=true

    if command -v direnv &>/dev/null; then
        # Try to auto-allow if possible
        if direnv allow . 2>/dev/null; then
            print_success "Activated .envrc with direnv"
            ENVRC_NEEDS_ACTIVATION=false
        else
            print_info "Run 'direnv allow' to activate for future sessions"
        fi
    else
        print_warning "direnv not installed - .envrc created but won't auto-load"
        print_info "Install with: brew install direnv"
        print_info "Or run: source .envrc"
    fi
}

# =============================================================================
# SSO CONFIGURATION
# =============================================================================

check_sso_configured() {
    print_header "Checking SSO Configuration"

    # Check if SSO profile exists in config
    if aws configure list-profiles 2>/dev/null | grep -q "^${SSO_PROFILE}$"; then
        print_success "SSO profile '${SSO_PROFILE}' found"
        return 0
    else
        print_warning "SSO profile '${SSO_PROFILE}' not found"
        return 1
    fi
}

configure_sso() {
    print_header "Configure AWS SSO"

    echo "Let's set up AWS CLI SSO access."
    echo ""
    echo "You'll need:"
    echo "  - Your SSO start URL (e.g., https://d-xxxxxxxxxx.awsapps.com/start)"
    echo "  - Your SSO region (usually us-east-1)"
    echo ""

    read -p "SSO Start URL: " SSO_START_URL
    read -p "SSO Region [us-east-1]: " SSO_REGION
    SSO_REGION=${SSO_REGION:-us-east-1}

    echo ""
    print_step "Creating SSO profile '${SSO_PROFILE}'..."

    # Create the config file entry
    mkdir -p ~/.aws

    # Check if profile already exists in config
    if grep -q "\[profile ${SSO_PROFILE}\]" ~/.aws/config 2>/dev/null; then
        print_warning "Profile already exists, updating..."
        # Use a temp file to update
        python3 << EOF
import configparser
config = configparser.ConfigParser()
config.read('$HOME/.aws/config')
section = 'profile ${SSO_PROFILE}'
if not config.has_section(section):
    config.add_section(section)
config.set(section, 'sso_start_url', '${SSO_START_URL}')
config.set(section, 'sso_region', '${SSO_REGION}')
config.set(section, 'sso_registration_scopes', 'sso:account:access')
with open('$HOME/.aws/config', 'w') as f:
    config.write(f)
EOF
    else
        cat >> ~/.aws/config << EOF

[profile ${SSO_PROFILE}]
sso_start_url = ${SSO_START_URL}
sso_region = ${SSO_REGION}
sso_registration_scopes = sso:account:access
EOF
    fi

    print_success "SSO profile created"
    echo ""
    print_step "Starting SSO login (this will open your browser)..."
    echo ""

    aws sso login --profile "${SSO_PROFILE}"

    # After login, we need to select the account and role
    echo ""
    print_step "Fetching available accounts..."

    # Get access token
    local cache_dir="$HOME/.aws/sso/cache"
    local token_file=$(ls -t "$cache_dir"/*.json 2>/dev/null | head -1)

    if [ -z "$token_file" ]; then
        print_error "Could not find SSO token cache"
        exit 1
    fi

    local access_token=$(jq -r '.accessToken' "$token_file")

    # List accounts
    local accounts=$(aws sso list-accounts \
        --access-token "$access_token" \
        --region "${SSO_REGION}" \
        --output json)

    echo ""
    echo "Available accounts:"
    echo "$accounts" | jq -r '.accountList[] | "  \(.accountId) - \(.accountName)"'
    echo ""

    read -p "Enter AWS Account ID to use: " SELECTED_ACCOUNT

    # List roles for the account
    local roles=$(aws sso list-account-roles \
        --access-token "$access_token" \
        --account-id "$SELECTED_ACCOUNT" \
        --region "${SSO_REGION}" \
        --output json)

    echo ""
    echo "Available roles:"
    echo "$roles" | jq -r '.roleList[] | "  \(.roleName)"'
    echo ""

    read -p "Enter role name to use: " SELECTED_ROLE

    # Update the profile with account and role
    python3 << EOF
import configparser
config = configparser.ConfigParser()
config.read('$HOME/.aws/config')
section = 'profile ${SSO_PROFILE}'
config.set(section, 'sso_account_id', '${SELECTED_ACCOUNT}')
config.set(section, 'sso_role_name', '${SELECTED_ROLE}')
config.set(section, 'region', '${AWS_REGION}')
with open('$HOME/.aws/config', 'w') as f:
    config.write(f)
EOF

    print_success "SSO profile configured with account ${SELECTED_ACCOUNT}"

    # Store for later use
    AWS_ACCOUNT_ID="$SELECTED_ACCOUNT"
}

login_sso() {
    print_step "Logging in via SSO..."

    # Check if we have valid credentials
    if aws sts get-caller-identity --profile "${SSO_PROFILE}" &>/dev/null; then
        print_success "Already logged in"
        return 0
    fi

    # Need to login
    print_info "Opening browser for SSO login..."
    aws sso login --profile "${SSO_PROFILE}"

    # Verify login worked
    if aws sts get-caller-identity --profile "${SSO_PROFILE}" &>/dev/null; then
        print_success "SSO login successful"
    else
        print_error "SSO login failed"
        exit 1
    fi
}

# =============================================================================
# GITHUB OIDC DEPLOYMENT
# =============================================================================

deploy_github_oidc() {
    print_header "Deploying GitHub OIDC Federation"

    # Get GitHub org/repo info
    if ! command -v gh &>/dev/null; then
        print_error "GitHub CLI not installed"
        exit 1
    fi

    local repo_info=$(gh repo view --json owner,name 2>/dev/null || echo "")
    if [ -z "$repo_info" ]; then
        print_warning "Not in a GitHub repository"
        read -p "Enter GitHub organization/username: " GITHUB_ORG
        read -p "Enter repository name: " GITHUB_REPO
    else
        GITHUB_ORG=$(echo "$repo_info" | jq -r '.owner.login')
        GITHUB_REPO=$(echo "$repo_info" | jq -r '.name')
        print_info "Detected repository: ${GITHUB_ORG}/${GITHUB_REPO}"
    fi

    echo ""
    read -p "GitHub Organization [${GITHUB_ORG}]: " input_org
    GITHUB_ORG=${input_org:-$GITHUB_ORG}

    echo ""
    print_info "Repository names are hardcoded for security:"
    print_info "Backend role will allow:"
    print_info "  - ${GITHUB_ORG}/robosystems"
    print_info "Frontend role will allow:"
    print_info "  - ${GITHUB_ORG}/robosystems-app"
    print_info "  - ${GITHUB_ORG}/roboledger-app"
    print_info "  - ${GITHUB_ORG}/roboinvestor-app"

    # Branch patterns are hardcoded in the template: main, release/*, v* tags
    echo ""
    print_step "Deploying CloudFormation stack: ${OIDC_STACK_NAME}"

    # Check if stack exists
    local stack_status=""
    stack_status=$(aws cloudformation describe-stacks \
        --stack-name "${OIDC_STACK_NAME}" \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null) || true

    local cf_action="create-stack"
    if [ -n "$stack_status" ]; then
        print_warning "Stack already exists (status: ${stack_status})"
        read -p "Update existing stack? (y/N): " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            cf_action="update-stack"
        else
            print_info "Skipping stack deployment"
            # Get existing role ARN
            GITHUB_ACTIONS_ROLE_ARN=$(aws cloudformation describe-stacks \
                --stack-name "${OIDC_STACK_NAME}" \
                --profile "${SSO_PROFILE}" \
                --region "${AWS_REGION}" \
                --query 'Stacks[0].Outputs[?OutputKey==`GitHubActionsRoleArn`].OutputValue' \
                --output text)
            return 0
        fi
    fi

    # Deploy/update stack
    local cf_output=""
    if [ "$cf_action" = "create-stack" ]; then
        aws cloudformation create-stack \
            --stack-name "${OIDC_STACK_NAME}" \
            --template-body file://cloudformation/bootstrap-oidc.yaml \
            --parameters \
                ParameterKey=GitHubOrg,ParameterValue="${GITHUB_ORG}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --profile "${SSO_PROFILE}" \
            --region "${AWS_REGION}" \
            --tags Key=Service,Value=RoboSystems Key=Component,Value=GitHubOIDC

        print_step "Waiting for stack creation..."
        aws cloudformation wait stack-create-complete \
            --stack-name "${OIDC_STACK_NAME}" \
            --profile "${SSO_PROFILE}" \
            --region "${AWS_REGION}"
    else
        aws cloudformation update-stack \
            --stack-name "${OIDC_STACK_NAME}" \
            --template-body file://cloudformation/bootstrap-oidc.yaml \
            --parameters \
                ParameterKey=GitHubOrg,ParameterValue="${GITHUB_ORG}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --profile "${SSO_PROFILE}" \
            --region "${AWS_REGION}" 2>&1 || {
                if [[ $? -eq 255 ]] && aws cloudformation describe-stacks \
                    --stack-name "${OIDC_STACK_NAME}" \
                    --profile "${SSO_PROFILE}" \
                    --region "${AWS_REGION}" &>/dev/null; then
                    print_info "No updates needed"
                else
                    print_error "Stack update failed"
                    exit 1
                fi
            }

        print_step "Waiting for stack update..."
        aws cloudformation wait stack-update-complete \
            --stack-name "${OIDC_STACK_NAME}" \
            --profile "${SSO_PROFILE}" \
            --region "${AWS_REGION}" 2>/dev/null || true
    fi

    # Get outputs
    GITHUB_ACTIONS_ROLE_ARN=$(aws cloudformation describe-stacks \
        --stack-name "${OIDC_STACK_NAME}" \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}" \
        --query 'Stacks[0].Outputs[?OutputKey==`GitHubActionsRoleArn`].OutputValue' \
        --output text)

    print_success "GitHub OIDC stack deployed"
    print_info "Role ARN: ${GITHUB_ACTIONS_ROLE_ARN}"
}

# =============================================================================
# GITHUB CONFIGURATION
# =============================================================================

configure_github() {
    print_header "Configuring GitHub Repository"

    # Get AWS Account ID
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity \
        --profile "${SSO_PROFILE}" \
        --query 'Account' \
        --output text)

    print_step "Setting GitHub variables for OIDC..."

    # Set the role ARN as a variable (not a secret - it's not sensitive)
    gh variable set AWS_ROLE_ARN --body "${GITHUB_ACTIONS_ROLE_ARN}"
    print_success "Set AWS_ROLE_ARN"

    gh variable set AWS_ACCOUNT_ID --body "${AWS_ACCOUNT_ID}"
    print_success "Set AWS_ACCOUNT_ID"

    gh variable set AWS_REGION --body "${AWS_REGION}"
    print_success "Set AWS_REGION"

    echo ""
    print_step "Note: No AWS secrets needed with OIDC!"
    print_info "Workflows will use role assumption instead of access keys"
}

# =============================================================================
# SSO USER EMAIL DETECTION
# =============================================================================

get_sso_user_email() {
    # Try to get the current SSO user's email from AWS Identity Store
    # Returns empty string if not found

    # Get identity store ID from SSO admin
    local store_id
    store_id=$(aws sso-admin list-instances \
        --query 'Instances[0].IdentityStoreId' \
        --output text 2>/dev/null) || return 0

    if [ -z "$store_id" ] || [ "$store_id" = "None" ]; then
        return 0
    fi

    # Get current caller identity ARN to extract username
    local caller_arn
    caller_arn=$(aws sts get-caller-identity --query 'Arn' --output text 2>/dev/null) || return 0

    # Extract username from ARN (format: ...assumed-role/AWSReservedSSO_.../username)
    local sso_username
    sso_username=$(echo "$caller_arn" | sed -n 's|.*/||p')

    if [ -z "$sso_username" ]; then
        return 0
    fi

    # Look up user in identity store by username
    local user_id
    user_id=$(aws identitystore list-users \
        --identity-store-id "$store_id" \
        --filters "AttributePath=UserName,AttributeValue=$sso_username" \
        --query 'Users[0].UserId' \
        --output text 2>/dev/null) || return 0

    if [ -z "$user_id" ] || [ "$user_id" = "None" ]; then
        return 0
    fi

    # Get user's email
    local email
    email=$(aws identitystore describe-user \
        --identity-store-id "$store_id" \
        --user-id "$user_id" \
        --query 'Emails[0].Value' \
        --output text 2>/dev/null) || return 0

    if [ -n "$email" ] && [ "$email" != "None" ]; then
        echo "$email"
    fi
}

# =============================================================================
# ESSENTIAL GITHUB VARIABLES
# =============================================================================

configure_essential_variables() {
    print_header "Essential Configuration"

    # AWS_SNS_ALERT_EMAIL - required for CloudWatch alarms (GitHub variable)
    print_step "Alert Email Configuration"

    # Check if already set in GitHub
    EXISTING_EMAIL=$(gh variable get AWS_SNS_ALERT_EMAIL 2>/dev/null || echo "")

    if [ -n "$EXISTING_EMAIL" ]; then
        print_success "AWS_SNS_ALERT_EMAIL already set: $EXISTING_EMAIL"
        ALERT_EMAIL="$EXISTING_EMAIL"
    else
        # Try to auto-detect from SSO user
        print_info "Detecting SSO user email..."
        ALERT_EMAIL=$(get_sso_user_email)

        if [ -n "$ALERT_EMAIL" ]; then
            print_success "Detected SSO user email: $ALERT_EMAIL"
            read -p "Use this email for alerts? (Y/n): " -n 1 -r
            echo ""
            if [[ $REPLY =~ ^[Nn]$ ]]; then
                read -p "Enter alert email address: " ALERT_EMAIL
            fi
        else
            echo ""
            echo "CloudWatch alarms will send notifications to this email."
            echo "You'll receive a confirmation email from AWS to activate alerts."
            echo ""
            read -p "Enter alert email address: " ALERT_EMAIL
        fi

        if [ -z "$ALERT_EMAIL" ]; then
            print_error "Alert email is required for deployment"
            exit 1
        fi

        gh variable set AWS_SNS_ALERT_EMAIL --body "$ALERT_EMAIL"
        print_success "Set AWS_SNS_ALERT_EMAIL (GitHub variable)"
    fi

    # Export for downstream scripts (gha.sh)
    export ALERT_EMAIL
}

# =============================================================================
# ECR REPOSITORY
# =============================================================================

setup_ecr_repository() {
    print_header "ECR Repository Setup"

    # Derive ECR repository name from GitHub repo name
    local ecr_repo_name
    ecr_repo_name=$(echo "${GITHUB_REPO}" | tr '[:upper:]' '[:lower:]')

    echo "ECR Repository: ${ecr_repo_name}"
    echo ""

    # Check if repository already exists - don't modify existing repos
    if aws ecr describe-repositories --repository-names "${ecr_repo_name}" --region "${AWS_REGION}" >/dev/null 2>&1; then
        print_success "ECR repository already exists"
        return 0
    fi

    print_step "Creating ECR repository..."

    aws ecr create-repository \
        --repository-name "${ecr_repo_name}" \
        --region "${AWS_REGION}" \
        --image-scanning-configuration scanOnPush=true \
        --encryption-configuration encryptionType=AES256 \
        --tags Key=Project,Value=RoboSystems Key=ManagedBy,Value=Bootstrap >/dev/null

    print_success "ECR repository created: ${ecr_repo_name}"

    # Set basic lifecycle policy for new repos (keeps things tidy)
    print_step "Setting basic lifecycle policy..."
    aws ecr put-lifecycle-policy \
        --repository-name "${ecr_repo_name}" \
        --region "${AWS_REGION}" \
        --lifecycle-policy-text '{
            "rules": [
                {
                    "rulePriority": 1,
                    "description": "Keep last 20 untagged images",
                    "selection": {
                        "tagStatus": "untagged",
                        "countType": "imageCountMoreThan",
                        "countNumber": 20
                    },
                    "action": { "type": "expire" }
                }
            ]
        }' >/dev/null

    print_success "ECR repository ready"
}

# =============================================================================
# AWS SECRETS & GITHUB VARIABLES SETUP
# =============================================================================

prompt_environment_choice() {
    # Prompt for environment choice and export for downstream scripts
    echo ""
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
        export SETUP_STAGING=true
        print_success "Configuring: Production + Staging"
    else
        export SETUP_STAGING=false
        print_success "Configuring: Production only"
    fi
}

setup_secrets_and_variables() {
    print_header "Application Configuration"

    echo "The following optional setup steps are available:"
    echo ""
    echo "  AWS Secrets Manager - Application secrets & feature flags"
    echo "                        (required for deployment, safe to re-run)"
    echo ""
    echo "  GitHub Variables    - ~80 variables for custom domains, scaling,"
    echo "                        instance sizes, etc. (optional, has defaults)"
    echo ""

    # Ask what to configure
    local run_aws=false
    local run_gha=false

    read -p "Setup AWS Secrets Manager? (Y/n): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        run_aws=true

        # Ask about API access mode for JWT configuration
        echo ""
        echo "API Access Mode:"
        echo "  1) Internal (default) - Access via bastion tunnel, no public exposure"
        echo "  2) Public             - Internet-facing with custom domain (HTTPS)"
        echo "  3) Public HTTP        - Internet-facing without domain (HTTP only)"
        echo ""
        read -p "Select access mode [1]: " access_mode_choice
        access_mode_choice=${access_mode_choice:-1}

        case "$access_mode_choice" in
            1) export API_ACCESS_MODE="internal" ;;
            2) export API_ACCESS_MODE="public" ;;
            3) export API_ACCESS_MODE="public-http" ;;
            *) export API_ACCESS_MODE="internal" ;;
        esac
        print_info "API access mode: $API_ACCESS_MODE"
    fi

    read -p "Setup GitHub Variables? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        run_gha=true
    fi

    # If neither selected, we're done
    if ! $run_aws && ! $run_gha; then
        print_info "Skipping optional configuration"
        echo ""
        print_info "Run later if needed:"
        echo "   just setup-aws   # AWS Secrets (required before deploy)"
        echo "   just setup-gha   # GitHub Variables (optional)"
        return 0
    fi

    # Only prompt for environment choice if we're running something
    prompt_environment_choice

    # Export profile for scripts
    export AWS_PROFILE="${SSO_PROFILE}"

    # Run selected setups
    if $run_aws; then
        echo ""
        print_step "Running AWS Secrets Manager setup..."
        ./bin/setup/aws.sh
    fi

    if $run_gha; then
        echo ""
        print_step "Running GitHub Variables setup..."
        ./bin/setup/gha.sh
    fi
}

# =============================================================================
# CHECK GITHUB SECRETS
# =============================================================================

check_github_secrets() {
    print_header "Checking GitHub Secrets"

    print_step "Checking for secrets (repo and org level)..."

    # Get repo-level secrets
    REPO_SECRETS=$(gh secret list 2>/dev/null || echo "")

    # Get org-level secrets (may fail if user doesn't have org admin access)
    ORG_SECRETS=$(gh secret list --org "${GITHUB_ORG}" 2>/dev/null || echo "")

    # Combine both lists for checking
    ALL_SECRETS="${REPO_SECRETS}"$'\n'"${ORG_SECRETS}"

    # Check for ACTIONS_TOKEN (optional - enhances PR/release automations)
    if echo "$ALL_SECRETS" | grep -q "ACTIONS_TOKEN"; then
        if echo "$REPO_SECRETS" | grep -q "ACTIONS_TOKEN"; then
            print_success "ACTIONS_TOKEN exists (repo-level)"
        else
            print_success "ACTIONS_TOKEN exists (org-level)"
        fi
    else
        print_info "ACTIONS_TOKEN not set (optional - enhances PR/release automations)"
        echo ""
        echo "  All workflows work without it, but with limitations:"
        echo "    - PRs created by create-pr.yml won't auto-trigger CI workflows"
        echo "    - Self-hosted runner checks limited to repo-level (no org runners)"
        echo ""
        echo "  Why? GitHub's GITHUB_TOKEN has anti-recursion protection:"
        echo "    - PRs created with GITHUB_TOKEN don't trigger on:pull_request"
        echo "    - Limited API access for checking org-level runners"
        echo ""
        echo "  To enable full functionality (create a PAT with repo scope):"
        echo "    gh secret set ACTIONS_TOKEN"
        echo ""
    fi

    # Check for ANTHROPIC_API_KEY (optional - enables Claude PR/release workflows)
    if echo "$ALL_SECRETS" | grep -q "ANTHROPIC_API_KEY"; then
        if echo "$REPO_SECRETS" | grep -q "ANTHROPIC_API_KEY"; then
            print_success "ANTHROPIC_API_KEY exists (repo-level)"
        else
            print_success "ANTHROPIC_API_KEY exists (org-level)"
        fi
    else
        print_info "ANTHROPIC_API_KEY not set (optional - enables Claude PR/release workflows)"
    fi

    echo ""
    print_info "Note: AWS credentials (access keys) are NOT needed with OIDC"
    print_info "Workflows authenticate via AWS_ROLE_ARN instead"
    print_info "AI inference uses AWS Bedrock (separate from ANTHROPIC_API_KEY)"
}

# =============================================================================
# SUMMARY
# =============================================================================

show_summary() {
    print_header "Bootstrap Complete!"

    echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  BOOTSTRAP COMPLETED SUCCESSFULLY${NC}"
    echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "${CYAN}AWS Profile:${NC} ${SSO_PROFILE}"
    echo -e "${CYAN}AWS Account:${NC} ${AWS_ACCOUNT_ID}"
    echo -e "${CYAN}AWS Region:${NC} ${AWS_REGION}"
    echo ""
    echo -e "${CYAN}GitHub OIDC Role:${NC}"
    echo "  ${GITHUB_ACTIONS_ROLE_ARN}"
    echo ""
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  SECURITY BENEFITS${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "  ✓ No long-term credentials stored anywhere"
    echo "  ✓ Credentials scoped to specific repo/branch"
    echo "  ✓ 1-hour max session (can't be abused if compromised)"
    echo ""
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  NEXT STEPS${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo ""
    # Show activation reminder if .envrc was created/updated
    if [ "$ENVRC_NEEDS_ACTIVATION" = true ]; then
        echo -e "${YELLOW}════════════════════════════════════════════════════════════${NC}"
        echo -e "${YELLOW}  ACTION REQUIRED: Activate .envrc${NC}"
        echo -e "${YELLOW}════════════════════════════════════════════════════════════${NC}"
        echo ""
        if command -v direnv &>/dev/null; then
            echo "  Run: direnv allow"
        else
            echo "  Run: source .envrc"
        fi
        echo ""
        echo "  This ensures future terminal sessions use the correct AWS profile."
        echo ""
    fi

    echo "1. Deploy to production:"
    echo "   just deploy prod"
    echo ""
    echo "   Workflows use sensible defaults. Deploys to VPC-only mode"
    echo "   (access via bastion tunnel) unless domain is configured."
    echo ""
    echo "To run skipped steps later:"
    echo "   just setup-aws      # Application secrets & feature flags (required)"
    echo "   just setup-gha      # Full variable control (optional)"
    echo ""
    echo "For CLI access:"
    echo "   aws sso login --profile ${SSO_PROFILE}"
    if command -v direnv &>/dev/null; then
        echo "   # AWS_PROFILE is auto-set by direnv"
    else
        echo "   export AWS_PROFILE=${SSO_PROFILE}"
    fi
    echo ""
}

# =============================================================================
# MAIN
# =============================================================================

main() {
    print_header "RoboSystems Bootstrap"

    echo "This script sets up AWS access for GitHub Actions:"
    echo ""
    echo "  • AWS CLI via SSO (temporary credentials)"
    echo "  • GitHub Actions via OIDC (no stored secrets)"
    echo ""
    echo -e "${CYAN}Configuration:${NC}"
    echo "  AWS Profile: ${SSO_PROFILE}"
    echo "  AWS Region:  ${AWS_REGION}"
    echo ""
    echo "Prerequisites:"
    echo "  ✓ AWS IAM Identity Center enabled"
    echo "  ✓ SSO admin account exists"
    echo "  ✓ GitHub CLI installed and authenticated"
    echo ""

    # Check prerequisites
    if ! command -v aws &>/dev/null; then
        print_error "AWS CLI not installed"
        exit 1
    fi

    if ! command -v gh &>/dev/null; then
        print_error "GitHub CLI not installed"
        exit 1
    fi

    if ! command -v jq &>/dev/null; then
        print_error "jq not installed"
        exit 1
    fi

    if ! gh auth status &>/dev/null; then
        print_error "GitHub CLI not authenticated. Run: gh auth login"
        exit 1
    fi

    read -p "Continue with bootstrap? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Bootstrap cancelled"
        exit 0
    fi

    # Setup direnv for AWS profile
    setup_direnv

    # Check/configure SSO
    if ! check_sso_configured; then
        configure_sso
    else
        login_sso
    fi

    # Get account ID for later
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity \
        --profile "${SSO_PROFILE}" \
        --query 'Account' \
        --output text)

    # Deploy GitHub OIDC
    deploy_github_oidc

    # Configure GitHub
    configure_github

    # Configure essential variables (alert email, S3 namespace)
    configure_essential_variables

    # Setup ECR repository
    setup_ecr_repository

    # Check GitHub secrets
    check_github_secrets

    # Setup AWS secrets and GitHub variables (with environment choice)
    setup_secrets_and_variables

    # Summary
    show_summary
}

main "$@"
