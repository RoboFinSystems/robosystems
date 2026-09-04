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
#   2. Deploys the GitHub OIDC CloudFormation stack (the deploy roles)
#   3. Sets GitHub variables (role ARN, etc.)
#   4. Sets the same identity variables on the frontend app repos, which own
#      no AWS access of their own — the repos come from the stack parameters
#   5. First run only: ECR repository, SES identity, application secrets
#
# MODES:
#   full (default)  Everything above. On a re-run the application-config
#                   step (Secrets Manager, GitHub variables) is skipped unless
#                   --with-app-config is given — those are the two steps that
#                   can rewrite a live account, so they never run by accident.
#   --oidc          Only the OIDC stack + the identity variables, here and on
#                   the frontend app repos. Use this
#                   after editing cloudformation/bootstrap-oidc.yaml. The stack
#                   update is previewed as a change set: you see exactly which
#                   resources change before anything is applied, and a stack
#                   that already matches the template is reported, not touched.
#
# USAGE:
#   just bootstrap [profile] [region]          # full
#   just bootstrap-oidc [profile] [region]     # --oidc
#   bin/setup/bootstrap.sh [--oidc] [--with-app-config] [profile] [region]
#
# ARGUMENTS:
#   profile: AWS SSO profile name (default: robosystems-sso)
#   region:  AWS region (default: us-east-1)
#
# EXAMPLES:
#   just bootstrap                           # Use defaults
#   just bootstrap my-fork-sso               # Custom profile
#   just bootstrap my-fork-sso eu-west-1     # Custom profile and region
#   just bootstrap-oidc                      # Re-apply the deploy roles only
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

usage() {
    echo "Usage: bin/setup/bootstrap.sh [--oidc] [--with-app-config] [profile] [region]"
    echo ""
    echo "  --oidc             Only deploy/update the GitHub OIDC stack and refresh the"
    echo "                     identity variables (AWS_ROLE_ARN, AWS_ACCOUNT_ID, AWS_REGION)."
    echo "  --with-app-config  On a re-run, also offer the Secrets Manager / GitHub"
    echo "                     variables setup (skipped by default after the first run)."
    echo "  profile            AWS SSO profile name (default: robosystems-sso)"
    echo "  region             AWS region (default: us-east-1)"
}

# Parse arguments - flags first, then positional profile/region. Positionals
# take priority over environment variables.
BOOTSTRAP_MODE="full"
WITH_APP_CONFIG=false
_pos_profile=""
_pos_region=""
for arg in "$@"; do
    case "$arg" in
        --oidc|--oidc-only) BOOTSTRAP_MODE="oidc" ;;
        --with-app-config)  WITH_APP_CONFIG=true ;;
        -h|--help)          usage; exit 0 ;;
        --*)                echo "Unknown option: $arg" >&2; usage >&2; exit 1 ;;
        *)
            if [ -z "$_pos_profile" ]; then
                _pos_profile="$arg"
            elif [ -z "$_pos_region" ]; then
                _pos_region="$arg"
            else
                echo "Unexpected argument: $arg" >&2; usage >&2; exit 1
            fi
            ;;
    esac
done
SSO_PROFILE="${_pos_profile:-${AWS_PROFILE:-robosystems-sso}}"
AWS_REGION="${_pos_region:-${AWS_REGION:-us-east-1}}"
OIDC_STACK_NAME="RoboSystemsGitHubOIDC"

# Set when this run creates the OIDC stack, i.e. this is the first bootstrap
# of the account. Gates the application-config step in full mode.
OIDC_STACK_CREATED=false

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

    local repo_info
    repo_info=$(gh repo view --json owner,name 2>/dev/null || echo "")
    if [ -z "$repo_info" ]; then
        print_warning "Not in a GitHub repository"
        read -p "Enter GitHub organization/username: " GITHUB_ORG
        read -p "Enter repository name: " GITHUB_REPO
    else
        GITHUB_ORG=$(echo "$repo_info" | jq -r '.owner.login')
        GITHUB_REPO=$(echo "$repo_info" | jq -r '.name')
        print_info "Detected repository: ${GITHUB_ORG}/${GITHUB_REPO}"
    fi

    # Check if stack exists
    local stack_status=""
    stack_status=$(aws cloudformation describe-stacks \
        --stack-name "${OIDC_STACK_NAME}" \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null) || true

    if [ -n "$stack_status" ]; then
        # An existing stack keeps its trust identity: the org/repo come from
        # its parameters, not from a prompt, so a re-run can never re-point
        # the deploy roles at a different repository by a stray keystroke.
        local stack_org stack_repo
        stack_org=$(aws cloudformation describe-stacks \
            --stack-name "${OIDC_STACK_NAME}" \
            --profile "${SSO_PROFILE}" \
            --region "${AWS_REGION}" \
            --query 'Stacks[0].Parameters[?ParameterKey==`GitHubOrg`].ParameterValue' \
            --output text 2>/dev/null) || stack_org=""
        stack_repo=$(aws cloudformation describe-stacks \
            --stack-name "${OIDC_STACK_NAME}" \
            --profile "${SSO_PROFILE}" \
            --region "${AWS_REGION}" \
            --query 'Stacks[0].Parameters[?ParameterKey==`GitHubRepoName`].ParameterValue' \
            --output text 2>/dev/null) || stack_repo=""
        [ -n "$stack_org" ] && [ "$stack_org" != "None" ] && GITHUB_ORG="$stack_org"
        [ -n "$stack_repo" ] && [ "$stack_repo" != "None" ] && GITHUB_REPO="$stack_repo"
        print_info "Stack trusts: ${GITHUB_ORG}/${GITHUB_REPO} (from the existing stack's parameters)"
    else
        echo ""
        read -p "GitHub Organization [${GITHUB_ORG}]: " input_org
        GITHUB_ORG=${input_org:-$GITHUB_ORG}
    fi

    echo ""
    print_info "Backend role will allow:"
    print_info "  - ${GITHUB_ORG}/${GITHUB_REPO}"
    print_info "Frontend role will allow (template defaults; override via stack parameters):"
    print_info "  - ${GITHUB_ORG}/robosystems-app"
    print_info "  - ${GITHUB_ORG}/roboledger-app"
    print_info "  - ${GITHUB_ORG}/roboinvestor-app"
    print_info "  - ${GITHUB_ORG}/robosystems-holon-viewer"

    # Branch patterns are hardcoded in the template: main, release/*, v* tags
    echo ""
    print_step "Deploying CloudFormation stack: ${OIDC_STACK_NAME}"

    if [ -z "$stack_status" ]; then
        deploy_oidc_create
    else
        deploy_oidc_update "$stack_status"
    fi

    # Get outputs
    GITHUB_ACTIONS_ROLE_ARN=$(aws cloudformation describe-stacks \
        --stack-name "${OIDC_STACK_NAME}" \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}" \
        --query 'Stacks[0].Outputs[?OutputKey==`GitHubActionsRoleArn`].OutputValue' \
        --output text)

    print_info "Role ARN: ${GITHUB_ACTIONS_ROLE_ARN}"
}

deploy_oidc_create() {
    aws cloudformation create-stack \
        --stack-name "${OIDC_STACK_NAME}" \
        --template-body file://cloudformation/bootstrap-oidc.yaml \
        --parameters \
            ParameterKey=GitHubOrg,ParameterValue="${GITHUB_ORG}" \
            ParameterKey=GitHubRepoName,ParameterValue="${GITHUB_REPO}" \
        --capabilities CAPABILITY_NAMED_IAM \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}" \
        --tags Key=Service,Value=RoboSystems Key=Component,Value=GitHubOIDC

    print_step "Waiting for stack creation..."
    aws cloudformation wait stack-create-complete \
        --stack-name "${OIDC_STACK_NAME}" \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}"

    OIDC_STACK_CREATED=true
    print_success "GitHub OIDC stack created"
}

# Update path. The template is applied through a change set so the operator
# sees which resources would change before anything is touched, and a stack
# that already matches the template is reported as such — the prompt only
# appears when there is something to apply. That closes the gap where a
# hardening edit sat in the template for weeks, unapplied, because nothing
# said the live stack was behind.
deploy_oidc_update() {
    local stack_status="$1"

    # Where the template and the live stack each stand.
    local template_commit template_date branch dirty="" stack_updated
    template_commit=$(git log -1 --format='%h' -- cloudformation/bootstrap-oidc.yaml 2>/dev/null || echo "?")
    template_date=$(git log -1 --format='%ad' --date=short -- cloudformation/bootstrap-oidc.yaml 2>/dev/null || echo "?")
    branch=$(git branch --show-current 2>/dev/null || echo "?")
    if ! git diff --quiet -- cloudformation/bootstrap-oidc.yaml 2>/dev/null; then
        dirty=" + uncommitted edits"
    fi
    stack_updated=$(aws cloudformation describe-stacks \
        --stack-name "${OIDC_STACK_NAME}" \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}" \
        --query 'Stacks[0].[LastUpdatedTime, CreationTime] | [?@ != `null`] | [0]' \
        --output text 2>/dev/null) || stack_updated="?"

    echo ""
    print_info "Stack:    ${stack_status}, last applied ${stack_updated}"
    print_info "Template: cloudformation/bootstrap-oidc.yaml @ ${template_commit} (${template_date}) on '${branch}'${dirty}"
    if [ "$branch" != "main" ]; then
        print_warning "Not on main — whatever is in this checkout is what gets applied"
    fi

    case "$stack_status" in
        *_IN_PROGRESS)
            print_error "Stack is busy (${stack_status}) — wait for the current operation to finish"
            exit 1
            ;;
        UPDATE_ROLLBACK_FAILED|ROLLBACK_COMPLETE|*_FAILED)
            print_error "Stack is ${stack_status} and cannot be updated from here — inspect it in the console first"
            exit 1
            ;;
    esac

    local change_set
    change_set="bootstrap-$(date +%Y%m%d%H%M%S)"
    print_step "Computing what would change (change set ${change_set})..."
    aws cloudformation create-change-set \
        --stack-name "${OIDC_STACK_NAME}" \
        --change-set-name "${change_set}" \
        --change-set-type UPDATE \
        --template-body file://cloudformation/bootstrap-oidc.yaml \
        --parameters \
            ParameterKey=GitHubOrg,ParameterValue="${GITHUB_ORG}" \
            ParameterKey=GitHubRepoName,ParameterValue="${GITHUB_REPO}" \
        --capabilities CAPABILITY_NAMED_IAM \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}" \
        --tags Key=Service,Value=RoboSystems Key=Component,Value=GitHubOIDC >/dev/null

    if ! aws cloudformation wait change-set-create-complete \
        --stack-name "${OIDC_STACK_NAME}" \
        --change-set-name "${change_set}" \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}" 2>/dev/null; then
        local reason
        reason=$(aws cloudformation describe-change-set \
            --stack-name "${OIDC_STACK_NAME}" \
            --change-set-name "${change_set}" \
            --profile "${SSO_PROFILE}" \
            --region "${AWS_REGION}" \
            --query 'StatusReason' --output text 2>/dev/null) || reason=""
        aws cloudformation delete-change-set \
            --stack-name "${OIDC_STACK_NAME}" \
            --change-set-name "${change_set}" \
            --profile "${SSO_PROFILE}" \
            --region "${AWS_REGION}" >/dev/null 2>&1 || true
        if echo "$reason" | grep -qi "didn't contain changes\|No updates are to be performed"; then
            print_success "Stack already matches the template — nothing to apply"
            return 0
        fi
        print_error "Could not compute the change set: ${reason:-unknown reason}"
        exit 1
    fi

    echo ""
    echo "Changes the template would make to ${OIDC_STACK_NAME}:"
    aws cloudformation describe-change-set \
        --stack-name "${OIDC_STACK_NAME}" \
        --change-set-name "${change_set}" \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}" \
        --query 'Changes[].ResourceChange.[Action, LogicalResourceId, ResourceType, Replacement]' \
        --output text | while IFS=$'\t' read -r action logical rtype replacement; do
            local note=""
            if [ "$replacement" = "True" ]; then note="  (REPLACEMENT)"; fi
            echo "  ${action}  ${logical}  [${rtype}]${note}"
        done
    echo ""

    read -p "Apply these changes to ${OIDC_STACK_NAME}? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        aws cloudformation delete-change-set \
            --stack-name "${OIDC_STACK_NAME}" \
            --change-set-name "${change_set}" \
            --profile "${SSO_PROFILE}" \
            --region "${AWS_REGION}" >/dev/null 2>&1 || true
        print_info "Left the stack unchanged"
        return 0
    fi

    aws cloudformation execute-change-set \
        --stack-name "${OIDC_STACK_NAME}" \
        --change-set-name "${change_set}" \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}" >/dev/null

    print_step "Waiting for stack update..."
    aws cloudformation wait stack-update-complete \
        --stack-name "${OIDC_STACK_NAME}" \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}"

    print_success "GitHub OIDC stack updated"
}

# =============================================================================
# GITHUB CONFIGURATION
# =============================================================================

# The optional third argument targets another repository (owner/name); with it
# omitted the variable is set on the current one. Written without arrays so the
# script still runs on a stock macOS bash 3.2.
set_variable_if_changed() {
    local name="$1" value="$2" repo="${3:-}" current
    if [ -n "$repo" ]; then
        current=$(gh variable get "$name" --repo "$repo" 2>/dev/null || echo "")
    else
        current=$(gh variable get "$name" 2>/dev/null || echo "")
    fi

    if [ "$current" = "$value" ]; then
        print_success "${name} already set"
        return 0
    fi

    if [ -n "$repo" ]; then
        gh variable set "$name" --body "$value" --repo "$repo"
    else
        gh variable set "$name" --body "$value"
    fi
    print_success "Set ${name}"
}

configure_github() {
    print_header "Configuring GitHub Repository"

    # Get AWS Account ID
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity \
        --profile "${SSO_PROFILE}" \
        --query 'Account' \
        --output text)

    print_step "Checking GitHub identity variables..."

    # These three are derived, not configured (the ARN is not sensitive), so
    # they are safe to re-assert — but only write when the value differs, so a
    # re-run against a correct account is read-only here.
    set_variable_if_changed AWS_ROLE_ARN "${GITHUB_ACTIONS_ROLE_ARN}"
    set_variable_if_changed AWS_ACCOUNT_ID "${AWS_ACCOUNT_ID}"
    set_variable_if_changed AWS_REGION "${AWS_REGION}"

    echo ""
    print_step "Note: No AWS secrets needed with OIDC!"
    print_info "Workflows will use role assumption instead of access keys"
}

# =============================================================================
# FRONTEND REPOSITORY CONFIGURATION
# =============================================================================

# The frontend apps deploy through the stack this script owns, assuming the
# separate frontend role. Nothing they need is per-repo — one role ARN, one
# account, one region, all three known here the moment the stack is up. So the
# identity variables are pushed from this side, and the app repos need no AWS
# access, no aws CLI and no SSO profile of their own.
configure_frontend_repos() {
    print_header "Configuring Frontend Repositories"

    local frontend_role_arn
    frontend_role_arn=$(aws cloudformation describe-stacks \
        --stack-name "${OIDC_STACK_NAME}" \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}" \
        --query 'Stacks[0].Outputs[?OutputKey==`GitHubActionsFrontendRoleArn`].OutputValue' \
        --output text 2>/dev/null) || frontend_role_arn=""

    if [ -z "$frontend_role_arn" ] || [ "$frontend_role_arn" = "None" ]; then
        print_warning "Stack exposes no GitHubActionsFrontendRoleArn — skipping frontends"
        print_info "Run 'just bootstrap-oidc' to update the stack from the current template"
        return 0
    fi

    # Repo names are stack parameters, so a fork that renamed its apps stays
    # consistent: take the trusted names from the stack rather than a list
    # hardcoded here that would silently drift from the trust policy.
    local params
    params=$(aws cloudformation describe-stacks \
        --stack-name "${OIDC_STACK_NAME}" \
        --profile "${SSO_PROFILE}" \
        --region "${AWS_REGION}" \
        --query 'Stacks[0].Parameters' \
        --output json 2>/dev/null) || params="[]"

    local repo_names="" key name
    for key in GitHubAppRepoName GitHubLedgerAppRepoName \
        GitHubInvestorAppRepoName GitHubHolonViewerRepoName; do
        name=$(echo "$params" | jq -r --arg k "$key" \
            '.[] | select(.ParameterKey == $k) | .ParameterValue')
        if [ -n "$name" ] && [ "$name" != "null" ]; then
            repo_names="${repo_names}${name}
"
        fi
    done

    if [ -z "$repo_names" ]; then
        print_warning "Stack carries no frontend repository parameters — skipping frontends"
        return 0
    fi

    print_info "Frontend role: ${frontend_role_arn}"

    local repo_name target
    while IFS= read -r repo_name; do
        [ -z "$repo_name" ] && continue
        target="${GITHUB_ORG}/${repo_name}"

        echo ""
        print_step "${target}"

        if ! gh repo view "$target" &>/dev/null; then
            print_warning "Not accessible to this gh account — skipping"
            continue
        fi

        set_variable_if_changed AWS_ROLE_ARN "$frontend_role_arn" "$target"
        set_variable_if_changed AWS_ACCOUNT_ID "$AWS_ACCOUNT_ID" "$target"
        set_variable_if_changed AWS_REGION "$AWS_REGION" "$target"
    done <<EOF
${repo_names}
EOF

    echo ""
    print_info "Each app's remaining setup is GitHub-only: bin/gha-setup.sh"
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

    # Fleet-uniform ECR name: the deploy role's ECR scope, the AWS_ECR_REPOSITORY
    # default in gha.sh, and the workflow fallback all assume "robosystems"
    # regardless of the GitHub repo name — a renamed fork deriving the name from
    # its repo would create a repository the deploy role cannot access
    local ecr_repo_name="robosystems"

    # Resolve the bundled operational policy file (lives alongside this script)
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local policy_file="${script_dir}/ecr-lifecycle-policy.json"

    echo "ECR Repository: ${ecr_repo_name}"
    echo ""

    # Create the repository if it does not exist yet (never modifies an existing one)
    if aws ecr describe-repositories --repository-names "${ecr_repo_name}" --region "${AWS_REGION}" >/dev/null 2>&1; then
        print_success "ECR repository already exists"
    else
        print_step "Creating ECR repository..."
        aws ecr create-repository \
            --repository-name "${ecr_repo_name}" \
            --region "${AWS_REGION}" \
            --image-scanning-configuration scanOnPush=true \
            --encryption-configuration encryptionType=AES256 \
            --tags Key=Project,Value=RoboSystems Key=ManagedBy,Value=Bootstrap >/dev/null
        print_success "ECR repository created: ${ecr_repo_name}"
    fi

    # Choose the lifecycle policy. Honors a non-interactive override:
    #   ECR_LIFECYCLE_POLICY=robust|basic|skip
    # Without an override, a repository that already carries a lifecycle policy
    # is never prompted about: when it matches the bundled operational policy
    # there is nothing to do, and when it differs the reconcile is a deliberate
    # act (just bootstrap-ecr-lifecycle), not a keystroke in a setup flow.
    local policy_choice="${ECR_LIFECYCLE_POLICY:-}"
    if [ -z "$policy_choice" ]; then
        local live_policy
        live_policy=$(aws ecr get-lifecycle-policy \
            --repository-name "${ecr_repo_name}" \
            --region "${AWS_REGION}" \
            --query 'lifecyclePolicyText' --output text 2>/dev/null) || live_policy=""
        if [ -n "$live_policy" ] && [ "$live_policy" != "None" ]; then
            if [ -f "$policy_file" ] && [ "$(echo "$live_policy" | jq -S -c .)" = "$(jq -S -c . "$policy_file")" ]; then
                print_success "Lifecycle policy matches ${policy_file##*/} — nothing to do"
            else
                print_warning "Live lifecycle policy differs from ${policy_file##*/} — leaving it unchanged"
                print_info "Reconcile deliberately with: just bootstrap-ecr-lifecycle"
            fi
            return 0
        fi
        echo ""
        echo "Which ECR image lifecycle policy do you want to apply?"
        echo "  1) Robust - full operational policy, count-based dev-image retention (recommended)"
        echo "  2) Basic  - minimal, keep last 20 untagged images only"
        echo "  3) Skip   - leave the existing lifecycle policy unchanged"
        echo ""
        read -p "Select [1]: " lc_choice
        lc_choice=${lc_choice:-1}
        case "$lc_choice" in
            1) policy_choice="robust" ;;
            2) policy_choice="basic" ;;
            3) policy_choice="skip" ;;
            *)
                print_warning "Unrecognized choice '${lc_choice}' — leaving the lifecycle policy unchanged"
                policy_choice="skip"
                ;;
        esac
    fi

    # 'none' is an accepted alias for 'skip' (e.g. ECR_LIFECYCLE_POLICY=none)
    if [ "$policy_choice" = "none" ]; then
        policy_choice="skip"
    fi

    # Skip path: never touches an existing repo's lifecycle policy.
    if [ "$policy_choice" = "skip" ]; then
        print_info "Leaving the existing ECR lifecycle policy unchanged"
        print_success "ECR repository ready (lifecycle policy unchanged)"
        return 0
    fi

    if [ "$policy_choice" = "basic" ]; then
        print_step "Applying basic lifecycle policy..."
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
    else
        if [ ! -f "$policy_file" ]; then
            print_error "Operational policy file not found: ${policy_file}"
            return 1
        fi
        print_step "Applying robust lifecycle policy (${policy_file})..."
        aws ecr put-lifecycle-policy \
            --repository-name "${ecr_repo_name}" \
            --region "${AWS_REGION}" \
            --lifecycle-policy-text "file://${policy_file}" >/dev/null
    fi

    print_success "ECR repository ready (${policy_choice} lifecycle policy)"
}

# =============================================================================
# SES EMAIL IDENTITY
# =============================================================================

setup_ses_identity() {
    print_header "SES Email Identity Setup"

    print_info "Transactional emails (verification, password reset, welcome) require a verified SES domain."
    echo ""

    # Check if already set in GitHub
    local existing_domain
    existing_domain=$(gh variable get AWS_SES_DOMAIN 2>/dev/null || echo "")

    if [ -n "$existing_domain" ]; then
        print_success "AWS_SES_DOMAIN already set: $existing_domain"
        local email_domain="$existing_domain"
    else
        read -p "Enter email domain [robosystems.ai]: " email_domain
        email_domain=${email_domain:-robosystems.ai}
    fi

    print_info "Domain: ${email_domain}"
    echo ""

    # Save to GitHub variables for future reference
    if [ -z "$existing_domain" ] || [ "$existing_domain" != "$email_domain" ]; then
        gh variable set AWS_SES_DOMAIN --body "$email_domain"
        print_success "Set AWS_SES_DOMAIN GitHub variable"
    fi

    # Check if identity already exists and is verified
    local identity_status
    identity_status=$(aws sesv2 get-email-identity \
        --email-identity "${email_domain}" \
        --region "${AWS_REGION}" \
        --query 'DkimAttributes.Status' \
        --output text 2>/dev/null) || identity_status=""

    if [ "$identity_status" = "SUCCESS" ]; then
        print_success "SES domain identity already verified: ${email_domain}"
        ensure_ses_production_access
        return 0
    fi

    if [ -n "$identity_status" ]; then
        print_warning "SES identity exists but DKIM status: ${identity_status}"
    else
        print_step "Creating SES email identity for ${email_domain}..."

        aws sesv2 create-email-identity \
            --email-identity "${email_domain}" \
            --region "${AWS_REGION}" >/dev/null

        print_success "SES email identity created"
    fi

    # Get DKIM tokens
    local dkim_tokens
    dkim_tokens=$(aws sesv2 get-email-identity \
        --email-identity "${email_domain}" \
        --region "${AWS_REGION}" \
        --query 'DkimAttributes.Tokens' \
        --output json 2>/dev/null)

    if [ -z "$dkim_tokens" ] || [ "$dkim_tokens" = "null" ]; then
        print_warning "Could not retrieve DKIM tokens"
        print_info "Check the SES console for DNS records"
        return 0
    fi

    # Try to add DKIM records to Route53 automatically
    local hosted_zone_id
    hosted_zone_id=$(aws route53 list-hosted-zones \
        --query "HostedZones[?Name=='${email_domain}.'].Id" \
        --output text 2>/dev/null | sed 's|/hostedzone/||') || hosted_zone_id=""

    if [ -n "$hosted_zone_id" ]; then
        print_step "Adding DKIM records to Route53 hosted zone..."

        # Build the change batch JSON
        local changes="["
        local first=true
        for token in $(echo "$dkim_tokens" | jq -r '.[]'); do
            if [ "$first" = true ]; then
                first=false
            else
                changes+=","
            fi
            changes+="{
                \"Action\": \"UPSERT\",
                \"ResourceRecordSet\": {
                    \"Name\": \"${token}._domainkey.${email_domain}\",
                    \"Type\": \"CNAME\",
                    \"TTL\": 300,
                    \"ResourceRecords\": [{\"Value\": \"${token}.dkim.amazonses.com\"}]
                }
            }"
        done
        changes+="]"

        aws route53 change-resource-record-sets \
            --hosted-zone-id "$hosted_zone_id" \
            --change-batch "{
                \"Comment\": \"SES DKIM verification for ${email_domain}\",
                \"Changes\": ${changes}
            }" >/dev/null

        print_success "DKIM records added to Route53"
        print_info "DKIM verification usually completes within a few minutes"
    else
        # No Route53 zone - print records for manual DNS setup
        echo ""
        print_warning "Route53 hosted zone not found for ${email_domain}"
        print_step "Add these CNAME records to your DNS provider:"
        echo ""

        echo "$dkim_tokens" | jq -r '.[]' | while read -r token; do
            echo -e "  ${CYAN}CNAME${NC}  ${token}._domainkey.${email_domain}"
            echo -e "  ${CYAN}Value${NC}  ${token}.dkim.amazonses.com"
            echo ""
        done
    fi

    # Request production access
    ensure_ses_production_access
}

ensure_ses_production_access() {
    # Check if already in production mode
    local production_access
    production_access=$(aws sesv2 get-account \
        --region "${AWS_REGION}" \
        --query 'ProductionAccessEnabled' \
        --output text 2>/dev/null) || production_access=""

    if [ "$production_access" = "True" ]; then
        print_success "SES production access already enabled"
        return 0
    fi

    print_step "Requesting SES production access..."
    print_info "SES sandbox mode only allows sending to verified addresses"
    print_info "Production access is required for transactional emails"
    echo ""

    aws sesv2 put-account-details \
        --production-access-enabled \
        --mail-type TRANSACTIONAL \
        --website-url "https://${email_domain:-robosystems.ai}" \
        --use-case-description "Transactional emails only: account email verification, password reset, and welcome emails for our B2B SaaS platform. No marketing or bulk emails. Low volume - estimated under 1000 emails/month." \
        --contact-language EN \
        --additional-contact-email-addresses "${ALERT_EMAIL:-}" \
        --region "${AWS_REGION}" 2>/dev/null && {
        print_success "SES production access requested"
        print_info "AWS typically approves within 24 hours"
    } || {
        print_warning "Could not request production access automatically"
        print_info "Request production access via the SES console"
    }
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

    # Only a first bootstrap (this run created the OIDC stack) walks into the
    # secrets/variables setup. On a re-run these are the steps that can rewrite
    # a live account — gha.sh re-asserts every repository variable — so they
    # run only when asked for by flag, never by an Enter or a stray 'y'.
    if [ "$OIDC_STACK_CREATED" != true ] && [ "$WITH_APP_CONFIG" != true ]; then
        print_info "Skipped on a re-run. Nothing here is touched unless you ask for it:"
        echo ""
        echo "   just setup-aws        # secrets + SSM parameters (never overwrites an existing secret)"
        echo "   just setup-gha        # GitHub variables — RE-ASSERTS all ~185 repo variables;"
        echo "                         # review the defaults before running against a live account"
        echo "   bin/setup/bootstrap.sh --with-app-config   # include them in this flow"
        return 0
    fi

    echo "The following optional setup steps are available:"
    echo ""
    echo "  AWS Secrets Manager - Application secrets & feature flags"
    echo "                        (required for deployment, safe to re-run)"
    echo ""
    echo "  GitHub Variables    - ~185 variables for custom domains, scaling,"
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
        echo "  1) Internal (default) - Access via SSM tunnel, no public exposure"
        echo "  2) Public             - Internet-facing with custom domain (HTTPS)"
        echo ""
        read -p "Select access mode [1]: " access_mode_choice
        access_mode_choice=${access_mode_choice:-1}

        case "$access_mode_choice" in
            1) export API_ACCESS_MODE="internal" ;;
            2) export API_ACCESS_MODE="public" ;;
            *) export API_ACCESS_MODE="internal" ;;
        esac
        print_info "API access mode: $API_ACCESS_MODE"
    fi

    echo ""
    print_warning "GitHub Variables (re)writes ~185 repository variables. On a live account"
    print_warning "that resets instance sizes, capacities and feature toggles to the defaults."
    read -p "Type 'yes' to run the GitHub Variables setup, anything else to skip: " -r
    if [ "$REPLY" = "yes" ]; then
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
        echo "  ACTIONS_TOKEN enables:"
        echo "    - Push to protected branches (create-release.yml)"
        echo "    - Push tags and create GitHub releases (tag-release.yml)"
        echo "    - Org-level self-hosted runner checks (claude.yml)"
        echo ""
        echo "  Without it, workflows fall back to github.token with limitations:"
        echo "    - May fail on protected branches/tags"
        echo "    - PRs won't trigger on:pull_request workflows"
        echo "    - Runner checks limited to repo-level"
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
    if [ "$BOOTSTRAP_MODE" = "oidc" ]; then
        print_header "OIDC Stack Complete"
        echo -e "${CYAN}AWS Account:${NC} ${AWS_ACCOUNT_ID}   ${CYAN}Region:${NC} ${AWS_REGION}"
        echo -e "${CYAN}GitHub OIDC Role:${NC} ${GITHUB_ACTIONS_ROLE_ARN}"
        echo ""
        echo "Nothing else was touched (no ECR, SES, secrets or variable changes)."
        return 0
    fi

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
    echo "   (access via SSM tunnel) unless domain is configured."
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
    if [ "$BOOTSTRAP_MODE" = "oidc" ]; then
        echo "  Mode:        oidc — deploy roles + identity variables only"
    elif [ "$WITH_APP_CONFIG" = true ]; then
        echo "  Mode:        full, including application config (secrets / variables)"
    else
        echo "  Mode:        full (application config only on a first run)"
    fi
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

    # The OIDC-only path touches no local files: the profile is already
    # exported above, and .envrc belongs to the full first-run flow.
    if [ "$BOOTSTRAP_MODE" != "oidc" ]; then
        setup_direnv
    fi

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

    # Push the identity variables to the frontend app repos
    configure_frontend_repos

    if [ "$BOOTSTRAP_MODE" = "oidc" ]; then
        show_summary
        return 0
    fi

    # Configure essential variables (alert email, S3 namespace)
    configure_essential_variables

    # Setup ECR repository
    setup_ecr_repository

    # Setup SES email identity for transactional emails
    setup_ses_identity

    # Check GitHub secrets
    check_github_secrets

    # Setup AWS secrets and GitHub variables (with environment choice)
    setup_secrets_and_variables

    # Summary
    show_summary
}

main "$@"
