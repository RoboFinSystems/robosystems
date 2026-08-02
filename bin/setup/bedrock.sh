#!/bin/bash
# =============================================================================
# BEDROCK LOCAL DEVELOPMENT SETUP
# =============================================================================
#
# Creates an IAM user with Bedrock access for local Docker development.
# Updates .env with the credentials.
#
# USAGE:
#   just setup-bedrock            # initial setup (user, policy, key, .env)
#   just setup-bedrock rotate     # rotate the access key and update .env
#
# ROTATION: creates a new key, updates .env, verifies the new key works,
# then deactivates the old key. The deactivated key is deleted on the NEXT
# rotation (two-key cycle), so there is always a working fallback.
#
# PREREQUISITES:
#   - Bootstrap completed first (just bootstrap)
#   - SSO session active (aws sso login --profile robosystems-sso)
#
# NOTE: This is optional. Only needed if you want to test AI/Bedrock features
# locally. Production uses IAM role credentials via ECS task role.
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

# Configuration
IAM_USER_NAME="RoboSystemsBedrockDev"
POLICY_NAME="RoboSystemsBedrockAccess"
AWS_REGION="${AWS_REGION:-us-east-1}"
SSO_PROFILE="${AWS_PROFILE:-robosystems-sso}"
ENV_FILE=".env"

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

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

# Build AWS CLI command with SSO profile
aws_cmd() {
    aws --profile "$SSO_PROFILE" "$@"
}

# =============================================================================
# POLICY DOCUMENT
# =============================================================================

# Marketplace product IDs (global, not account-specific):
#   prod-5ukwuglpt66kg = Claude Sonnet 4.6
#   prod-mxcfnwvpd6kb4 = Claude Sonnet 4.5
#   prod-4pmewlybdftbs = Claude Sonnet 4
# Full list: https://docs.aws.amazon.com/bedrock/latest/userguide/model-access-product-ids.html
BEDROCK_POLICY=$(cat <<'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "BedrockInvokeModel",
            "Effect": "Allow",
            "Action": [
                "bedrock:InvokeModel",
                "bedrock:InvokeModelWithResponseStream"
            ],
            "Resource": [
                "arn:aws:bedrock:*::foundation-model/anthropic.claude-*",
                "arn:aws:bedrock:*:*:inference-profile/*"
            ]
        },
        {
            "Sid": "BedrockModelAccess",
            "Effect": "Allow",
            "Action": [
                "bedrock:GetFoundationModel",
                "bedrock:ListFoundationModels"
            ],
            "Resource": "*"
        },
        {
            "Sid": "MarketplaceModelSubscription",
            "Effect": "Allow",
            "Action": [
                "aws-marketplace:ViewSubscriptions",
                "aws-marketplace:Subscribe"
            ],
            "Resource": "*",
            "Condition": {
                "ForAnyValue:StringEquals": {
                    "aws-marketplace:ProductId": [
                        "prod-5ukwuglpt66kg",
                        "prod-mxcfnwvpd6kb4",
                        "prod-4pmewlybdftbs"
                    ]
                }
            }
        }
    ]
}
EOF
)

# =============================================================================
# MAIN FUNCTIONS
# =============================================================================

check_prerequisites() {
    print_header "Checking Prerequisites"

    # Check AWS CLI
    if ! command -v aws &>/dev/null; then
        print_error "AWS CLI not installed"
        exit 1
    fi
    print_success "AWS CLI installed"

    # Check AWS credentials
    if ! aws_cmd sts get-caller-identity &>/dev/null; then
        print_error "AWS credentials not configured or expired"
        echo ""
        echo "  Run: aws sso login --profile ${SSO_PROFILE}"
        echo ""
        echo "  If profile doesn't exist, run bootstrap first:"
        echo "    just bootstrap"
        exit 1
    fi
    print_success "Using SSO profile: ${SSO_PROFILE}"

    AWS_ACCOUNT_ID=$(aws_cmd sts get-caller-identity --query 'Account' --output text)
    print_success "AWS credentials valid (account: ${AWS_ACCOUNT_ID})"
}

create_or_get_policy() {
    print_header "Setting Up Bedrock Policy"

    POLICY_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME}"

    # Check if policy exists
    if aws_cmd iam get-policy --policy-arn "$POLICY_ARN" &>/dev/null; then
        print_step "Policy exists, updating to latest version: ${POLICY_NAME}"

        # Delete old non-default versions (max 5 versions allowed)
        OLD_VERSIONS=$(aws_cmd iam list-policy-versions --policy-arn "$POLICY_ARN" \
            --query 'Versions[?IsDefaultVersion==`false`].VersionId' --output text)
        for ver in $OLD_VERSIONS; do
            aws_cmd iam delete-policy-version --policy-arn "$POLICY_ARN" --version-id "$ver" 2>/dev/null || true
        done

        aws_cmd iam create-policy-version \
            --policy-arn "$POLICY_ARN" \
            --policy-document "$BEDROCK_POLICY" \
            --set-as-default \
            --output text > /dev/null
        print_success "Updated policy: ${POLICY_NAME}"
    else
        print_step "Creating policy: ${POLICY_NAME}"
        aws_cmd iam create-policy \
            --policy-name "$POLICY_NAME" \
            --policy-document "$BEDROCK_POLICY" \
            --description "Allow RoboSystems to invoke Bedrock Claude models" \
            --output text > /dev/null
        print_success "Created policy: ${POLICY_NAME}"
    fi
}

create_or_get_user() {
    print_header "Setting Up IAM User"

    # Check if user exists
    if aws_cmd iam get-user --user-name "$IAM_USER_NAME" &>/dev/null; then
        print_success "User already exists: ${IAM_USER_NAME}"
    else
        print_step "Creating user: ${IAM_USER_NAME}"
        aws_cmd iam create-user \
            --user-name "$IAM_USER_NAME" \
            --tags Key=Purpose,Value=BedrockDevelopment Key=Environment,Value=Development \
            --output text > /dev/null
        print_success "Created user: ${IAM_USER_NAME}"
    fi

    # Attach policy
    print_step "Ensuring policy is attached..."
    aws_cmd iam attach-user-policy \
        --user-name "$IAM_USER_NAME" \
        --policy-arn "$POLICY_ARN" 2>/dev/null || true
    print_success "Policy attached to user"
}

create_access_key() {
    print_header "Setting Up Access Keys"

    # Check existing keys
    EXISTING_KEYS=$(aws_cmd iam list-access-keys --user-name "$IAM_USER_NAME" --query 'AccessKeyMetadata[*].AccessKeyId' --output text)

    if [ -n "$EXISTING_KEYS" ]; then
        print_warning "User already has access key(s): ${EXISTING_KEYS}"
        echo ""
        read -p "Create new key and update .env? (y/N): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_info "Keeping existing keys. Check .env manually if needed."
            return 1
        fi
    fi

    print_step "Creating new access key..."
    KEY_OUTPUT=$(aws_cmd iam create-access-key --user-name "$IAM_USER_NAME" --output json)

    ACCESS_KEY_ID=$(echo "$KEY_OUTPUT" | jq -r '.AccessKey.AccessKeyId')
    SECRET_ACCESS_KEY=$(echo "$KEY_OUTPUT" | jq -r '.AccessKey.SecretAccessKey')

    print_success "Created access key: ${ACCESS_KEY_ID}"

    # Store for later use
    NEW_ACCESS_KEY_ID="$ACCESS_KEY_ID"
    NEW_SECRET_ACCESS_KEY="$SECRET_ACCESS_KEY"
}

update_env_file() {
    print_header "Updating .env File"

    ENV_PATH="${PROJECT_ROOT}/${ENV_FILE}"

    if [ ! -f "$ENV_PATH" ]; then
        print_error ".env file not found at ${ENV_PATH}"
        print_info "Run 'just init' first to create .env from template"
        echo ""
        echo "Manual setup - add these to your .env:"
        echo "  AWS_BEDROCK_ACCESS_KEY_ID=${NEW_ACCESS_KEY_ID}"
        echo "  AWS_BEDROCK_SECRET_ACCESS_KEY=${NEW_SECRET_ACCESS_KEY}"
        return 1
    fi

    print_step "Updating ${ENV_FILE}..."

    # Update or add AWS_BEDROCK_ACCESS_KEY_ID
    if grep -q "^AWS_BEDROCK_ACCESS_KEY_ID=" "$ENV_PATH"; then
        sed -i.bak "s|^AWS_BEDROCK_ACCESS_KEY_ID=.*|AWS_BEDROCK_ACCESS_KEY_ID=${NEW_ACCESS_KEY_ID}|" "$ENV_PATH"
    else
        echo "" >> "$ENV_PATH"
        echo "# Bedrock credentials for local development" >> "$ENV_PATH"
        echo "AWS_BEDROCK_ACCESS_KEY_ID=${NEW_ACCESS_KEY_ID}" >> "$ENV_PATH"
    fi

    # Update or add AWS_BEDROCK_SECRET_ACCESS_KEY
    if grep -q "^AWS_BEDROCK_SECRET_ACCESS_KEY=" "$ENV_PATH"; then
        sed -i.bak "s|^AWS_BEDROCK_SECRET_ACCESS_KEY=.*|AWS_BEDROCK_SECRET_ACCESS_KEY=${NEW_SECRET_ACCESS_KEY}|" "$ENV_PATH"
    else
        echo "AWS_BEDROCK_SECRET_ACCESS_KEY=${NEW_SECRET_ACCESS_KEY}" >> "$ENV_PATH"
    fi

    # Clean up backup file
    rm -f "${ENV_PATH}.bak"

    print_success "Updated ${ENV_FILE} with Bedrock credentials"
}

show_summary() {
    print_header "Setup Complete"

    echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  BEDROCK SETUP COMPLETED${NC}"
    echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "${CYAN}IAM User:${NC} ${IAM_USER_NAME}"
    echo -e "${CYAN}Policy:${NC} ${POLICY_NAME}"
    echo -e "${CYAN}Access Key:${NC} ${NEW_ACCESS_KEY_ID:-existing}"
    echo ""
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  NEXT STEPS${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "1. Restart Docker to pick up new credentials:"
    echo "   just restart"
    echo ""
    echo "2. Test Bedrock access:"
    echo "   just test operations/agents"
    echo ""
}

# =============================================================================
# KEY ROTATION
# =============================================================================

rotate_access_key() {
    print_header "Bedrock Access Key Rotation"

    check_prerequisites

    # Inventory existing keys: "AccessKeyId Status" per line
    KEY_ROWS=$(aws_cmd iam list-access-keys --user-name "$IAM_USER_NAME" \
        --query 'AccessKeyMetadata[*].[AccessKeyId,Status]' --output text)

    if [ -z "$KEY_ROWS" ]; then
        print_error "No access keys exist for ${IAM_USER_NAME}"
        print_info "Run 'just setup-bedrock' for initial setup"
        exit 1
    fi

    KEY_COUNT=$(echo "$KEY_ROWS" | wc -l | tr -d ' ')

    # IAM allows max 2 keys. If a previous rotation left an inactive key,
    # delete it to make room; two active keys means state we didn't create.
    if [ "$KEY_COUNT" -ge 2 ]; then
        INACTIVE_KEY=$(echo "$KEY_ROWS" | awk '$2 == "Inactive" {print $1; exit}')
        if [ -n "$INACTIVE_KEY" ]; then
            print_step "Deleting inactive key from previous rotation: ${INACTIVE_KEY}"
            aws_cmd iam delete-access-key --user-name "$IAM_USER_NAME" --access-key-id "$INACTIVE_KEY"
            print_success "Deleted ${INACTIVE_KEY}"
        else
            print_error "Two ACTIVE keys exist — cannot rotate safely."
            print_info "Determine which key is unused (aws iam get-access-key-last-used),"
            print_info "delete it manually, then re-run: just setup-bedrock rotate"
            exit 1
        fi
    fi

    OLD_KEY_ID=$(aws_cmd iam list-access-keys --user-name "$IAM_USER_NAME" \
        --query 'AccessKeyMetadata[?Status==`Active`].AccessKeyId | [0]' --output text)
    print_info "Rotating out active key: ${OLD_KEY_ID}"

    print_step "Creating replacement access key..."
    KEY_OUTPUT=$(aws_cmd iam create-access-key --user-name "$IAM_USER_NAME" --output json)
    NEW_ACCESS_KEY_ID=$(echo "$KEY_OUTPUT" | jq -r '.AccessKey.AccessKeyId')
    NEW_SECRET_ACCESS_KEY=$(echo "$KEY_OUTPUT" | jq -r '.AccessKey.SecretAccessKey')
    print_success "Created access key: ${NEW_ACCESS_KEY_ID}"

    update_env_file

    # New IAM keys are eventually consistent — verify before touching the old one
    print_step "Verifying new key (IAM propagation can take ~10s)..."
    VERIFIED=false
    for _ in 1 2 3 4 5 6; do
        if AWS_ACCESS_KEY_ID="$NEW_ACCESS_KEY_ID" \
           AWS_SECRET_ACCESS_KEY="$NEW_SECRET_ACCESS_KEY" \
           AWS_SESSION_TOKEN="" AWS_PROFILE="" \
           aws sts get-caller-identity --output text &>/dev/null; then
            VERIFIED=true
            break
        fi
        sleep 5
    done

    if [ "$VERIFIED" != "true" ]; then
        print_error "New key failed verification after 30s — old key left ACTIVE"
        print_info "The .env now holds the new key; investigate before deactivating ${OLD_KEY_ID}"
        exit 1
    fi
    print_success "New key verified"

    print_step "Deactivating old key: ${OLD_KEY_ID}"
    aws_cmd iam update-access-key --user-name "$IAM_USER_NAME" \
        --access-key-id "$OLD_KEY_ID" --status Inactive
    print_success "Old key deactivated (deleted automatically on next rotation)"

    print_header "Rotation Complete"
    echo -e "${CYAN}New Access Key:${NC} ${NEW_ACCESS_KEY_ID}"
    echo -e "${CYAN}Old Key:${NC} ${OLD_KEY_ID} (Inactive)"
    echo ""
    echo "Restart Docker to pick up the new credentials:"
    echo "   just restart"
    echo ""
}

# =============================================================================
# MAIN
# =============================================================================

main() {
    if [[ "${1:-}" == "rotate" ]]; then
        rotate_access_key
        exit 0
    fi

    print_header "Bedrock Local Development Setup"

    echo "This script creates an IAM user with Bedrock access for local Docker development."
    echo ""
    echo "It will:"
    echo "  1. Create IAM user: ${IAM_USER_NAME}"
    echo "  2. Create/attach Bedrock policy"
    echo "  3. Generate access key"
    echo "  4. Update .env with credentials"
    echo ""

    read -p "Continue? (Y/n): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        print_info "Setup cancelled"
        exit 0
    fi

    check_prerequisites
    create_or_get_policy
    create_or_get_user

    if create_access_key; then
        update_env_file
    fi

    show_summary
}

main "$@"
