#!/bin/bash
# =============================================================================
# ROBOSYSTEMS SSO PERMISSION SETS
# =============================================================================
#
# Deploys cloudformation/bootstrap-sso.yaml: the RoboSystemsOperator (daily)
# and RoboSystemsAuditor permission sets in IAM Identity Center, assigned in
# this account, then splits the CLI into two profiles on one SSO session:
#
#   robosystems-admin   AdministratorAccess - bootstrap and infrastructure only
#   robosystems-sso     RoboSystemsOperator - everything else; .envrc points here
#
# Runs with the admin profile. Creating the stack asks once; updating it shows
# the change set and asks before applying. Every edit to ~/.aws/config or
# .envrc is asked for too.
#
# USAGE:
#   just bootstrap-sso [admin-profile] [region]
#
# ENVIRONMENT:
#   OPERATOR_USER  Identity Center user name to give the Operator set
#                  ("none" for no assignment); default: the stack's current
#                  assignment, else you
#   AUDITOR_USER   Same for the Auditor set; default: the current assignment,
#                  else none
#
# =============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"
# shellcheck source=bin/setup/profiles.sh
source bin/setup/profiles.sh

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

print_header() { echo -e "\n${BLUE}=== $1 ===${NC}\n"; }
print_step() { echo -e "${CYAN}▶ $1${NC}"; }
print_success() { echo -e "${GREEN}✓ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠ $1${NC}"; }
print_error() { echo -e "${RED}✗ $1${NC}" >&2; }
print_info() { echo -e "  $1"; }

ADMIN_PROFILE="$(resolve_admin_profile "${1:-}")"
OPERATOR_PROFILE="$ROBOSYSTEMS_OPERATOR_PROFILE"
AWS_REGION="${2:-${AWS_REGION:-us-east-1}}"
STACK_NAME="RoboSystemsSSO"
TEMPLATE="cloudformation/bootstrap-sso.yaml"
SERVICE_TAG="RoboSystems"
OPERATOR_SET="${SERVICE_TAG}Operator"
AUDITOR_SET="${SERVICE_TAG}Auditor"

export AWS_PROFILE="$ADMIN_PROFILE"
export AWS_REGION

PARAMS_FILE="$(mktemp -t bootstrap-sso-params.XXXXXX)"
trap 'rm -f "$PARAMS_FILE"' EXIT

# =============================================================================
# IDENTITY
# =============================================================================

login() {
    if aws sts get-caller-identity >/dev/null 2>&1; then
        return 0
    fi
    print_step "Logging in via SSO (profile ${ADMIN_PROFILE})..."
    aws sso login --profile "$ADMIN_PROFILE"
    aws sts get-caller-identity >/dev/null || { print_error "SSO login failed"; exit 1; }
}

load_identity_center() {
    INSTANCE_ARN=$(aws sso-admin list-instances --query 'Instances[0].InstanceArn' --output text 2>/dev/null || echo "")
    IDENTITY_STORE_ID=$(aws sso-admin list-instances --query 'Instances[0].IdentityStoreId' --output text 2>/dev/null || echo "")
    if [ -z "$INSTANCE_ARN" ] || [ "$INSTANCE_ARN" = "None" ]; then
        print_error "No IAM Identity Center instance visible from profile ${ADMIN_PROFILE} in ${AWS_REGION}"
        print_info "Enable Identity Center first (docs/technical/Bootstrap-Guide.md, Fresh AWS Account Setup)"
        exit 1
    fi
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    print_success "Identity Center ${INSTANCE_ARN##*/}, account ${ACCOUNT_ID}"
}

stack_param() {
    aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
        --query "Stacks[0].Parameters[?ParameterKey=='$1'].ParameterValue | [0]" \
        --output text 2>/dev/null | grep -v '^None$' || true
}

user_id_for() {
    aws identitystore get-user-id \
        --identity-store-id "$IDENTITY_STORE_ID" \
        --alternate-identifier "{\"UniqueAttribute\":{\"AttributePath\":\"userName\",\"AttributeValue\":\"$1\"}}" \
        --query UserId --output text 2>/dev/null || echo ""
}

user_name_for() {
    aws identitystore describe-user --identity-store-id "$IDENTITY_STORE_ID" --user-id "$1" \
        --query UserName --output text 2>/dev/null || echo "$1"
}

# Prints the principal ID for a user name, "" for "none"; exits on an unknown name.
resolve_user() {
    local name="$1" id
    if [ -z "$name" ] || [ "$name" = "none" ]; then
        echo ""
        return 0
    fi
    id=$(user_id_for "$name")
    if [ -z "$id" ] || [ "$id" = "None" ]; then
        print_error "No Identity Center user named '${name}'"
        exit 1
    fi
    echo "$id"
}

choose_principals() {
    print_header "Assignments"

    OPERATOR_PRINCIPAL_ID=$(stack_param OperatorPrincipalId)
    OPERATOR_PRINCIPAL_TYPE=$(stack_param OperatorPrincipalType)
    AUDITOR_PRINCIPAL_ID=$(stack_param AuditorPrincipalId)
    AUDITOR_PRINCIPAL_TYPE=$(stack_param AuditorPrincipalType)

    if [ -n "${OPERATOR_USER:-}" ]; then
        OPERATOR_PRINCIPAL_ID=$(resolve_user "$OPERATOR_USER")
        OPERATOR_PRINCIPAL_TYPE=USER
    elif [ -z "$OPERATOR_PRINCIPAL_ID" ] && [ "$STACK_STATUS" = "" ]; then
        # First run: offer the set to whoever is running this. An SSO
        # session's name is the Identity Center user name.
        local me
        me=$(aws sts get-caller-identity --query Arn --output text | awk -F/ '{print $NF}')
        read -r -p "Give ${OPERATOR_SET} to '${me}'? (Y/n, or type another user name): " reply
        case "$reply" in
            ""|[Yy]) OPERATOR_PRINCIPAL_ID=$(resolve_user "$me") ;;
            [Nn]) OPERATOR_PRINCIPAL_ID="" ;;
            *) OPERATOR_PRINCIPAL_ID=$(resolve_user "$reply") ;;
        esac
        OPERATOR_PRINCIPAL_TYPE=USER
    fi

    if [ -n "${AUDITOR_USER:-}" ]; then
        AUDITOR_PRINCIPAL_ID=$(resolve_user "$AUDITOR_USER")
        AUDITOR_PRINCIPAL_TYPE=USER
    fi

    OPERATOR_PRINCIPAL_TYPE="${OPERATOR_PRINCIPAL_TYPE:-USER}"
    AUDITOR_PRINCIPAL_TYPE="${AUDITOR_PRINCIPAL_TYPE:-USER}"

    if [ -n "$OPERATOR_PRINCIPAL_ID" ]; then
        print_info "${OPERATOR_SET}: $(user_name_for "$OPERATOR_PRINCIPAL_ID")"
    else
        print_info "${OPERATOR_SET}: no one"
    fi
    if [ -n "$AUDITOR_PRINCIPAL_ID" ]; then
        print_info "${AUDITOR_SET}:  $(user_name_for "$AUDITOR_PRINCIPAL_ID")"
    else
        print_info "${AUDITOR_SET}:  no one (set AUDITOR_USER=<user name> to assign)"
    fi
}

# The cost review queries the CUR through Athena; grant result writes only
# where the workgroup already sends them.
detect_athena_results() {
    local location
    location=$(aws athena get-work-group --work-group primary \
        --query 'WorkGroup.Configuration.ResultConfiguration.OutputLocation' \
        --output text 2>/dev/null || echo "")
    if [[ "$location" == s3://* ]]; then
        ATHENA_RESULTS="${location#s3://}"
        print_info "Athena results: s3://${ATHENA_RESULTS} (workgroup primary)"
    else
        ATHENA_RESULTS=""
        print_info "Athena results: none configured on workgroup primary (no Athena grant)"
    fi
}

write_params() {
    jq -n \
        --arg instance "$INSTANCE_ARN" \
        --arg service "$SERVICE_TAG" \
        --arg op_id "$OPERATOR_PRINCIPAL_ID" \
        --arg op_type "$OPERATOR_PRINCIPAL_TYPE" \
        --arg au_id "$AUDITOR_PRINCIPAL_ID" \
        --arg au_type "$AUDITOR_PRINCIPAL_TYPE" \
        --arg athena "$ATHENA_RESULTS" \
        '[
          {ParameterKey: "InstanceArn", ParameterValue: $instance},
          {ParameterKey: "ServiceTag", ParameterValue: $service},
          {ParameterKey: "OperatorPrincipalId", ParameterValue: $op_id},
          {ParameterKey: "OperatorPrincipalType", ParameterValue: $op_type},
          {ParameterKey: "AuditorPrincipalId", ParameterValue: $au_id},
          {ParameterKey: "AuditorPrincipalType", ParameterValue: $au_type},
          {ParameterKey: "AthenaResultsLocation", ParameterValue: $athena}
        ]' > "$PARAMS_FILE"
}

# =============================================================================
# STACK
# =============================================================================

create_stack() {
    echo ""
    read -r -p "Create ${STACK_NAME} (permission sets ${OPERATOR_SET} and ${AUDITOR_SET})? (y/N): " -n 1 reply
    echo ""
    if [[ ! $reply =~ ^[Yy]$ ]]; then
        print_info "Nothing created"
        exit 0
    fi
    aws cloudformation create-stack \
        --stack-name "$STACK_NAME" \
        --template-body "file://${TEMPLATE}" \
        --parameters "file://${PARAMS_FILE}" \
        --tags Key=Service,Value=RoboSystems Key=Component,Value=SSO >/dev/null
    print_step "Waiting for stack creation..."
    aws cloudformation wait stack-create-complete --stack-name "$STACK_NAME"
    print_success "${STACK_NAME} created"
}

update_stack() {
    case "$STACK_STATUS" in
        *_IN_PROGRESS)
            print_error "Stack is busy (${STACK_STATUS}); wait for the current operation to finish"
            exit 1 ;;
        UPDATE_ROLLBACK_FAILED|ROLLBACK_COMPLETE|*_FAILED)
            print_error "Stack is ${STACK_STATUS} and cannot be updated from here; inspect it in the console first"
            exit 1 ;;
    esac

    local branch change_set reason
    branch=$(git branch --show-current 2>/dev/null || echo "?")
    if [ "$branch" != "main" ]; then
        print_warning "Not on main: whatever is in this checkout is what gets applied"
    fi

    change_set="bootstrap-$(date +%Y%m%d%H%M%S)"
    print_step "Computing what would change (change set ${change_set})..."
    aws cloudformation create-change-set \
        --stack-name "$STACK_NAME" \
        --change-set-name "$change_set" \
        --change-set-type UPDATE \
        --template-body "file://${TEMPLATE}" \
        --parameters "file://${PARAMS_FILE}" \
        --tags Key=Service,Value=RoboSystems Key=Component,Value=SSO >/dev/null

    if ! aws cloudformation wait change-set-create-complete \
        --stack-name "$STACK_NAME" --change-set-name "$change_set" 2>/dev/null; then
        reason=$(aws cloudformation describe-change-set \
            --stack-name "$STACK_NAME" --change-set-name "$change_set" \
            --query StatusReason --output text 2>/dev/null || echo "")
        aws cloudformation delete-change-set \
            --stack-name "$STACK_NAME" --change-set-name "$change_set" >/dev/null 2>&1 || true
        if echo "$reason" | grep -qi "didn't contain changes\|No updates are to be performed"; then
            print_success "Stack already matches the template; nothing to apply"
            return 0
        fi
        print_error "Could not compute the change set: ${reason:-unknown reason}"
        exit 1
    fi

    echo ""
    echo "Changes the template would make to ${STACK_NAME}:"
    aws cloudformation describe-change-set \
        --stack-name "$STACK_NAME" --change-set-name "$change_set" \
        --query 'Changes[].ResourceChange.[Action, LogicalResourceId, ResourceType, Replacement]' \
        --output text | while IFS=$'\t' read -r action logical rtype replacement; do
            note=""
            [ "$replacement" = "True" ] && note="  (REPLACEMENT)"
            echo "  ${action}  ${logical}  [${rtype}]${note}"
        done
    echo ""

    read -r -p "Apply these changes to ${STACK_NAME}? (y/N): " -n 1 reply
    echo ""
    if [[ ! $reply =~ ^[Yy]$ ]]; then
        aws cloudformation delete-change-set \
            --stack-name "$STACK_NAME" --change-set-name "$change_set" >/dev/null 2>&1 || true
        print_info "Left the stack unchanged"
        return 0
    fi

    aws cloudformation execute-change-set \
        --stack-name "$STACK_NAME" --change-set-name "$change_set" >/dev/null
    print_step "Waiting for stack update..."
    aws cloudformation wait stack-update-complete --stack-name "$STACK_NAME"
    print_success "${STACK_NAME} updated"
}

# =============================================================================
# CLI PROFILES
# =============================================================================

# Copies the SSO settings of one profile into another, with a different role.
copy_profile() {
    local from="$1" to="$2" role="$3" key value
    for key in sso_session sso_start_url sso_region sso_registration_scopes region output; do
        value=$(aws configure get "$key" --profile "$from" 2>/dev/null || true)
        [ -n "$value" ] && aws configure set "$key" "$value" --profile "$to"
    done
    aws configure set sso_account_id "$ACCOUNT_ID" --profile "$to"
    aws configure set sso_role_name "$role" --profile "$to"
}

split_profiles() {
    print_header "CLI Profiles"

    if [ -z "$OPERATOR_PRINCIPAL_ID" ]; then
        print_info "${OPERATOR_SET} is assigned to no one here; leaving profiles alone"
        return 0
    fi

    local admin_role operator_role
    admin_role=$(aws configure get sso_role_name --profile "$ADMIN_PROFILE" 2>/dev/null || echo "AdministratorAccess")
    operator_role=$(aws configure get sso_role_name --profile "$OPERATOR_PROFILE" 2>/dev/null || echo "")

    if [ "$operator_role" = "$OPERATOR_SET" ] && [ "$ADMIN_PROFILE" != "$OPERATOR_PROFILE" ]; then
        print_success "${OPERATOR_PROFILE} -> ${OPERATOR_SET}, ${ADMIN_PROFILE} -> ${admin_role}"
        return 0
    fi

    local new_admin="$ADMIN_PROFILE"
    [ "$ADMIN_PROFILE" = "$OPERATOR_PROFILE" ] && new_admin="robosystems-admin"

    echo "Proposed ~/.aws/config changes (same SSO session, no new login):"
    if [ "$new_admin" != "$ADMIN_PROFILE" ]; then
        print_info "[profile ${new_admin}]  new, role ${admin_role} (copied from ${ADMIN_PROFILE})"
    fi
    if [ -z "$operator_role" ]; then
        print_info "[profile ${OPERATOR_PROFILE}]  new, role ${OPERATOR_SET}"
    else
        print_info "[profile ${OPERATOR_PROFILE}]  role ${operator_role} -> ${OPERATOR_SET}"
    fi
    echo ""
    read -r -p "Write these profiles? (y/N): " -n 1 reply
    echo ""
    if [[ ! $reply =~ ^[Yy]$ ]]; then
        print_info "Profiles unchanged. To do it later: aws configure set sso_role_name ${OPERATOR_SET} --profile ${OPERATOR_PROFILE}"
        return 0
    fi

    if [ "$new_admin" != "$ADMIN_PROFILE" ]; then
        copy_profile "$ADMIN_PROFILE" "$new_admin" "$admin_role"
        print_success "Wrote profile ${new_admin} (${admin_role})"
        ADMIN_PROFILE="$new_admin"
        export AWS_PROFILE="$ADMIN_PROFILE"
    fi
    copy_profile "$ADMIN_PROFILE" "$OPERATOR_PROFILE" "$OPERATOR_SET"
    print_success "Wrote profile ${OPERATOR_PROFILE} (${OPERATOR_SET})"
}

point_envrc_at_operator() {
    [ -f .envrc ] || return 0
    local current
    current=$(grep -E "^export AWS_PROFILE=" .envrc | cut -d'=' -f2 | tr -d "\"'" || true)
    if [ "$current" = "$OPERATOR_PROFILE" ]; then
        return 0
    fi
    [ "$(aws configure get sso_role_name --profile "$OPERATOR_PROFILE" 2>/dev/null || true)" = "$OPERATOR_SET" ] || return 0

    read -r -p "Point .envrc at ${OPERATOR_PROFILE} (now ${current:-unset})? (y/N): " -n 1 reply
    echo ""
    [[ $reply =~ ^[Yy]$ ]] || return 0
    local tmp
    tmp=$(mktemp)
    if [ -n "$current" ]; then
        sed "s|^export AWS_PROFILE=.*|export AWS_PROFILE=${OPERATOR_PROFILE}|" .envrc > "$tmp"
    else
        { cat .envrc; echo "export AWS_PROFILE=${OPERATOR_PROFILE}"; } > "$tmp"
    fi
    mv "$tmp" .envrc
    command -v direnv >/dev/null 2>&1 && direnv allow . 2>/dev/null || true
    print_success ".envrc now uses ${OPERATOR_PROFILE}"
}

# A new assignment takes a little while to reach the account.
verify_operator() {
    [ "$(aws configure get sso_role_name --profile "$OPERATOR_PROFILE" 2>/dev/null || true)" = "$OPERATOR_SET" ] || return 0
    print_step "Checking ${OPERATOR_PROFILE} can sign in as ${OPERATOR_SET}..."
    local _
    for _ in 1 2 3 4 5 6 7 8 9 10 11 12; do
        if aws sts get-caller-identity --profile "$OPERATOR_PROFILE" --query Arn --output text 2>/dev/null | grep -q "_${OPERATOR_SET}_"; then
            print_success "${OPERATOR_PROFILE} works"
            return 0
        fi
        sleep 5
    done
    print_warning "${OPERATOR_PROFILE} could not sign in yet; try: aws sso login --profile ${OPERATOR_PROFILE}"
}

show_summary() {
    print_header "Done"
    echo "  Daily:        ${OPERATOR_PROFILE} (${OPERATOR_SET}); tunnels, pg-mcp, admin CLI, ssm-set"
    echo "  Break-glass:  ${ADMIN_PROFILE}; just bootstrap*, setup-aws, setup-bedrock, serve, migrations"
    echo ""
    echo "  Not managed here, on purpose: the AdministratorAccess permission set."
    echo "  Shorten its session in the console (IAM Identity Center > Permission sets >"
    echo "  AdministratorAccess > Edit, session duration 1 hour) so a forgotten admin"
    echo "  login expires quickly."
    echo ""
}

main() {
    print_header "RoboSystems SSO Permission Sets"
    print_info "Admin profile: ${ADMIN_PROFILE}   Region: ${AWS_REGION}"
    command -v jq >/dev/null 2>&1 || { print_error "jq not installed"; exit 1; }

    login
    load_identity_center

    STACK_STATUS=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
        --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "")

    choose_principals
    detect_athena_results
    write_params

    print_header "Stack ${STACK_NAME}"
    if [ -z "$STACK_STATUS" ]; then
        create_stack
    else
        print_info "Status: ${STACK_STATUS}"
        update_stack
    fi

    split_profiles
    point_envrc_at_operator
    verify_operator
    show_summary
}

main "$@"
