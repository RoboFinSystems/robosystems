#!/usr/bin/env bash

# Deploy current branch or tag to specified environment for RoboSystems Service
# Usage: ./bin/tools/deploy staging|prod [branch_or_tag]

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_step() {
    echo -e "${PURPLE}🚀 $1${NC}"
}

# Function to show usage
show_usage() {
    echo "Usage: ./bin/tools/deploy <environment> [branch_or_tag]"
    echo ""
    echo "Arguments:"
    echo "  environment      Target environment (staging|prod)"
    echo "  branch_or_tag    Optional: specific branch or tag to deploy (defaults to current)"
    echo ""
    echo "Examples:"
    echo "  ./bin/tools/deploy staging           # Deploy current branch/tag to staging"
    echo "  ./bin/tools/deploy prod              # Deploy current branch/tag to production"
    echo "  ./bin/tools/deploy staging v1.2.3    # Deploy specific tag to staging"
    echo "  ./bin/tools/deploy prod main         # Deploy main branch to production"
    echo ""
}

# Check if environment argument is provided
if [ $# -eq 0 ]; then
    print_error "Environment argument is required"
    show_usage
    exit 1
fi

ENVIRONMENT="$1"
BRANCH_OR_TAG="${2:-}"  # Optional second argument

# Validate environment argument
case "$ENVIRONMENT" in
    staging|prod)
        ;;
    *)
        print_error "Invalid environment: $ENVIRONMENT"
        echo "Valid environments: staging, prod"
        show_usage
        exit 1
        ;;
esac

# Set environment-specific variables
case "$ENVIRONMENT" in
    staging)
        WORKFLOW_NAME="Staging Deployment"
        WORKFLOW_FILE="staging.yml"
        TARGET_URL="https://staging.api.robosystems.ai"
        ;;
    prod)
        WORKFLOW_NAME="Production Deployment"
        WORKFLOW_FILE="prod.yml"
        TARGET_URL="https://api.robosystems.ai"
        ;;
esac

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    print_error "pyproject.toml not found. Are you in the root of the Python project?"
    exit 1
fi

# Check if workflow file exists
if [ ! -f ".github/workflows/${WORKFLOW_FILE}" ]; then
    print_error "Workflow file ${WORKFLOW_FILE} not found"
    exit 1
fi

# Check if gh CLI is installed
if ! command -v gh &> /dev/null; then
    print_error "GitHub CLI (gh) is not installed. Please install it first:"
    echo "  brew install gh"
    echo "  or visit: https://cli.github.com/"
    exit 1
fi

# Check if gh is authenticated
if ! gh auth status &> /dev/null; then
    print_error "GitHub CLI is not authenticated. Please run:"
    echo "  gh auth login"
    exit 1
fi

# Determine what to deploy (branch or tag)
if [ -n "$BRANCH_OR_TAG" ]; then
    # User specified a branch or tag
    DEPLOY_REF="$BRANCH_OR_TAG"

    # Check if it's a valid git reference
    if ! git rev-parse --verify "$DEPLOY_REF" >/dev/null 2>&1; then
        print_error "Invalid branch or tag: $DEPLOY_REF"
        exit 1
    fi

    # Determine if it's a tag or branch for display
    if git show-ref --tags "refs/tags/$DEPLOY_REF" >/dev/null 2>&1; then
        REF_TYPE="tag"
    else
        REF_TYPE="branch"
    fi
else
    # Auto-detect current branch or tag
    CURRENT_BRANCH=$(git branch --show-current)

    if [ -n "$CURRENT_BRANCH" ]; then
        # We're on a branch
        DEPLOY_REF="$CURRENT_BRANCH"
        REF_TYPE="branch"
    else
        # We might be on a detached HEAD (tag)
        CURRENT_TAG=$(git describe --exact-match --tags HEAD 2>/dev/null || echo "")
        if [ -n "$CURRENT_TAG" ]; then
            DEPLOY_REF="$CURRENT_TAG"
            REF_TYPE="tag"
        else
            print_error "Could not determine current branch or tag. Please specify one as an argument."
            exit 1
        fi
    fi
fi

print_info "Deploying $REF_TYPE: $DEPLOY_REF to $ENVIRONMENT"

# Trigger deployment workflow
if gh workflow run "${WORKFLOW_FILE}" --ref "${DEPLOY_REF}" >/dev/null 2>&1; then
    print_success "Deployed ${REF_TYPE} ${DEPLOY_REF} to ${ENVIRONMENT}"

    # Get repository information for URL
    REPO_INFO=$(gh repo view --json owner,name -q '.owner.login + "/" + .name')

    echo
    echo "View runs: https://github.com/${REPO_INFO}/actions/workflows/${WORKFLOW_FILE}"
    echo "URL: ${TARGET_URL}"
else
    print_error "Failed to trigger workflow"
    exit 1
fi
