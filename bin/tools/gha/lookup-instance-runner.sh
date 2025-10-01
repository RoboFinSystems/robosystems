#!/bin/bash
set -euo pipefail

# GitHub Actions Runner Reverse Lookup
# Usage: ./lookup-instance-runner.sh <instance-id> [--org GITHUB_ORG]
# Example: ./lookup-instance-runner.sh i-0ffe7a6b48a7f89ef --org MyOrg

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

# Check dependencies
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI is not installed"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    print_error "AWS credentials not configured or expired"
    exit 1
fi

# Parse arguments
INSTANCE_ID=""
GITHUB_ORG="${GITHUB_ORG:-RoboFinSystems}"

while [[ $# -gt 0 ]]; do
  case $1 in
    --org)
      GITHUB_ORG="$2"
      shift 2
      ;;
    -*)
      print_error "Unknown option: $1"
      exit 1
      ;;
    *)
      INSTANCE_ID="$1"
      shift
      ;;
  esac
done

# Validate instance ID format
if [ -z "$INSTANCE_ID" ]; then
  echo "Usage: $0 <instance-id> [--org GITHUB_ORG]"
  echo "Example: $0 i-0ffe7a6b48a7f89ef --org MyOrg"
  echo ""
  echo "Options:"
  echo "  --org GITHUB_ORG    GitHub organization name (can also use GITHUB_ORG env var)"
  exit 1
fi

if ! [[ "$INSTANCE_ID" =~ ^i-[a-f0-9]{17}$ ]]; then
  print_error "Invalid instance ID format: $INSTANCE_ID"
  echo "Expected format: i-XXXXXXXXXXXXXXXXX"
  exit 1
fi

print_info "Looking up runner for instance: $INSTANCE_ID"
echo ""

# Get MAC address for the instance
AWS_REGION="${AWS_REGION:-us-east-1}"
MAC_ADDRESS=$(aws ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[].Instances[].NetworkInterfaces[0].MacAddress' \
  --output text --region "$AWS_REGION" 2>&1)

if [ $? -ne 0 ]; then
  print_error "Failed to query instance details"
  echo "Error: $MAC_ADDRESS"
  exit 1
fi

if [ -z "$MAC_ADDRESS" ] || [ "$MAC_ADDRESS" = "None" ]; then
  print_error "Instance not found or has no network interface: $INSTANCE_ID"
  echo "The instance may be terminated or in a different region."
  exit 1
fi

# Extract MAC suffix (last 6 characters without colons)
MAC_SUFFIX=$(echo "$MAC_ADDRESS" | tr -d ':' | tail -c 7)

# Construct expected runner name
EXPECTED_RUNNER_NAME="robosystems-gha-runner-$MAC_SUFFIX-1"

print_info "Instance MAC address: $MAC_ADDRESS"
print_info "Expected runner name: $EXPECTED_RUNNER_NAME"
echo ""

# Verify runner exists in GitHub (if GITHUB_TOKEN and GITHUB_ORG are available)
if [ -n "${GITHUB_TOKEN:-}" ] && [ -n "$GITHUB_ORG" ]; then
  print_info "Checking GitHub for runner in organization: $GITHUB_ORG"

  # Check for jq dependency
  if ! command -v jq &> /dev/null; then
    print_warning "jq is not installed - skipping GitHub runner verification"
  else
    RUNNER_STATUS=$(curl -s -H "Authorization: token $GITHUB_TOKEN" \
      -H "Accept: application/vnd.github.v3+json" \
      "https://api.github.com/orgs/$GITHUB_ORG/actions/runners" | \
      jq -r --arg name "$EXPECTED_RUNNER_NAME" \
      '.runners[] | select(.name == $name) | "\(.name): \(.status)"' 2>/dev/null || echo "")

    if [ -n "$RUNNER_STATUS" ]; then
      print_success "Found runner in GitHub: $RUNNER_STATUS"
    else
      print_warning "Runner not found in GitHub (may be offline or not yet registered)"
    fi
  fi
elif [ -n "${GITHUB_TOKEN:-}" ]; then
  print_warning "GITHUB_ORG not set - skipping GitHub runner verification"
  print_info "Use --org flag or set GITHUB_ORG environment variable"
else
  print_info "Set GITHUB_TOKEN and GITHUB_ORG to verify runner status in GitHub"
fi

# Show instance details
echo ""
print_info "Instance details:"
INSTANCE_DETAILS=$(aws ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[].Instances[].[InstanceId,PrivateIpAddress,State.Name,Placement.AvailabilityZone,InstanceType]' \
  --output table --region "$AWS_REGION" 2>&1)

if [ $? -eq 0 ]; then
  echo "$INSTANCE_DETAILS"

  # Provide SSM connection command
  echo ""
  print_info "Connect via SSM: aws ssm start-session --target $INSTANCE_ID --region $AWS_REGION"
else
  print_error "Failed to get instance details"
  echo "Error: $INSTANCE_DETAILS"
fi
