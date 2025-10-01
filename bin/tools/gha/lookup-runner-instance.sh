#!/bin/bash
set -euo pipefail

# GitHub Actions Runner Instance Lookup
# Usage: ./lookup-runner-instance.sh robosystems-gha-runner-3a2b13-1

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

RUNNER_NAME="${1:-}"

if [ -z "$RUNNER_NAME" ]; then
  echo "Usage: $0 <runner-name>"
  echo "Example: $0 robosystems-gha-runner-3a2b13-1"
  exit 1
fi

# Extract MAC suffix from runner name
MAC_SUFFIX=$(echo "$RUNNER_NAME" | grep -o '[a-f0-9]\{6\}-[0-9]' | cut -d- -f1 || echo "")

if [ -z "$MAC_SUFFIX" ]; then
  print_error "Could not extract MAC suffix from runner name: $RUNNER_NAME"
  echo "Expected format: robosystems-gha-runner-XXXXXX-N"
  exit 1
fi

print_info "Looking up runner: $RUNNER_NAME"
print_info "MAC suffix: $MAC_SUFFIX"
echo ""

# Query AWS for instance with matching MAC
AWS_REGION="${AWS_REGION:-us-east-1}"
print_info "Searching in region: $AWS_REGION"

RESULT=$(aws ec2 describe-instances \
  --filters "Name=network-interface.mac-address,Values=*:*:*:${MAC_SUFFIX:0:2}:${MAC_SUFFIX:2:2}:${MAC_SUFFIX:4:2}" \
  --query 'Reservations[].Instances[].[InstanceId,PrivateIpAddress,NetworkInterfaces[0].MacAddress,State.Name,Placement.AvailabilityZone]' \
  --output table --region "$AWS_REGION" 2>&1)

if [ $? -ne 0 ]; then
  print_error "Failed to query AWS EC2"
  echo "Error: $RESULT"
  exit 1
fi

if echo "$RESULT" | grep -q "i-"; then
  print_success "Found matching instance:"
  echo "$RESULT"
  
  # Extract instance ID for easy copying
  INSTANCE_ID=$(echo "$RESULT" | grep -o 'i-[a-f0-9]\{17\}' | head -1)
  if [ -n "$INSTANCE_ID" ]; then
    echo ""
    print_info "Instance ID: $INSTANCE_ID"
    print_info "Connect via SSM: aws ssm start-session --target $INSTANCE_ID --region $AWS_REGION"
  fi
else
  print_error "No instance found with MAC ending in $MAC_SUFFIX"
  echo "This runner may have been terminated or is in a different region."
fi