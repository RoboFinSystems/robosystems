#!/bin/bash
set -euo pipefail

# GitHub Actions Runner Cleanup Script
# Removes offline runners that have been offline for more than X hours

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

# Configuration with defaults
GITHUB_TOKEN="${GITHUB_TOKEN:-}"
GITHUB_ORG="${GITHUB_ORG:-YourGitHubOrg}"
OFFLINE_HOURS_THRESHOLD="${OFFLINE_HOURS_THRESHOLD:-1}"

# Validate prerequisites
check_dependencies() {
    local missing_deps=()

    # Check for required commands
    for cmd in curl jq date; do
        if ! command -v "$cmd" &> /dev/null; then
            missing_deps+=("$cmd")
        fi
    done

    if [ ${#missing_deps[@]} -ne 0 ]; then
        print_error "Missing required dependencies: ${missing_deps[*]}"
        exit 1
    fi
}

# Check dependencies
check_dependencies

# Validate GitHub token
if [ -z "$GITHUB_TOKEN" ]; then
  print_error "GITHUB_TOKEN environment variable is required"
  echo "Usage: GITHUB_TOKEN=your_token $0"
  exit 1
fi

# Validate hours threshold
if ! [[ "$OFFLINE_HOURS_THRESHOLD" =~ ^[0-9]+$ ]]; then
    print_error "OFFLINE_HOURS_THRESHOLD must be a positive integer"
    exit 1
fi

print_info "Cleaning up offline GitHub Actions runners"
print_info "Removing runners offline for more than $OFFLINE_HOURS_THRESHOLD hours"

# Get all runners
RUNNERS_RESPONSE=$(curl -s -H "Authorization: token $GITHUB_TOKEN" \
  -H "Accept: application/vnd.github.v3+json" \
  "https://api.github.com/orgs/$GITHUB_ORG/actions/runners")

# Check if response is valid JSON
if ! echo "$RUNNERS_RESPONSE" | jq . >/dev/null 2>&1; then
  print_error "Failed to get runners from GitHub API"
  echo "Response: $RUNNERS_RESPONSE"
  exit 1
fi

# Check for API errors
if echo "$RUNNERS_RESPONSE" | jq -e '.message' >/dev/null 2>&1; then
    ERROR_MSG=$(echo "$RUNNERS_RESPONSE" | jq -r '.message')
    print_error "GitHub API error: $ERROR_MSG"
    exit 1
fi

# Calculate threshold timestamp (current time - offline hours)
THRESHOLD_TIMESTAMP=$(date -d "$OFFLINE_HOURS_THRESHOLD hours ago" +%s 2>/dev/null || \
  date -r $(($(date +%s) - OFFLINE_HOURS_THRESHOLD * 3600)) +%s 2>/dev/null || \
  echo $(($(date +%s) - OFFLINE_HOURS_THRESHOLD * 3600)))

# Find offline runners older than threshold
# Handle null status_updated_at by using a very old timestamp for comparison
OFFLINE_RUNNERS=$(echo "$RUNNERS_RESPONSE" | jq -r --arg threshold "$THRESHOLD_TIMESTAMP" '
  .runners[] |
  select(.status == "offline") |
  select(
    if .status_updated_at == null then
      true
    else
      (.status_updated_at | strptime("%Y-%m-%dT%H:%M:%SZ") | mktime) < ($threshold | tonumber)
    end
  ) |
  "\(.id) \(.name) \(.status_updated_at // "unknown")"
')

if [ -z "$OFFLINE_RUNNERS" ]; then
  print_success "No offline runners found that meet the cleanup criteria"
  exit 0
fi

OFFLINE_COUNT=$(echo "$OFFLINE_RUNNERS" | wc -l | tr -d ' ')
print_info "Found $OFFLINE_COUNT offline runners to clean up"
echo ""

# Track cleanup results
DELETED_COUNT=0
FAILED_COUNT=0

# Use process substitution instead of pipe to avoid subshell
while IFS= read -r line; do
  runner_id=$(echo "$line" | cut -d' ' -f1)
  runner_name=$(echo "$line" | cut -d' ' -f2)
  status_updated=$(echo "$line" | cut -d' ' -f3-)

  print_info "Deleting runner (offline since: $status_updated)"

  DELETE_RESPONSE=$(curl -s -w "\nHTTP_CODE:%{http_code}" -X DELETE \
    -H "Authorization: token $GITHUB_TOKEN" \
    -H "Accept: application/vnd.github.v3+json" \
    "https://api.github.com/orgs/$GITHUB_ORG/actions/runners/$runner_id")

  HTTP_CODE=$(echo "$DELETE_RESPONSE" | grep -o "HTTP_CODE:[0-9]*" | cut -d: -f2)
  BODY=$(echo "$DELETE_RESPONSE" | sed '$d')  # Remove last line with HTTP code

  if [ "$HTTP_CODE" = "204" ]; then
    print_success "Successfully deleted runner"
    DELETED_COUNT=$((DELETED_COUNT + 1))
  else
    print_error "Failed to delete runner (HTTP $HTTP_CODE)"
    if [ -n "$BODY" ]; then
        echo "Error details: $BODY"
    fi
    FAILED_COUNT=$((FAILED_COUNT + 1))
  fi
done < <(echo "$OFFLINE_RUNNERS")

echo ""
if [ $DELETED_COUNT -gt 0 ] || [ $FAILED_COUNT -gt 0 ]; then
    print_info "Cleanup summary: $DELETED_COUNT deleted, $FAILED_COUNT failed"
fi
print_success "Cleanup completed!"

# Show current runner status
echo ""
print_info "Current runner status:"
CURRENT_RUNNERS=$(curl -s -H "Authorization: token $GITHUB_TOKEN" \
  -H "Accept: application/vnd.github.v3+json" \
  "https://api.github.com/orgs/$GITHUB_ORG/actions/runners")

if echo "$CURRENT_RUNNERS" | jq -e '.runners' >/dev/null 2>&1; then
    TOTAL_RUNNERS=$(echo "$CURRENT_RUNNERS" | jq -r '.runners | length')
    ONLINE_RUNNERS=$(echo "$CURRENT_RUNNERS" | jq -r '.runners | map(select(.status == "online")) | length')
    OFFLINE_RUNNERS=$(echo "$CURRENT_RUNNERS" | jq -r '.runners | map(select(.status == "offline")) | length')
    echo "Total runners: $TOTAL_RUNNERS"
    echo "Online: $ONLINE_RUNNERS"
    echo "Offline: $OFFLINE_RUNNERS"
else
    print_error "Failed to get current runner status"
fi
