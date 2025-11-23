#!/bin/bash
#
# Helper script to get stack configuration from stacks.yml
# Used by GitHub Actions workflows to maintain consistency with application code
#
# Usage:
#   ./get-stack-config.sh <environment> <component> [variant] [output]
#
# Examples:
#   ./get-stack-config.sh prod valkey
#   ./get-stack-config.sh staging ladybug.writers.standard
#   ./get-stack-config.sh prod workers default
#   ./get-stack-config.sh prod valkey - url

set -e

ENVIRONMENT=$1
COMPONENT=$2
VARIANT=${3:-}
OUTPUT=${4:-}

# Map environment names
if [[ "$ENVIRONMENT" == "prod" ]] || [[ "$ENVIRONMENT" == "production" ]]; then
  ENV_KEY="production"
elif [[ "$ENVIRONMENT" == "staging" ]]; then
  ENV_KEY="staging"
else
  echo "Error: Invalid environment. Must be 'prod' or 'staging'" >&2
  exit 1
fi

# Find the config file
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/../../.github/configs/stacks.yml"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Error: Config file not found: $CONFIG_FILE" >&2
  exit 1
fi

# Use Python to parse the YAML (more reliable than yq/jq)
python3 -c "
import yaml
import sys

with open('$CONFIG_FILE', 'r') as f:
    config = yaml.safe_load(f)

env_config = config.get('$ENV_KEY', {})

# Navigate to the component
parts = '$COMPONENT'.split('.')
current = env_config

for part in parts:
    if isinstance(current, dict):
        if '$VARIANT' and '$VARIANT' != '-' and part in current:
            current = current.get(part, {}).get('$VARIANT', {})
        else:
            current = current.get(part, {})
    else:
        sys.exit(1)

# Get the requested field
if '$OUTPUT' and '$OUTPUT' != '-':
    # Get specific output key
    if isinstance(current, dict) and 'outputs' in current:
        outputs = current['outputs']
        if isinstance(outputs, dict):
            print(outputs.get('$OUTPUT', ''))
else:
    # Get stack name
    if isinstance(current, dict) and 'stack_name' in current:
        print(current['stack_name'])
"