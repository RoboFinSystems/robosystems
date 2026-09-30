#!/usr/bin/env bash
# Wait until no writer in a graph ASG has an in-flight destructive operation
# (materialization, SEC stage, extensions materialization).
#
# Usage: wait-graph-writers-idle.sh <environment> <region> <asg-name> <max-wait-minutes> [force]
# Exit 0 when idle (or force=true), 1 on timeout, 2 when the ASG can't be read.
#
# Reads the `active_leases` set (`<started_at>|<op_kind>|<id>` tokens) per
# instance from the graph instance registry. The instance-side twin is
# wait_until_idle in bin/userdata/common/refresh-graph-container.sh, and the
# fleet refresh walk reads it too (_busy in bin/lambda/graph_container_refresh.py);
# all three must read the leases the same way (no leases = idle, a stale or
# unparseable lease = crashed holder, missing row = proceed), so change them
# together.
set -euo pipefail

ENVIRONMENT="$1"
REGION="$2"
ASG_NAME="$3"
MAX_WAIT_ATTEMPTS="$4"
FORCE_IGNORE_BUSY="${5:-false}"
# A lease older than 6h is a crashed holder (6h covers full SEC backfills).
STALE_WINDOW_SECONDS=21600

if [ "$FORCE_IGNORE_BUSY" = "true" ]; then
  echo "  force-ignore-busy=true — skipping destructive-op wait"
  exit 0
fi

# Fail closed: an unknown ASG or an AWS error is not "nothing to wait for".
if [ -z "$ASG_NAME" ] || [ "$ASG_NAME" = "None" ]; then
  echo "::error::No ASG name to drain"
  exit 2
fi
echo "  Checking $ASG_NAME for in-flight destructive operations..."
if ! ASG_JSON=$(aws autoscaling describe-auto-scaling-groups \
    --auto-scaling-group-names "$ASG_NAME" --output json --region "$REGION"); then
  echo "::error::Could not describe ASG $ASG_NAME"
  exit 2
fi
if [ "$(echo "$ASG_JSON" | jq '.AutoScalingGroups | length')" -eq 0 ]; then
  echo "::error::ASG $ASG_NAME not found"
  exit 2
fi
INSTANCE_IDS=$(echo "$ASG_JSON" | jq -r '.AutoScalingGroups[0].Instances[].InstanceId')
if [ -z "$INSTANCE_IDS" ]; then
  echo "  No instances in $ASG_NAME — nothing to wait for"
  exit 0
fi

WAIT_ATTEMPT=0
TOTAL_BUSY=0
while [ "$WAIT_ATTEMPT" -lt "$MAX_WAIT_ATTEMPTS" ]; do
  WAIT_ATTEMPT=$((WAIT_ATTEMPT + 1))
  TOTAL_BUSY=0

  for INSTANCE_ID in $INSTANCE_IDS; do
    ITEM=$(aws dynamodb get-item \
      --table-name "robosystems-graph-${ENVIRONMENT}-instance-registry" \
      --key "{\"instance_id\":{\"S\":\"${INSTANCE_ID}\"}}" \
      --query "Item" \
      --output json \
      --region "$REGION" 2>/dev/null || echo "null")
    if [ "$ITEM" = "null" ] || [ -z "$ITEM" ]; then
      continue
    fi

    COUNT=0
    NOW=$(date -u +%s)
    # `date -d` needs GNU coreutils; an unparseable lease reads as stale.
    while IFS= read -r LEASE; do
      [ -z "$LEASE" ] && continue
      LEASE_EPOCH=$(date -u -d "${LEASE%%|*}" +%s 2>/dev/null || echo 0)
      if [ "$LEASE_EPOCH" -le 0 ] || [ $((NOW - LEASE_EPOCH)) -gt "$STALE_WINDOW_SECONDS" ]; then
        if [ "$WAIT_ATTEMPT" -eq 1 ]; then
          echo "    ::warning::Stale busy lease on ${INSTANCE_ID}: ${LEASE}. Treating its holder as crashed."
        fi
        continue
      fi
      COUNT=$((COUNT + 1))
      echo "    ${INSTANCE_ID}: busy (${LEASE})"
    done < <(echo "$ITEM" | jq -r '.active_leases.SS // [] | .[]')

    TOTAL_BUSY=$((TOTAL_BUSY + COUNT))
  done

  if [ "${TOTAL_BUSY:-0}" -le 0 ] 2>/dev/null; then
    echo "  All instances idle after ${WAIT_ATTEMPT} attempt(s)"
    exit 0
  fi

  echo "  Attempt ${WAIT_ATTEMPT}/${MAX_WAIT_ATTEMPTS}: ${TOTAL_BUSY} in-flight op(s). Waiting 60s..."
  sleep 60
done

echo "::error::Timed out after ${MAX_WAIT_ATTEMPTS} minute(s) waiting for ASG $ASG_NAME to become idle. Use force-ignore-busy=true to override."
exit 1
