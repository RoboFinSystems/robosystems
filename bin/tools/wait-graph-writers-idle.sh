#!/usr/bin/env bash
# Wait until no writer in a graph ASG has an in-flight destructive operation
# (materialization, SEC stage, extensions materialization).
#
# Usage: wait-graph-writers-idle.sh <environment> <region> <asg-name> <max-wait-minutes> [force]
# Exit 0 when idle (or force=true), 1 on timeout.
#
# Reads `active_destructive_ops` per instance from the graph instance registry.
# The instance-side twin is wait_until_idle in
# bin/userdata/common/refresh-graph-container.sh; both must read the counter
# the same way (negative = idle, stale heartbeat = crashed writer, missing row
# = proceed), so change them together.
set -euo pipefail

ENVIRONMENT="$1"
REGION="$2"
ASG_NAME="$3"
MAX_WAIT_ATTEMPTS="$4"
FORCE_IGNORE_BUSY="${5:-false}"
# Counter > 0 with a heartbeat older than 6h is a crashed writer (6h covers
# full SEC backfills).
STALE_WINDOW_SECONDS=21600

if [ "$FORCE_IGNORE_BUSY" = "true" ]; then
  echo "  force-ignore-busy=true — skipping destructive-op wait"
  exit 0
fi

echo "  Checking $ASG_NAME for in-flight destructive operations..."
INSTANCE_IDS=$(aws autoscaling describe-auto-scaling-groups \
  --auto-scaling-group-names "$ASG_NAME" \
  --query 'AutoScalingGroups[0].Instances[].InstanceId' \
  --output text \
  --region "$REGION" 2>/dev/null || echo "")
if [ -z "$INSTANCE_IDS" ] || [ "$INSTANCE_IDS" = "None" ]; then
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

    COUNT=$(echo "$ITEM" | jq -r '.active_destructive_ops.N // "0"')
    LAST_AT=$(echo "$ITEM" | jq -r '.last_destructive_op_at.S // ""')
    KIND=$(echo "$ITEM" | jq -r '.last_destructive_op_kind.S // "unknown"')

    # A counter that went negative (a swallowed increment failure on the
    # writer) is idle, not "still waiting".
    if [ "${COUNT:-0}" -le 0 ] 2>/dev/null; then
      if [ "${COUNT:-0}" -lt 0 ] 2>/dev/null; then
        echo "    ::warning::Negative busy counter on ${INSTANCE_ID} (count=${COUNT}); treating as idle."
      fi
      continue
    fi

    if [ -n "$LAST_AT" ]; then
      LAST_EPOCH=$(date -u -d "${LAST_AT}" +%s 2>/dev/null || echo 0)
      AGE=$(($(date -u +%s) - LAST_EPOCH))
      if [ "$LAST_EPOCH" -gt 0 ] && [ "$AGE" -gt "$STALE_WINDOW_SECONDS" ]; then
        echo "    ::warning::Stale busy counter on ${INSTANCE_ID}: count=${COUNT}, kind=${KIND}, last=${LAST_AT} (${AGE}s ago > ${STALE_WINDOW_SECONDS}s). Treating as crashed."
        continue
      fi
      # An unparseable heartbeat disables stale detection; `date -d` needs GNU coreutils.
      if [ "$LAST_EPOCH" -eq 0 ] && [ "$WAIT_ATTEMPT" -eq 1 ]; then
        echo "    ::warning::Could not parse heartbeat '${LAST_AT}' on ${INSTANCE_ID} — stale-counter detection is inactive for this run"
      fi
    fi

    echo "    ${INSTANCE_ID}: busy (count=${COUNT}, kind=${KIND}, last=${LAST_AT})"
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
