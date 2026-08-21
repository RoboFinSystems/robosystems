#!/bin/bash
# Graph Container Refresh
#
# The instance-side entry point for a container refresh: "bring yourself to the
# current image, safely." Everything it needs is already in /etc/environment, so
# the caller passes nothing but the two optional overrides below. That makes a
# refresh debuggable by hand:
#
#   aws ssm send-command --instance-ids i-xxx \
#     --document-name AWS-RunShellScript \
#     --parameters 'commands=["/usr/local/bin/refresh-graph-container.sh"],executionTimeout=["3600"]'
#
# Optional environment overrides the caller may set on the command:
#   MAX_WAIT_MINUTES   minutes to wait for in-flight destructive ops (default 30)
#   FORCE_IGNORE_BUSY  "true" bypasses the busy-counter wait entirely. Emergency
#                      escape hatch — an interrupted materialization may require a
#                      full graph rebuild.
#   FORCE_RESTART      "true" restarts even when the image digest is unchanged, for
#                      callers whose goal is the restart itself rather than a new
#                      image. Secrets rotation is the case that matters: the
#                      container reads its credentials from Secrets Manager and
#                      caches them in-process, so the process has to be replaced to
#                      pick up rotated ones promptly. Without this the digest-skip
#                      correctly concludes there is nothing to pull, and rotation
#                      propagates only when the cache TTL lapses.
#
# Exits 0 when the container was refreshed OR was already current, non-zero on
# failure. The non-zero exit is load-bearing: SSM records it as `Failed`, which is
# what lets a fleet-wide --max-errors budget halt a bad rollout instead of
# marching a broken image across every customer's database. The one exception is
# exit 3 (below), a benign skip: the fleet-refresh document (graph-infra.yaml
# GraphRefreshDocument) normalizes it to 0 so a skip cannot consume that same
# error budget — from SSM's side, only real failures look like failures. Hand
# runs of this script still see the raw 3.

set -o pipefail

MAX_WAIT_MINUTES="${MAX_WAIT_MINUTES:-30}"
FORCE_IGNORE_BUSY="${FORCE_IGNORE_BUSY:-false}"
FORCE_RESTART="${FORCE_RESTART:-false}"

# Busy counter > 0 but no heartbeat for 6h → treat as crashed. 6h comfortably
# covers even full SEC historical backfills (30-120min); anything longer is
# almost certainly hung.
STALE_WINDOW_SECONDS=21600

# Bumped whenever /etc/environment gains a variable a refresh depends on.
# See require_complete_environment.
REQUIRED_ENV_SCHEMA=2

# Distinct exit code for "this instance predates the environment contract." The
# fleet document maps it to exit 0, and the aggregator classifies the skip off
# the REFRESH_RESULT marker printed alongside it: the instance has not cycled
# since the contract landed, which is a transitional state to be waited out or
# backfilled, not a broken refresh. Any other non-zero exit is a real failure
# and must stay one.
EXIT_STALE_ENV=3

log() { echo "[refresh] $*"; }
die() {
  echo "[refresh] ERROR: $*" >&2
  exit 1
}

# ==================================================================================
# ENVIRONMENT
# ==================================================================================
[ -f /etc/environment ] || die "/etc/environment is missing — cannot reconstitute the container environment"

set -a
# shellcheck disable=SC1091
source /etc/environment
set +a

# A refresh sources only /etc/environment. Any variable the boot exported but did
# not persist there silently falls back to run-graph-container.sh's defaults, and
# the refreshed container then differs from the one this instance booted with —
# drift that surfaces as a missing volume mount rather than an error. Rather than
# guess at per-node-type completeness, trust the schema marker the userdata writes
# once it has persisted the full set.
require_complete_environment() {
  local schema="${GRAPH_ENV_SCHEMA:-0}"
  if ! [ "${schema}" -ge "${REQUIRED_ENV_SCHEMA}" ] 2>/dev/null; then
    log "SKIPPED: /etc/environment predates the complete-environment contract"
    log "  found GRAPH_ENV_SCHEMA=${schema}, need >= ${REQUIRED_ENV_SCHEMA}"
    log "  Refreshing would start a container missing mounts this boot configured,"
    log "  so the running container is being left alone rather than degraded."
    log "  Replace the instance (ASG cycle) so it re-runs its userdata, or backfill"
    log "  the missing variables into /etc/environment by hand."
    echo "REFRESH_RESULT=skipped-stale-env"
    exit "${EXIT_STALE_ENV}"
  fi

  local missing=""
  for var in DATABASE_TYPE NODE_TYPE CONTAINER_PORT ECR_URI ECR_IMAGE_TAG \
    ENVIRONMENT INSTANCE_ID PRIVATE_IP AVAILABILITY_ZONE INSTANCE_TYPE \
    AWS_REGION CLUSTER_TIER; do
    [ -n "${!var:-}" ] || missing="${missing} ${var}"
  done
  [ -z "${missing}" ] || die "/etc/environment is missing required variables:${missing}"
}

require_complete_environment

# The image to converge on comes from /etc/environment, NOT from the environment
# name. They are usually the same moving tag, but the shared replicas can be
# pinned to a specific build tag during a storage-format-breaking engine upgrade
# (SHARED_REPLICA_IMAGE_TAG_{PROD,STAGING} → ECRImageTag), precisely so that a
# boot cannot pull a new engine before the new-format sec.lbug is published.
# Pulling ":${ENVIRONMENT}" here would defeat that pin.
export ECR_IMAGE="${ECR_URI}:${ECR_IMAGE_TAG}"
ECR_REGISTRY="${ECR_URI%/*}"

# run-graph-container.sh owns the NODE_TYPE → container-name mapping. Ask it
# rather than re-deriving, so there is exactly one spelling of that fact.
CONTAINER_NAME=$(/usr/local/bin/run-graph-container.sh --print-container-name) ||
  die "could not determine container name from run-graph-container.sh"
[ -n "${CONTAINER_NAME}" ] || die "run-graph-container.sh returned an empty container name"

log "instance=${INSTANCE_ID} node_type=${NODE_TYPE} container=${CONTAINER_NAME}"
log "target image=${ECR_IMAGE}"

# ==================================================================================
# WAIT FOR IN-FLIGHT DESTRUCTIVE OPS
# ==================================================================================
# This is one of two implementations of the `instance_busy` contract; the other
# is the ASG-wide gate in .github/actions/refresh-graph-asg/action.yml, which asks
# "is ANY instance in this ASG busy?" from a runner before replacing instances —
# a decision an instance cannot make about itself. Neither is redundant, but the
# fail-open rules below must stay identical in both. See that file's note at
# STALE_WINDOW_SECONDS.
#
# This is a coordination signal, NOT a guard. `instance_busy`'s own module logs
# write failures and never raises them, on the principle that a broken counter
# must not block the actual work. Every escape hatch
# below is therefore deliberate and must stay: a negative counter is idle, a
# counter stuck with a stale heartbeat is a crashed writer, and a missing registry
# row proceeds. Do not tighten these into a resource control.
wait_until_idle() {
  if [ "${FORCE_IGNORE_BUSY}" = "true" ]; then
    log "WARNING: FORCE_IGNORE_BUSY=true — bypassing the busy-counter check"
    return 0
  fi

  local table="robosystems-graph-${ENVIRONMENT}-instance-registry"
  log "checking destructive-op counter (table: ${table})"

  local attempt=0 count=0 last_at kind row
  while [ "${attempt}" -lt "${MAX_WAIT_MINUTES}" ]; do
    attempt=$((attempt + 1))

    # One call per poll, and no jq dependency: --query flattens the three fields
    # we need into a single tab-separated row.
    row=$(aws dynamodb get-item \
      --table-name "${table}" \
      --key "{\"instance_id\":{\"S\":\"${INSTANCE_ID}\"}}" \
      --query "Item.[active_destructive_ops.N, last_destructive_op_at.S, last_destructive_op_kind.S]" \
      --output text \
      --region "${AWS_REGION}" 2>/dev/null) || row=""

    # Fail open on a missing row or an unreadable registry: not yet registered,
    # unmanaged, throttled, or a permissions gap — none of which should block a
    # refresh.
    if [ -z "${row}" ] || [ "${row}" = "None" ]; then
      log "no registry entry for ${INSTANCE_ID} — proceeding"
      return 0
    fi

    IFS=$'\t' read -r count last_at kind <<<"${row}"
    [ "${count}" = "None" ] && count=0
    [ "${last_at}" = "None" ] && last_at=""
    [ "${kind}" = "None" ] && kind="unknown"

    if [ "${count:-0}" -le 0 ] 2>/dev/null; then
      if [ "${count:-0}" -lt 0 ] 2>/dev/null; then
        log "WARNING: negative busy counter (count=${count}); treating as idle. May indicate a swallowed increment failure."
      fi
      if [ "${attempt}" -eq 1 ]; then
        log "instance is idle — proceeding"
      else
        log "instance became idle after ${attempt} attempt(s)"
      fi
      return 0
    fi

    if [ -n "${last_at}" ]; then
      local last_epoch now_epoch age
      last_epoch=$(date -u -d "${last_at}" +%s 2>/dev/null || echo 0)
      now_epoch=$(date -u +%s)
      age=$((now_epoch - last_epoch))
      if [ "${last_epoch}" -gt 0 ] && [ "${age}" -gt "${STALE_WINDOW_SECONDS}" ]; then
        log "WARNING: stale busy counter (count=${count}, kind=${kind}, last=${last_at}, ${age}s ago > ${STALE_WINDOW_SECONDS}s). Treating as crashed and proceeding."
        return 0
      fi
      # An unparseable heartbeat disables the crashed-writer escape hatch, so
      # this run can only end in idle or timeout. Say so once rather than let
      # the operator watch 30 minutes of "busy" with no idea why the stale
      # check never fired. `date -d` needs GNU coreutils (present on AL2023).
      if [ "${last_epoch}" -eq 0 ] && [ "${attempt}" -eq 1 ]; then
        log "WARNING: could not parse heartbeat '${last_at}' — stale-counter detection is inactive for this run"
      fi
    fi

    log "attempt ${attempt}/${MAX_WAIT_MINUTES}: busy (count=${count}, kind=${kind}, last=${last_at}). Waiting 60s..."
    sleep 60
  done

  die "timed out after ${MAX_WAIT_MINUTES} minute(s) waiting for the instance to become idle (count=${count}, kind=${kind}). Set FORCE_IGNORE_BUSY=true to override."
}

wait_until_idle

# ==================================================================================
# PULL
# ==================================================================================
# The pull always precedes the stop, so customer-visible downtime is the restart
# and not the download. Any future edit that reorders these is a regression.
log "logging in to ${ECR_REGISTRY}"
aws ecr get-login-password --region "${AWS_REGION}" |
  docker login --username AWS --password-stdin "${ECR_REGISTRY}" >/dev/null ||
  die "ECR login failed"

log "pulling ${ECR_IMAGE}"
docker pull "${ECR_IMAGE}" || die "docker pull failed for ${ECR_IMAGE}"

# ==================================================================================
# DIGEST SKIP
# ==================================================================================
# The graph API image changes far less often than the API/worker images, so most
# fleet refreshes have nothing to do. Skipping the restart when the pulled image
# already matches the running one turns those into no-op pulls instead of a
# customer-visible bounce per instance.
#
# REFRESH_RESULT is printed either way: a run that no-ops on nearly every instance
# has to say so, or "success" becomes indistinguishable from "pulled nothing and
# quietly did nothing."
PULLED_IMAGE_ID=$(docker image inspect --format '{{.Id}}' "${ECR_IMAGE}" 2>/dev/null) || PULLED_IMAGE_ID=""
RUNNING_IMAGE_ID=$(docker inspect --format '{{.Image}}' "${CONTAINER_NAME}" 2>/dev/null) || RUNNING_IMAGE_ID=""
CONTAINER_RUNNING=$(docker inspect --format '{{.State.Running}}' "${CONTAINER_NAME}" 2>/dev/null) || CONTAINER_RUNNING="false"

if [ -n "${PULLED_IMAGE_ID}" ] &&
  [ "${PULLED_IMAGE_ID}" = "${RUNNING_IMAGE_ID}" ] &&
  [ "${CONTAINER_RUNNING}" = "true" ] &&
  [ "${FORCE_RESTART}" != "true" ]; then
  log "already on ${ECR_IMAGE} (${PULLED_IMAGE_ID:0:19}) and running — no restart needed"
  echo "REFRESH_RESULT=no-op"
  exit 0
fi

if [ "${CONTAINER_RUNNING}" != "true" ]; then
  log "container is not running — refreshing regardless of image match"
elif [ "${FORCE_RESTART}" = "true" ] && [ "${PULLED_IMAGE_ID}" = "${RUNNING_IMAGE_ID}" ]; then
  # The restart IS the point here, not the image. Secrets rotation relies on this:
  # credentials are fetched from Secrets Manager and cached in-process, so the
  # process must be replaced for a rotated secret to take effect promptly.
  log "FORCE_RESTART=true — restarting on an unchanged image to drop in-process caches"
fi

# ==================================================================================
# SWAP
# ==================================================================================
# run-graph-container.sh stops and removes the old container, starts the new one,
# and blocks on its own bounded health check, exiting non-zero if the container
# never becomes healthy. That health check is not repeated here — it lives in one
# place, the same place that knows how the container is started.
log "swapping container via run-graph-container.sh"
/usr/local/bin/run-graph-container.sh || die "run-graph-container.sh failed — container did not come up healthy"

# ==================================================================================
# CLEANUP
# ==================================================================================
# Best-effort: a full disk is worth warning about, but a prune failure must not
# turn a healthy refresh into a failed invocation that eats the error budget.
docker image prune -af --filter until=1h >/dev/null 2>&1 ||
  log "WARNING: docker image prune failed (non-fatal)"

log "refresh complete on ${INSTANCE_ID}"
echo "REFRESH_RESULT=updated"
exit 0
