"""
Refresh graph API containers across the fleet with one tag-targeted SSM command.

The per-instance work — wait for in-flight destructive ops, pull, skip on an
unchanged digest, swap, health-check, prune — lives on the instance in
`/usr/local/bin/refresh-graph-container.sh`. This function is the *trigger and
the aggregator, not the worker*: it dispatches one `AWS-RunShellScript` command
at a tag expression and reports back. No instance enumeration, no per-instance
job, no runner sleeping through a busy-wait.

Why that matters: `databases_per_instance: 1` on every tier means customer count
is instance count, so the enumerate-into-a-matrix approach this replaces hit a
hard 256-job ceiling and paid runner-minutes to poll. SSM's own queueing ramps
1 -> 2 -> 4 -> ... to the concurrency cap, which is a free canary, and
`--max-errors` halts a bad rollout instead of marching it across every
customer's database.

Targeting is per node-type group rather than one unified expression. SSM ANDs
across target keys and ORs within a key, so "writers by LadybugRole OR replicas
by NodeType" cannot be one command. Splitting them is not a workaround: a
replica restart is far less customer-visible than a writer's, so they want
different rate control anyway.

Dispatch is by `action` on the event; see `handler`.
"""

import logging
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")

ENVIRONMENT = os.environ["ENVIRONMENT"]
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")

REFRESH_SCRIPT = "/usr/local/bin/refresh-graph-container.sh"

# Rate control defaults live here, not only in the caller, so a hand-rolled
# invocation cannot accidentally hit the fleet at SSM's default concurrency of
# 50. Deliberately conservative on concurrency: the ECR interface endpoints make
# a pull storm a throughput question rather than a NAT-cost one, but the fleet
# has never been pulled at high concurrency, so this should be raised on
# evidence rather than optimism.
DEFAULT_MAX_CONCURRENCY = "10%"
DEFAULT_MAX_ERRORS = "10%"

# The busy-wait ceiling the instance script honors. executionTimeout must always
# exceed it, and is set explicitly rather than left to AWS-RunShellScript's
# 3600s default so that raising the wait cannot silently truncate the command.
DEFAULT_MAX_WAIT_MINUTES = 30
REFRESH_HEADROOM_SECONDS = 900

# Node-type groups, each resolving to its own tag expression. `writer` covers
# every writer tier — the launch template tags them all `LadybugRole=writer` and
# distinguishes the tier with `WriterTier`.
#
# Replicas are matched on `NodeType`, which they have always carried, rather than
# on `LadybugRole=replica`, which was added later and only reaches an instance
# when it next boots. Using the older tag means this works during that rollout
# instead of silently selecting nothing.
NODE_TYPE_TARGETS: dict[str, list[dict[str, Any]]] = {
  "writer": [{"Key": "tag:LadybugRole", "Values": ["writer"]}],
  "shared": [
    {"Key": "tag:LadybugRole", "Values": ["writer"]},
    {"Key": "tag:WriterTier", "Values": ["shared"]},
  ],
  "shared-replicas": [{"Key": "tag:NodeType", "Values": ["shared_replica"]}],
}

# What `all` expands to. Two commands, not one, per the module docstring.
ALL_GROUPS = ["writer", "shared-replicas"]

TERMINAL_STATUSES = {"Success", "Failed", "Cancelled", "TimedOut", "Cancelling"}

# Exit codes that mean "this instance has not picked up the new deployment yet"
# rather than "the refresh broke." SSM records any non-zero exit as `Failed`, so
# without this classification a fleet that has not cycled onto the new scripts
# fails the caller — which is the opposite of the intent, since the container was
# deliberately left untouched.
#
#   3 — script present, /etc/environment predates the completeness contract
#   4 — script not on the instance at all (installed at boot)
#
# These are matched on the script's own explicit exit codes, never inferred from a
# generic 127: a missing `docker` inside the script also exits 127, and that is a
# real failure that must not be downgraded.
SKIP_RESPONSE_CODES = {3: "skipped-stale-env", 4: "skipped-no-script"}


def _targets_for(group: str, environment: str) -> list[dict[str, Any]]:
  """Build the SSM target expression for a node-type group."""
  if group not in NODE_TYPE_TARGETS:
    raise ValueError(
      f"Unknown node_type '{group}'; expected one of "
      f"{sorted([*NODE_TYPE_TARGETS, 'all'])}"
    )
  return [
    {"Key": "tag:Environment", "Values": [environment]},
    *NODE_TYPE_TARGETS[group],
  ]


def _build_commands(
  max_wait_minutes: int, force_ignore_busy: bool, force_restart: bool
) -> list[str]:
  """The two shell lines the fleet runs.

  The first is a presence guard with its own exit code so that "this instance
  has not picked up the script yet" is reported as itself. It must NOT be
  inferred from bash's 127 — a missing `docker` inside the script exits 127 too,
  and that is a real failure that must not be downgraded to a benign skip.
  """
  env_prefix = (
    f"MAX_WAIT_MINUTES={max_wait_minutes} "
    f"FORCE_IGNORE_BUSY={'true' if force_ignore_busy else 'false'} "
    f"FORCE_RESTART={'true' if force_restart else 'false'}"
  )
  return [
    f"[ -x {REFRESH_SCRIPT} ] || {{ echo REFRESH_RESULT=skipped-no-script; exit 4; }}",
    f"{env_prefix} {REFRESH_SCRIPT}",
  ]


def start(event: dict[str, Any]) -> dict[str, Any]:
  """Dispatch the refresh and return the command ids, one per node-type group.

  Returns immediately — the fleet keeps working after this returns, which is the
  point. Poll `status` or let the EventBridge rule page on failures.
  """
  environment = event.get("environment", ENVIRONMENT)
  node_type = event.get("node_types", "writer")
  max_concurrency = event.get("max_concurrency", DEFAULT_MAX_CONCURRENCY)
  max_errors = event.get("max_errors", DEFAULT_MAX_ERRORS)
  max_wait_minutes = int(event.get("max_wait_minutes", DEFAULT_MAX_WAIT_MINUTES))
  force_ignore_busy = bool(event.get("force_ignore_busy", False))
  force_restart = bool(event.get("force_restart", False))

  groups = ALL_GROUPS if node_type == "all" else [node_type]
  execution_timeout = max_wait_minutes * 60 + REFRESH_HEADROOM_SECONDS
  commands = _build_commands(max_wait_minutes, force_ignore_busy, force_restart)

  dispatched: list[dict[str, Any]] = []
  for group in groups:
    targets = _targets_for(group, environment)
    try:
      response = ssm.send_command(
        DocumentName="AWS-RunShellScript",
        Targets=targets,
        Comment=f"graph container refresh ({environment}/{group})"[:100],
        Parameters={
          "commands": commands,
          "executionTimeout": [str(execution_timeout)],
        },
        MaxConcurrency=max_concurrency,
        MaxErrors=max_errors,
        TimeoutSeconds=3600,
      )
    except ClientError as e:
      # A group that matches zero instances is not an error worth failing on —
      # staging routinely runs with no graph fleet at all.
      if e.response.get("Error", {}).get("Code") == "InvalidInstanceId":
        logger.info(f"No instances matched {group} in {environment}; skipping")
        dispatched.append({"node_type": group, "command_id": None, "matched": 0})
        continue
      raise

    command_id = response["Command"]["CommandId"]
    logger.info(
      f"Dispatched {group} refresh in {environment}: command={command_id} "
      f"concurrency={max_concurrency} max_errors={max_errors} "
      f"force_restart={force_restart}"
    )
    dispatched.append(
      {
        "node_type": group,
        "command_id": command_id,
        "targets": targets,
        "max_concurrency": max_concurrency,
        "max_errors": max_errors,
      }
    )

  return {
    "statusCode": 200,
    "environment": environment,
    "node_types": node_type,
    "force_restart": force_restart,
    "execution_timeout_seconds": execution_timeout,
    "commands": dispatched,
  }


def status(event: dict[str, Any]) -> dict[str, Any]:
  """Aggregate invocations for one or more command ids into counts by status.

  Also breaks out the instance-side `REFRESH_RESULT` so a caller can tell a
  fleet that genuinely restarted from one that no-op'd on an unchanged digest,
  or skipped because it has not cycled onto the new scripts yet. Without that,
  "success" and "did nothing" look identical.
  """
  command_ids = event.get("command_ids") or (
    [event["command_id"]] if event.get("command_id") else []
  )
  if not command_ids:
    return {"statusCode": 400, "error": "command_ids or command_id is required"}

  overall: dict[str, int] = {}
  results: dict[str, int] = {}
  per_command: list[dict[str, Any]] = []
  failures: list[dict[str, str]] = []
  skipped = 0

  for command_id in command_ids:
    counts: dict[str, int] = {}
    paginator = ssm.get_paginator("list_command_invocations")
    for page in paginator.paginate(CommandId=command_id, Details=True):
      for inv in page.get("CommandInvocations", []):
        inv_status = inv.get("Status", "Unknown")
        counts[inv_status] = counts.get(inv_status, 0) + 1
        overall[inv_status] = overall.get(inv_status, 0) + 1

        response_code = inv.get("ResponseCode")
        skip_reason = SKIP_RESPONSE_CODES.get(response_code) if response_code else None

        outcome = _refresh_result(inv) or skip_reason
        if outcome:
          results[outcome] = results.get(outcome, 0) + 1

        if inv_status in {"Failed", "TimedOut"}:
          if skip_reason:
            # Deliberately not a failure: the container was left running its
            # current image on purpose.
            skipped += 1
            continue
          failures.append(
            {
              "instance_id": inv.get("InstanceId", "unknown"),
              "status": inv_status,
              "response_code": str(response_code if response_code is not None else ""),
            }
          )

    per_command.append({"command_id": command_id, "counts": counts})

  pending = sum(n for s, n in overall.items() if s not in TERMINAL_STATUSES)
  return {
    "statusCode": 200,
    "complete": pending == 0,
    "counts": overall,
    "refresh_results": results,
    # `failed` excludes the skip codes, so a caller can fail on it directly. A
    # fleet that has not cycled yet must not fail a deploy — but it must still be
    # visible, which is what `skipped` and `refresh_results` are for.
    "failed": len(failures),
    "skipped": skipped,
    "failures": failures[:50],
    "per_command": per_command,
  }


def _refresh_result(invocation: dict[str, Any]) -> str | None:
  """Pull `REFRESH_RESULT=<x>` out of an invocation's stdout, if present."""
  for plugin in invocation.get("CommandPlugins", []):
    output = plugin.get("Output") or ""
    for line in output.splitlines():
      line = line.strip()
      if line.startswith("REFRESH_RESULT="):
        return line.split("=", 1)[1]
  return None


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
  """Dispatch on `event["action"]`."""
  action = event.get("action")

  try:
    if action == "start":
      return start(event)
    elif action == "status":
      return status(event)
    else:
      return {"statusCode": 400, "error": f"Unknown action: {action}"}
  except Exception as e:
    logger.error(f"Error in {action}: {e!s}", exc_info=True)
    return {"statusCode": 500, "error": str(e)}


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
  """AWS Lambda entry point"""
  return handler(event, context)
