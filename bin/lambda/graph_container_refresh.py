"""
Refresh graph API containers across the fleet with one tag-targeted SSM command.

The per-instance work — wait for in-flight destructive ops, pull, skip on an
unchanged digest, swap, health-check, prune — lives on the instance in
`/usr/local/bin/refresh-graph-container.sh`. This function is the *trigger and
the aggregator, not the worker*: it dispatches the stack's own refresh document
(graph-infra.yaml `GraphRefreshDocument`, which wraps that script) at a tag
expression and reports back. No instance enumeration, no per-instance job, no
runner sleeping through a busy-wait.

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

# The stack-owned SSM document that wraps refresh-graph-container.sh. Dispatching
# our own document rather than AWS-RunShellScript is what lets the failure-paging
# EventBridge rule filter on document-name (the only environment- or
# purpose-specific field an SSM invocation event carries) and lets the role's
# SendCommand grant collapse to exactly this behavior. The wrapper shell — the
# presence guard and the skip-exit normalization — lives in the document, next to
# the rule that depends on it. Required, like ENVIRONMENT: falling back to the
# generic document would silently un-scope the paging rule.
REFRESH_DOCUMENT = os.environ["REFRESH_DOCUMENT_NAME"]

# Rate control defaults live here, not only in the caller, so a hand-rolled
# invocation cannot accidentally hit the fleet at SSM's default concurrency of
# 50. Deliberately conservative on concurrency: the ECR interface endpoints make
# a pull storm a throughput question rather than a NAT-cost one, but the fleet
# has never been pulled at high concurrency, so this should be raised on
# evidence rather than optimism.
DEFAULT_MAX_CONCURRENCY = "10%"
DEFAULT_MAX_ERRORS = "10%"

# The busy-wait ceiling the instance script honors. The document's
# ExecutionTimeout parameter must always exceed it, and is passed explicitly on
# every dispatch so that raising the wait cannot silently truncate the command.
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

# REFRESH_RESULT markers that mean "this instance has not picked up the new
# deployment yet" rather than "the refresh broke":
#
#   skipped-no-script — the refresh script is not on the instance at all
#                       (scripts install at boot; the instance predates them)
#   skipped-stale-env — script present, but /etc/environment predates the
#                       completeness contract (the script's exit 3)
#
# The document normalizes both to exit 0, because SSM counts every non-zero exit
# toward MaxErrors: a skip that exits non-zero burns the error budget and
# terminates the rest of the fleet's queued invocations, so a uniformly
# not-yet-cycled fleet could never even report itself. The stdout marker is the
# signal instead. Real failures keep their non-zero exits — MaxErrors still halts
# a bad rollout. Markers are matched exactly, never inferred from an exit code: a
# missing `docker` inside the script exits 127 and must stay a failure.
SKIP_RESULTS = {"skipped-no-script", "skipped-stale-env"}


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


def _build_parameters(
  max_wait_minutes: int,
  force_ignore_busy: bool,
  force_restart: bool,
  execution_timeout: int,
) -> dict[str, list[str]]:
  """The document parameters that vary per dispatch.

  The wrapper shell itself — the presence guard and the skip-exit
  normalization — lives in the document (graph-infra.yaml), not here: the
  document is the reviewable, IAM-scoped statement of what this function can
  make an instance do, and the paging rule filters on its name.
  """
  return {
    "MaxWaitMinutes": [str(max_wait_minutes)],
    "ForceIgnoreBusy": ["true" if force_ignore_busy else "false"],
    "ForceRestart": ["true" if force_restart else "false"],
    "ExecutionTimeout": [str(execution_timeout)],
  }


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
  parameters = _build_parameters(
    max_wait_minutes, force_ignore_busy, force_restart, execution_timeout
  )

  dispatched: list[dict[str, Any]] = []
  for group in groups:
    targets = _targets_for(group, environment)
    try:
      response = ssm.send_command(
        DocumentName=REFRESH_DOCUMENT,
        Targets=targets,
        Comment=f"graph container refresh ({environment}/{group})"[:100],
        Parameters=parameters,
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

        outcome = _refresh_result(inv)
        if outcome:
          results[outcome] = results.get(outcome, 0) + 1

        if outcome in SKIP_RESULTS:
          # Deliberately not a failure, whatever the invocation status says:
          # the container was left running its current image on purpose. Under
          # the current document skips arrive as Success (their exits are
          # normalized to 0); a Failed invocation carrying a skip marker is a
          # hand-run of the raw script, and gets the same grace.
          skipped += 1
          continue

        if inv_status in {"Failed", "TimedOut"}:
          failures.append(
            {
              "instance_id": inv.get("InstanceId", "unknown"),
              "status": inv_status,
              # Distinguishes "ran and failed" from "Terminated before running"
              # — the latter means MaxErrors tripped elsewhere in the fleet.
              "status_details": inv.get("StatusDetails", ""),
              "response_code": _response_code(inv),
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


def _response_code(invocation: dict[str, Any]) -> str:
  """The shell exit code, from where SSM actually reports it: on the plugin,
  never on the invocation itself. -1 means the plugin never ran (the invocation
  was terminated first) and is reported as empty, like a missing plugin."""
  for plugin in invocation.get("CommandPlugins", []):
    code = plugin.get("ResponseCode")
    if code is not None and code != -1:
      return str(code)
  return ""


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
  """Dispatch on `event["action"]`.

  Caller mistakes (unknown action, missing command_ids) return 400-shaped
  payloads. Operational failures propagate: an unhandled exception is what
  increments the AWS/Lambda Errors metric, which the stack's
  GraphContainerRefreshErrorsAlarm pages on. Swallowing them into a 500-shaped
  payload left that alarm blind — a failed dispatch surfaced only as a red job
  in a workflow someone had to be watching.
  """
  action = event.get("action")
  if action == "start":
    return start(event)
  if action == "status":
    return status(event)
  return {"statusCode": 400, "error": f"Unknown action: {action}"}


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
  """AWS Lambda entry point"""
  return handler(event, context)
