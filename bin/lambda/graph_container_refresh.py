"""
Refresh graph API containers across the fleet with one tag-targeted SSM command.

The trigger and aggregator, not the worker: per-instance work lives in
`/usr/local/bin/refresh-graph-container.sh`, wrapped by the stack's
`GraphRefreshDocument` (graph-infra.yaml). SSM's ramp to the concurrency cap is
a free canary and `--max-errors` halts a bad rollout.

One command per node-type group: SSM ANDs across target keys, so writers and
replicas cannot share one expression (and want different rate control anyway).
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

# The stack-owned document wrapping refresh-graph-container.sh. Required: the
# failure-paging rule and the role's SendCommand grant are scoped to its name.
REFRESH_DOCUMENT = os.environ["REFRESH_DOCUMENT_NAME"]

# Defaults here so a hand-rolled invocation can't hit the fleet at SSM's default
# concurrency of 50. Conservative; raise on evidence.
DEFAULT_MAX_CONCURRENCY = "10%"
DEFAULT_MAX_ERRORS = "10%"

# The instance script's busy-wait ceiling. ExecutionTimeout must exceed it, so it
# is derived and passed on every dispatch.
DEFAULT_MAX_WAIT_MINUTES = 30
REFRESH_HEADROOM_SECONDS = 900

# Node-type groups, each its own tag expression. `writer` covers every writer
# tier (the tier is in `WriterTier`). Replicas match on `NodeType`, which every
# replica carries; `LadybugRole=replica` only reaches an instance when it boots.
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

# REFRESH_RESULT markers meaning "not yet on the new deployment", not "broken":
#   skipped-no-script — the instance predates the refresh script
#   skipped-stale-env — /etc/environment predates the contract (script exit 3)
# The document exits 0 for both so they don't spend MaxErrors. Matched on the
# marker, never an exit code: e.g. a missing `docker` (127) must stay a failure.
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
  """The document parameters that vary per dispatch."""
  return {
    "MaxWaitMinutes": [str(max_wait_minutes)],
    "ForceIgnoreBusy": ["true" if force_ignore_busy else "false"],
    "ForceRestart": ["true" if force_restart else "false"],
    "ExecutionTimeout": [str(execution_timeout)],
  }


def start(event: dict[str, Any]) -> dict[str, Any]:
  """Dispatch the refresh and return the command ids, one per node-type group.

  Returns immediately; poll `status` or let the EventBridge rule page on failures.
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

  Also counts each `REFRESH_RESULT`, so a restart, a digest no-op and a skip are
  distinguishable.
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
          # Never a failure, whatever the status (a hand-run of the raw script
          # reports a skip as Failed).
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
    # `failed` excludes skips, so a caller can fail on it directly.
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
  """The shell exit code, which SSM reports on the plugin, not the invocation.
  -1 (plugin never ran) is reported as empty."""
  for plugin in invocation.get("CommandPlugins", []):
    code = plugin.get("ResponseCode")
    if code is not None and code != -1:
      return str(code)
  return ""


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
  """Dispatch on `event["action"]`.

  Caller mistakes return 400-shaped payloads. Operational failures propagate so
  they reach the Lambda Errors metric that GraphContainerRefreshErrorsAlarm
  pages on.
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
