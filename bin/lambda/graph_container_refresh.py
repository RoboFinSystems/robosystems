"""
Refresh graph API containers across the fleet, one instance at a time.

The scheduler, not the worker: per-instance work lives in
`/usr/local/bin/refresh-graph-container.sh`, wrapped by the stack's
`GraphRefreshDocument` (graph-infra.yaml).

A refresh is a queue walked one instance at a time. `start` resolves the fleet
into a queue and dispatches the first idle instance; the caller then calls
`step` with the returned state until its phase is terminal. Each step reads the
in-flight command, records its outcome, and dispatches the next idle instance.
The Lambda holds nothing between calls, so it never outlives its timeout, and a
caller that stops stepping stops the walk after the in-flight instance.

- A busy instance (an in-flight materialization) is passed over and moves to the
  back of the queue, so the walk comes back to it after the rest of the fleet.
- A real failure stops the walk: nothing further is dispatched. Sequential and
  fail-fast on purpose — a bad image should halt at the first instance, not be
  pushed through the fleet.
- Instances still busy at the deadline end the walk as `deferred`.
"""

import logging
import os
import time
from datetime import datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")
ec2 = boto3.client("ec2")
dynamodb = boto3.client("dynamodb")

ENVIRONMENT = os.environ["ENVIRONMENT"]

# The stack-owned document wrapping refresh-graph-container.sh. Required: the
# failure-paging rule and the role's SendCommand grant are scoped to its name.
REFRESH_DOCUMENT = os.environ["REFRESH_DOCUMENT_NAME"]

INSTANCE_REGISTRY_TABLE = os.environ["INSTANCE_REGISTRY_TABLE"]

# How long the walk keeps coming back to busy instances, from `start`.
DEFAULT_MAX_WAIT_MINUTES = 30

# The busy check happens here, before dispatch; the instance repeats it for the
# minute between this read and the script's own. A busy instance reports
# `deferred-busy` and is re-queued rather than waiting on the instance.
INSTANCE_WAIT_MINUTES = 1
REFRESH_HEADROOM_SECONDS = 900
EXECUTION_TIMEOUT_SECONDS = INSTANCE_WAIT_MINUTES * 60 + REFRESH_HEADROOM_SECONDS

# How long SSM tries to deliver to an unreachable instance. Sequential, so an
# undeliverable instance holds up everything behind it for this long.
DELIVERY_TIMEOUT_SECONDS = 600

# Busy counter > 0 with a heartbeat older than this is a crashed writer. Mirrors
# STALE_WINDOW_SECONDS in refresh-graph-container.sh and
# wait-graph-writers-idle.sh; the three read the counter the same way.
STALE_WINDOW_SECONDS = 21600

# EC2 filters per node-type group. `writer` covers every writer tier (the tier is
# in `WriterTier`, e.g. `ladybug-shared`). Replicas match on `NodeType`, which
# every replica carries; `LadybugRole=replica` only reaches an instance at boot.
NODE_TYPE_FILTERS: dict[str, list[dict[str, Any]]] = {
  "writer": [{"Name": "tag:LadybugRole", "Values": ["writer"]}],
  "shared": [
    {"Name": "tag:LadybugRole", "Values": ["writer"]},
    {"Name": "tag:WriterTier", "Values": ["ladybug-shared"]},
  ],
  "shared-replicas": [{"Name": "tag:NodeType", "Values": ["shared_replica"]}],
}

# What `all` expands to, in walk order.
ALL_GROUPS = ["writer", "shared-replicas"]

# Walked last within its group: the tier most likely to be mid-materialization,
# and the one whose data every shared repository reads.
LAST_WRITER_TIER = "ladybug-shared"

PENDING_STATUSES = {"Pending", "InProgress", "Delayed", "Cancelling"}

# REFRESH_RESULT markers meaning "not yet on the new deployment", not "broken":
#   skipped-no-script — the instance predates the refresh script
#   skipped-stale-env — /etc/environment predates the contract (script exit 3)
# Matched on the marker, never an exit code: e.g. a missing `docker` (127) must
# stay a failure.
SKIP_RESULTS = {"skipped-no-script", "skipped-stale-env"}

# The instance found itself busy (script exit 4). Re-queued, never a failure.
DEFERRED_RESULT = "deferred-busy"

TERMINAL_PHASES = {"complete", "failed", "deferred"}

# SSM keeps 24,000 chars of stdout; a failure's reason is at the end.
FAILURE_OUTPUT_TAIL_CHARS = 1500


def _filters_for(group: str, environment: str) -> list[dict[str, Any]]:
  """EC2 filters selecting a node-type group's running instances."""
  if group not in NODE_TYPE_FILTERS:
    raise ValueError(
      f"Unknown node_type '{group}'; expected one of "
      f"{sorted([*NODE_TYPE_FILTERS, 'all'])}"
    )
  return [
    {"Name": "tag:Environment", "Values": [environment]},
    {"Name": "instance-state-name", "Values": ["running"]},
    *NODE_TYPE_FILTERS[group],
  ]


def _resolve_queue(groups: list[str], environment: str) -> list[str]:
  """Instance ids in walk order: group by group, the shared writer tier last."""
  queue: list[str] = []
  for group in groups:
    found: list[tuple[bool, str]] = []
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(Filters=_filters_for(group, environment)):
      for reservation in page.get("Reservations", []):
        for instance in reservation.get("Instances", []):
          tags = {t["Key"]: t["Value"] for t in instance.get("Tags", [])}
          found.append(
            (tags.get("WriterTier") == LAST_WRITER_TIER, instance["InstanceId"])
          )
    queue.extend(iid for _, iid in sorted(found) if iid not in queue)
  return queue


def _busy(instance_id: str) -> dict[str, str] | None:
  """The instance's in-flight destructive op, or None when it is idle.

  A coordination signal, not a guard, so it fails open like its twins: a missing
  row, an unreadable registry, a non-positive counter and a stale heartbeat all
  read as idle. The instance script repeats the check before touching anything.
  """
  try:
    item = dynamodb.get_item(
      TableName=INSTANCE_REGISTRY_TABLE,
      Key={"instance_id": {"S": instance_id}},
    ).get("Item")
  except ClientError as e:
    logger.warning(f"Registry read failed for {instance_id}; treating as idle: {e}")
    return None
  if not item:
    return None

  try:
    count = int(item.get("active_destructive_ops", {}).get("N", "0"))
  except ValueError:
    count = 0
  if count <= 0:
    return None

  last_at = item.get("last_destructive_op_at", {}).get("S", "")
  kind = item.get("last_destructive_op_kind", {}).get("S", "unknown")
  if last_at:
    try:
      age = time.time() - datetime.fromisoformat(last_at).timestamp()
    except ValueError:
      age = 0
    if age > STALE_WINDOW_SECONDS:
      logger.warning(
        f"Stale busy counter on {instance_id} (count={count}, kind={kind}, "
        f"last={last_at}); treating as crashed"
      )
      return None
  return {"count": str(count), "kind": kind, "last_at": last_at}


def _build_parameters(
  force_ignore_busy: bool, force_restart: bool
) -> dict[str, list[str]]:
  """The document parameters for one instance's refresh."""
  return {
    "MaxWaitMinutes": [str(INSTANCE_WAIT_MINUTES)],
    "ForceIgnoreBusy": ["true" if force_ignore_busy else "false"],
    "ForceRestart": ["true" if force_restart else "false"],
    "ExecutionTimeout": [str(EXECUTION_TIMEOUT_SECONDS)],
  }


def _in_flight_command(instance_id: str) -> str | None:
  """A refresh already running on the instance, adopted rather than re-sent.

  A step whose response never reached the caller is retried from the previous
  state, which would otherwise send a second refresh to the same instance.
  """
  response = ssm.list_commands(
    InstanceId=instance_id,
    Filters=[{"key": "DocumentName", "value": REFRESH_DOCUMENT}],
    MaxResults=10,
  )
  for command in response.get("Commands", []):
    if command.get("Status") in PENDING_STATUSES:
      return command["CommandId"]
  return None


def _dispatch(state: dict[str, Any], instance_id: str) -> None:
  command_id = _in_flight_command(instance_id)
  if command_id:
    state["current"] = {"instance_id": instance_id, "command_id": command_id}
    state["phase"] = "running"
    state["log"].append(
      f"{instance_id}: refresh already in flight (command {command_id})"
    )
    return

  response = ssm.send_command(
    DocumentName=REFRESH_DOCUMENT,
    InstanceIds=[instance_id],
    Comment=f"graph container refresh ({state['environment']}/{instance_id})"[:100],
    Parameters=_build_parameters(state["force_ignore_busy"], state["force_restart"]),
    TimeoutSeconds=DELIVERY_TIMEOUT_SECONDS,
  )
  command_id = response["Command"]["CommandId"]
  state["current"] = {"instance_id": instance_id, "command_id": command_id}
  state["phase"] = "running"
  state["log"].append(f"{instance_id}: refreshing (command {command_id})")


def _dispatch_next(state: dict[str, Any]) -> None:
  """Dispatch the first idle queued instance; busy ones move behind the rest."""
  if not state["queue"]:
    state["phase"] = "complete"
    return

  passed: list[str] = []
  remaining = list(state["queue"])
  while remaining:
    instance_id = remaining.pop(0)
    busy = None if state["force_ignore_busy"] else _busy(instance_id)
    if busy:
      if instance_id not in state["busy"]:
        state["log"].append(
          f"{instance_id}: busy ({busy['kind']}, last heartbeat "
          f"{busy['last_at'] or 'unknown'}) — coming back to it"
        )
      state["busy"][instance_id] = busy
      passed.append(instance_id)
      continue
    state["busy"].pop(instance_id, None)
    state["queue"] = remaining + passed
    _dispatch(state, instance_id)
    return

  state["queue"] = passed
  if time.time() >= state["deadline"]:
    state["phase"] = "deferred"
    state["log"].append(
      f"Deadline reached with {len(passed)} instance(s) still busy: {', '.join(passed)}"
    )
  else:
    state["phase"] = "waiting"


def _read_current(state: dict[str, Any]) -> bool:
  """Record the in-flight command's outcome. False while it is still running."""
  current = state["current"]
  try:
    inv = ssm.get_command_invocation(
      CommandId=current["command_id"], InstanceId=current["instance_id"]
    )
  except ClientError as e:
    # Briefly absent right after dispatch.
    if e.response.get("Error", {}).get("Code") == "InvocationDoesNotExist":
      return False
    raise

  inv_status = inv.get("Status", "Unknown")
  if inv_status in PENDING_STATUSES:
    return False

  instance_id = current["instance_id"]
  output = inv.get("StandardOutputContent") or ""
  result = _refresh_result(output)
  state["current"] = None

  if result == DEFERRED_RESULT:
    state["queue"].append(instance_id)
    state["log"].append(
      f"{instance_id}: became busy before the refresh — coming back to it"
    )
    return True

  if result in SKIP_RESULTS or inv_status == "Success":
    # A hand-run of the raw script reports a skip as Failed; the marker decides.
    state["outcomes"].append(
      {"instance_id": instance_id, "result": result or "unknown"}
    )
    state["log"].append(f"{instance_id}: {result or 'succeeded (no REFRESH_RESULT)'}")
    return True

  code = inv.get("ResponseCode")
  state["failure"] = {
    "instance_id": instance_id,
    "command_id": current["command_id"],
    "status": inv_status,
    "status_details": inv.get("StatusDetails", ""),
    "response_code": "" if code in (None, -1) else str(code),
    "output_tail": output[-FAILURE_OUTPUT_TAIL_CHARS:],
  }
  state["phase"] = "failed"
  state["log"].append(
    f"{instance_id}: FAILED ({inv_status}/{inv.get('StatusDetails', '')}) — "
    f"stopping; {len(state['queue'])} instance(s) not attempted"
  )
  return True


def _refresh_result(output: str) -> str | None:
  """Pull `REFRESH_RESULT=<x>` out of an invocation's stdout, if present."""
  for line in output.splitlines():
    line = line.strip()
    if line.startswith("REFRESH_RESULT="):
      return line.split("=", 1)[1]
  return None


def start(event: dict[str, Any]) -> dict[str, Any]:
  """Resolve the fleet into a queue and dispatch its first idle instance."""
  environment = event.get("environment", ENVIRONMENT)
  node_type = event.get("node_types", "writer")
  max_wait_minutes = int(event.get("max_wait_minutes", DEFAULT_MAX_WAIT_MINUTES))
  groups = ALL_GROUPS if node_type == "all" else [node_type]

  state: dict[str, Any] = {
    "environment": environment,
    "node_types": node_type,
    "force_ignore_busy": bool(event.get("force_ignore_busy", False)),
    "force_restart": bool(event.get("force_restart", False)),
    "deadline": int(time.time()) + max_wait_minutes * 60,
    "queue": _resolve_queue(groups, environment),
    "current": None,
    "outcomes": [],
    "busy": {},
    "failure": None,
    "phase": "running",
    "log": [],
  }
  total = len(state["queue"])
  logger.info(f"Refreshing {total} {node_type} instance(s) in {environment}")
  state["log"].append(
    f"Refreshing {total} {node_type} instance(s) in {environment}, one at a time"
  )
  _dispatch_next(state)
  return {"statusCode": 200, **state}


def step(event: dict[str, Any]) -> dict[str, Any]:
  """Advance the walk: read the in-flight instance, dispatch the next."""
  state = event.get("state")
  if not isinstance(state, dict) or "queue" not in state:
    return {"statusCode": 400, "error": "state (as returned by start/step) is required"}
  state = {k: v for k, v in state.items() if k != "statusCode"}
  state["log"] = []

  if state["phase"] in TERMINAL_PHASES:
    return {"statusCode": 200, **state}
  if state["current"] and not _read_current(state):
    return {"statusCode": 200, **state}
  if state["phase"] != "failed":
    _dispatch_next(state)
  return {"statusCode": 200, **state}


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
  """Dispatch on `event["action"]`.

  Caller mistakes return 400-shaped payloads. Operational failures propagate so
  they reach the Lambda Errors metric that GraphContainerRefreshErrorsAlarm
  pages on.
  """
  action = event.get("action")
  if action == "start":
    return start(event)
  if action == "step":
    return step(event)
  return {"statusCode": 400, "error": f"Unknown action: {action}"}


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
  """AWS Lambda entry point"""
  return handler(event, context)
