"""Wake/sleep ("parking") of the shared-master EC2 instance.

Pure boto3 + registry logic, no Dagster imports; the dev skip lives in the
asset layer (``master``).
"""

import asyncio
import time
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from robosystems.config import env
from robosystems.graph_api.client.factory import get_graph_client_for_instance
from robosystems.logger import logger

# Default shared repository whose data volume gates the wake health check.
# The shared master is SEC-only today; a second shared repo passes its own name.
DEFAULT_SHARED_DATABASE = "sec"

# gp3 performance of the shared data volume. Provisioned IOPS and throughput
# bill whether or not the volume is attached, so it is boosted for a run and
# parked at the free gp3 baseline while the master sleeps. BOOSTED matches the
# ladybug-shared floor in bin/lambda/graph_volume_manager.py, which raises the
# volume again on attach should a boost not have been applied.
BOOSTED_VOLUME_PERFORMANCE = {"Iops": 12000, "Throughput": 500}
PARKED_VOLUME_PERFORMANCE = {"Iops": 3000, "Throughput": 125}
# A 3000 -> 12000 IOPS change on the SEC volume took 51 minutes to complete.
VOLUME_MODIFICATION_WAIT_S = 3600


class MasterWakeTimeout(Exception):
  """Raised when the shared master does not reach a healthy state in time."""


def get_shared_master_asg_name() -> str:
  """Return the shared-master ASG name (config-driven, never hardcoded)."""
  return env.SHARED_MASTER_ASG_NAME


def _autoscaling_client():
  region = env.AWS_REGION
  if env.is_development() and env.AWS_ENDPOINT_URL:
    return boto3.client(
      "autoscaling", endpoint_url=env.AWS_ENDPOINT_URL, region_name=region
    )
  return boto3.client("autoscaling", region_name=region)


def _ec2_client():
  region = env.AWS_REGION
  if env.is_development() and env.AWS_ENDPOINT_URL:
    return boto3.client("ec2", endpoint_url=env.AWS_ENDPOINT_URL, region_name=region)
  return boto3.client("ec2", region_name=region)


def _dynamodb_resource():
  region = env.AWS_REGION
  if env.is_development() and env.AWS_ENDPOINT_URL:
    return boto3.resource(
      "dynamodb", endpoint_url=env.AWS_ENDPOINT_URL, region_name=region
    )
  return boto3.resource("dynamodb", region_name=region)


def set_desired_capacity(asg_name: str, desired: int) -> None:
  """Set the ASG desired capacity (immediate — no cooldown)."""
  _autoscaling_client().set_desired_capacity(
    AutoScalingGroupName=asg_name, DesiredCapacity=desired, HonorCooldown=False
  )
  logger.info(f"Set desired capacity of ASG {asg_name} to {desired}")


def describe_asg_instance_id(asg_name: str) -> str | None:
  """Return the first instance id in the ASG, or None if it has no instances."""
  resp = _autoscaling_client().describe_auto_scaling_groups(
    AutoScalingGroupNames=[asg_name]
  )
  groups = resp.get("AutoScalingGroups", [])
  if not groups:
    return None
  instances = groups[0].get("Instances", [])
  if not instances:
    return None
  return instances[0].get("InstanceId")


def _instance_is_running(instance_id: str) -> bool:
  resp = _ec2_client().describe_instances(InstanceIds=[instance_id])
  for reservation in resp.get("Reservations", []):
    for inst in reservation.get("Instances", []):
      return inst.get("State", {}).get("Name") == "running"
  return False


def _volume_attached_to(instance_id: str, database: str) -> bool:
  """True if ``database``'s data volume is attached to ``instance_id``.

  Scans all rows tagged with ``database`` and matches only a row bound to THIS
  instance — a transient stale row (e.g. an old row mid-reattach) must not shadow
  the real match, since that reattach race is exactly what this gate guards
  against.

  ``expanding`` counts as attached: the volume monitor stamps that status while
  an online EBS resize runs, and the volume stays attached and usable throughout
  (the resize never takes it offline). The monitor does not reset the status
  until the next detach/reattach, so if the master is kept awake across an
  auto-expansion, requiring exactly ``attached`` here would hang the wake on a
  volume that is in fact ready.
  """
  table = _dynamodb_resource().Table(env.VOLUME_REGISTRY_TABLE)
  for item in table.scan().get("Items", []):
    if (
      database in (item.get("databases") or [])
      and item.get("status") in ("attached", "expanding")
      and item.get("instance_id") == instance_id
    ):
      return True
  return False


def _master_registered_healthy(instance_id: str) -> str | None:
  """Return the private IP if the instance is a healthy shared_master, else None."""
  table = _dynamodb_resource().Table(env.INSTANCE_REGISTRY_TABLE)
  item = table.get_item(Key={"instance_id": instance_id}).get("Item")
  if not item:
    return None
  if item.get("node_type") != "shared_master" or item.get("status") != "healthy":
    return None
  return item.get("private_ip")


async def _graph_api_healthy(private_ip: str) -> bool:
  client = await get_graph_client_for_instance(private_ip)
  try:
    return bool(await client.health_check())
  except Exception as exc:  # connection refused while booting, etc.
    logger.info(f"Master /health not ready yet at {private_ip}: {exc}")
    return False
  finally:
    await client.close()


async def wait_for_master_healthy(
  asg_name: str,
  *,
  database: str = DEFAULT_SHARED_DATABASE,
  timeout_s: int = 900,
  poll_interval_s: int = 15,
) -> dict[str, Any]:
  """Poll until the shared master is fully ready, else raise ``MasterWakeTimeout``.

  Four independent signals must all hold: EC2 ``running``, the ``database``
  volume reattached (registry ``status=attached``), the instance registered as a
  healthy ``shared_master``, and a live graph-api ``/health``. The health check
  hits the instance directly by private IP rather than the 5-minute
  Redis-cached discovery, so a stale or not-yet-ready master can never
  green-light staging.
  """
  deadline = time.monotonic() + timeout_s
  last_state = "no-instance"
  while True:
    instance_id = describe_asg_instance_id(asg_name)
    if instance_id:
      if not _instance_is_running(instance_id):
        last_state = f"{instance_id}: not running"
      elif not _volume_attached_to(instance_id, database):
        last_state = f"{instance_id}: {database} volume not attached"
      else:
        private_ip = _master_registered_healthy(instance_id)
        if not private_ip:
          last_state = f"{instance_id}: not registered healthy"
        elif not await _graph_api_healthy(private_ip):
          last_state = f"{instance_id}: /health not ready"
        else:
          logger.info(f"Shared master {instance_id} ({private_ip}) is healthy")
          return {"instance_id": instance_id, "private_ip": private_ip}
    if time.monotonic() >= deadline:
      raise MasterWakeTimeout(
        f"Shared master did not become healthy within {timeout_s}s "
        f"(last state: {last_state})"
      )
    await asyncio.sleep(poll_interval_s)


async def wake_master(
  *, database: str = DEFAULT_SHARED_DATABASE, timeout_s: int | None = None
) -> dict[str, Any]:
  """Boost the data volume, let the change land, then scale the master to 1.

  The wait happens while the master is still asleep, so it costs time but no
  instance hours; the boost started with the download has usually landed by
  now, leaving little or nothing to wait for.
  """
  asg_name = get_shared_master_asg_name()
  set_volume_performance(BOOSTED_VOLUME_PERFORMANCE, database=database)
  volume = await wait_for_volume_modifications(database=database)
  set_desired_capacity(asg_name, 1)
  result = await wait_for_master_healthy(
    asg_name,
    database=database,
    timeout_s=timeout_s or env.SHARED_MASTER_WAKE_TIMEOUT_S,
  )
  return {"status": "awake", "volume": volume, **result}


def _find_shared_volumes(database: str) -> list[dict[str, Any]]:
  return (
    _ec2_client()
    .describe_volumes(
      Filters=[
        {"Name": "tag:Tier", "Values": ["ladybug-shared"]},
        {"Name": "tag:DatabaseId", "Values": [database]},
        {"Name": "tag:Environment", "Values": [env.ENVIRONMENT]},
      ]
    )
    .get("Volumes", [])
  )


def set_volume_performance(
  target: dict[str, int], *, database: str = DEFAULT_SHARED_DATABASE
) -> list[dict[str, Any]]:
  """Move ``database``'s shared data volume(s) to ``target`` IOPS/throughput.

  Never raises: a modification still in flight, or the EBS limit of four per
  volume per rolling 24 hours, is logged and left for the next call. Returns
  one outcome per volume found.
  """
  try:
    volumes = _find_shared_volumes(database)
  except (BotoCoreError, ClientError) as exc:
    logger.warning(f"Could not look up the {database} data volume: {exc}")
    return []
  if not volumes:
    logger.warning(f"No {database} shared data volume found to set {target}")

  outcomes = []
  for volume in volumes:
    volume_id = volume["VolumeId"]
    if volume.get("VolumeType") != "gp3":
      outcomes.append({"volume_id": volume_id, "result": "not_gp3"})
      continue
    changes = {k: v for k, v in target.items() if int(volume.get(k) or 0) != v}
    if not changes:
      outcomes.append({"volume_id": volume_id, "result": "unchanged"})
      continue
    try:
      _ec2_client().modify_volume(VolumeId=volume_id, **changes)
    except (BotoCoreError, ClientError) as exc:
      code = getattr(exc, "response", {}).get("Error", {}).get("Code", str(exc))
      logger.warning(f"Could not set {volume_id} to {changes}: {code}")
      outcomes.append({"volume_id": volume_id, "result": f"deferred: {code}"})
      continue
    logger.info(f"Set {volume_id} to {changes}")
    outcomes.append({"volume_id": volume_id, "result": "modified", **changes})
  return outcomes


async def wait_for_volume_modifications(
  *,
  database: str = DEFAULT_SHARED_DATABASE,
  timeout_s: int = VOLUME_MODIFICATION_WAIT_S,
  poll_interval_s: int = 30,
) -> str:
  """Wait until no change to ``database``'s shared data volume is in flight.

  Until a change completes the volume performs somewhere between the old and
  new spec. Never raises: a lookup failure or a timeout returns, and the wake
  proceeds on a volume that works, only slower.
  """
  deadline = time.monotonic() + timeout_s
  while True:
    try:
      volume_ids = [v["VolumeId"] for v in _find_shared_volumes(database)]
      if not volume_ids:
        return "no_volume"
      in_flight = (
        _ec2_client()
        .describe_volumes_modifications(
          Filters=[
            {"Name": "volume-id", "Values": volume_ids},
            {"Name": "modification-state", "Values": ["modifying", "optimizing"]},
          ]
        )
        .get("VolumesModifications", [])
      )
    except (BotoCoreError, ClientError) as exc:
      logger.warning(f"Could not check {database} volume modifications: {exc}")
      return "lookup_failed"
    if not in_flight:
      return "settled"
    progress = ", ".join(
      f"{m.get('VolumeId')} {m.get('ModificationState')} {m.get('Progress', 0)}%"
      for m in in_flight
    )
    if time.monotonic() >= deadline:
      logger.warning(f"Volume change still in flight after {timeout_s}s: {progress}")
      return "timed_out"
    logger.info(f"Waiting on volume change: {progress}")
    await asyncio.sleep(poll_interval_s)


def park_volume_if_master_asleep(
  *, database: str = DEFAULT_SHARED_DATABASE
) -> dict[str, Any]:
  """Park the shared data volume if the master is asleep and it was left boosted.

  The sleep parks it on every path that ends a run. This catches the ones that
  do not reach a sleep — a cancelled run, a chain that stalled before the wake —
  and only when the ASG is at 0 with no instance left, so it never touches a
  volume a master is using.
  """
  asg_name = get_shared_master_asg_name()
  groups = _autoscaling_client().describe_auto_scaling_groups(
    AutoScalingGroupNames=[asg_name]
  )["AutoScalingGroups"]
  if not groups:
    return {"status": "skipped", "reason": "asg_not_found"}
  if groups[0].get("DesiredCapacity", 0) or groups[0].get("Instances"):
    return {"status": "skipped", "reason": "master_awake"}
  return {
    "status": "checked",
    "volumes": set_volume_performance(PARKED_VOLUME_PERFORMANCE, database=database),
  }


def sleep_master() -> dict[str, Any]:
  """Clear scale-in protection, scale the shared master to 0, park its volume.

  Order is load-bearing: the ASG cancels scale-in while the instance is
  protected, so protection MUST be cleared before desired capacity drops.
  Idempotent — a clean no-op when no instance is running.
  """
  asg_name = get_shared_master_asg_name()
  instance_id = describe_asg_instance_id(asg_name)
  if instance_id:
    _autoscaling_client().set_instance_protection(
      InstanceIds=[instance_id],
      AutoScalingGroupName=asg_name,
      ProtectedFromScaleIn=False,
    )
    logger.info(f"Cleared scale-in protection on {instance_id} in ASG {asg_name}")
  else:
    logger.info(f"No instance in ASG {asg_name}; scaling to 0 anyway")
  set_desired_capacity(asg_name, 0)
  volumes = set_volume_performance(PARKED_VOLUME_PERFORMANCE)
  return {"status": "asleep", "instance_id": instance_id, "volumes": volumes}
