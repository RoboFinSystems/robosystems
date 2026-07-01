"""Wake/sleep ("parking") of the SEC shared-master EC2 instance.

Pure boto3 + DynamoDB-registry logic with no Dagster imports, so it stays
unit-testable and reusable. Drives the same scale-to-1 / scale-to-0 lifecycle
validated by hand in the 2026-06-30 dry-run, wired into the nightly SEC pipeline
as bookend ops.

Load-bearing invariant (confirmed in prod): the master ASG runs
``NewInstancesProtectedFromScaleIn=true``, so a scale-in is *cancelled* unless
per-instance scale-in protection is cleared first. ``sleep_master`` therefore
always clears protection before setting desired capacity to 0.

The dev/test skip lives in the Dagster asset layer (``master_wake`` /
``master_sleep``), not here — these functions always execute their logic so
they can be unit-tested against mocked boto3.
"""

import asyncio
import time
from typing import Any

import boto3

from robosystems.config import env
from robosystems.graph_api.client.factory import get_graph_client_for_instance
from robosystems.logger import logger

SEC_DATABASE = "sec"


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


def _sec_volume_attached_to(instance_id: str) -> bool:
  """True if the SEC data volume is ``attached`` to ``instance_id`` per registry."""
  table = _dynamodb_resource().Table(env.VOLUME_REGISTRY_TABLE)
  resp = table.scan()
  for item in resp.get("Items", []):
    if SEC_DATABASE in (item.get("databases") or []):
      return item.get("status") == "attached" and item.get("instance_id") == instance_id
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
  timeout_s: int = 900,
  poll_interval_s: int = 15,
) -> dict[str, Any]:
  """Poll until the shared master is fully ready, else raise ``MasterWakeTimeout``.

  Four independent signals must all hold: EC2 ``running``, the SEC volume
  reattached (registry ``status=attached``), the instance registered as a
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
      elif not _sec_volume_attached_to(instance_id):
        last_state = f"{instance_id}: sec volume not attached"
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


async def wake_master(*, timeout_s: int | None = None) -> dict[str, Any]:
  """Scale the shared master to 1 and wait until it is healthy."""
  asg_name = get_shared_master_asg_name()
  set_desired_capacity(asg_name, 1)
  result = await wait_for_master_healthy(
    asg_name, timeout_s=timeout_s or env.SHARED_MASTER_WAKE_TIMEOUT_S
  )
  return {"status": "awake", **result}


def sleep_master() -> dict[str, Any]:
  """Clear scale-in protection then scale the shared master to 0.

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
  return {"status": "asleep", "instance_id": instance_id}
