"""Tests for the volume detachment Lambda's lifecycle-hook handler.

Covers the wire-up gaps from the 2026-05-08 incident:
  1. Wait for volume_available (real EBS state) before signalling CONTINUE.
  2. Return ABANDON when the inner detach fails or the volume doesn't settle.
  3. Sweep the terminating instance's instance-registry entry so the
     graph-registry routing layer can't keep pointing at a dead IP.
"""

import json
from unittest.mock import MagicMock, patch

import boto3

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_attached_volume(instance_id: str | None = None) -> tuple[str, str]:
  """Create an EC2 instance + EBS volume and attach the volume to it.

  Returns (instance_id, volume_id). If instance_id is provided, only the
  volume is created and attached.
  """
  ec2 = boto3.client("ec2", region_name="us-east-1")
  if instance_id is None:
    amis = ec2.describe_images(Owners=["amazon"])["Images"]
    resp = ec2.run_instances(
      ImageId=amis[0]["ImageId"],
      MinCount=1,
      MaxCount=1,
      InstanceType="m7g.large",
      Placement={"AvailabilityZone": "us-east-1c"},
    )
    instance_id = resp["Instances"][0]["InstanceId"]

  vol = ec2.create_volume(AvailabilityZone="us-east-1c", Size=50, VolumeType="gp3")
  ec2.attach_volume(
    VolumeId=vol["VolumeId"], InstanceId=instance_id, Device="/dev/xvdf"
  )
  return instance_id, vol["VolumeId"]


def _seed_instance_registry(instance_id: str) -> None:
  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  ddb.Table("robosystems-graph-test-instance-registry").put_item(
    Item={
      "instance_id": instance_id,
      "private_ip": "10.0.0.99",
      "status": "healthy",
      "node_type": "writer",
    }
  )


def _make_lifecycle_event(instance_id: str) -> dict:
  return {
    "Records": [
      {
        "Sns": {
          "Message": json.dumps(
            {
              "EC2InstanceId": instance_id,
              "LifecycleHookName": "TerminationLifecycleHook",
              "AutoScalingGroupName": "test-writers-asg",
            }
          )
        }
      }
    ]
  }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_detach_waits_for_volume_available_before_continue(gvd):
  """The volume_available waiter must run after the inner detach call."""
  instance_id, volume_id = _create_attached_volume()
  _seed_instance_registry(instance_id)

  call_order: list[str] = []
  real_get_waiter = gvd.ec2.get_waiter

  def tracking_get_waiter(name):
    call_order.append(f"waiter:{name}")
    return real_get_waiter(name)

  vm_response = MagicMock()
  vm_response["Payload"].read.return_value = json.dumps({"statusCode": 200}).encode()

  def fake_invoke(**kwargs):
    call_order.append("vm_invoke")
    # Simulate the volume-manager's API-level detach.
    # Pass InstanceId explicitly: moto's detach_volume crashes on len(None)
    # when InstanceId is omitted, even though real AWS allows it.
    boto3.client("ec2", region_name="us-east-1").detach_volume(
      VolumeId=volume_id, InstanceId=instance_id
    )
    return vm_response

  asg_calls: list[dict] = []

  def fake_complete(**kwargs):
    call_order.append(f"complete:{kwargs.get('LifecycleActionResult')}")
    asg_calls.append(kwargs)
    return {}

  with (
    patch.object(gvd.ec2, "get_waiter", side_effect=tracking_get_waiter),
    patch.object(gvd.lambda_client, "invoke", side_effect=fake_invoke),
    patch.object(gvd.asg, "complete_lifecycle_action", side_effect=fake_complete),
  ):
    gvd.handler(_make_lifecycle_event(instance_id), context=None)

  # Order: vm_invoke → volume_available waiter → complete CONTINUE
  vm_idx = call_order.index("vm_invoke")
  waiter_idx = call_order.index("waiter:volume_available")
  complete_idx = next(i for i, c in enumerate(call_order) if c.startswith("complete:"))
  assert vm_idx < waiter_idx < complete_idx
  assert asg_calls[0]["LifecycleActionResult"] == "CONTINUE"


def test_detach_returns_abandon_on_inner_failure(gvd):
  """If volume-manager invoke returns non-200, lifecycle must signal ABANDON."""
  instance_id, _vol = _create_attached_volume()
  _seed_instance_registry(instance_id)

  vm_response = MagicMock()
  vm_response["Payload"].read.return_value = json.dumps(
    {"statusCode": 500, "error": "boom"}
  ).encode()

  asg_calls: list[dict] = []

  def fake_complete(**kwargs):
    asg_calls.append(kwargs)
    return {}

  with (
    patch.object(gvd.lambda_client, "invoke", return_value=vm_response),
    patch.object(gvd.asg, "complete_lifecycle_action", side_effect=fake_complete),
  ):
    gvd.handler(_make_lifecycle_event(instance_id), context=None)

  assert asg_calls[0]["LifecycleActionResult"] == "ABANDON"


def test_detach_returns_abandon_when_volume_does_not_become_available(gvd):
  """If the EBS waiter times out, ABANDON so the next launch doesn't race."""
  instance_id, volume_id = _create_attached_volume()
  _seed_instance_registry(instance_id)

  # Fake waiter that always raises (simulates volume stuck detaching).
  fake_waiter = MagicMock()
  fake_waiter.wait.side_effect = Exception("Waiter timed out")

  vm_response = MagicMock()
  vm_response["Payload"].read.return_value = json.dumps({"statusCode": 200}).encode()

  asg_calls: list[dict] = []

  with (
    patch.object(gvd.ec2, "get_waiter", return_value=fake_waiter),
    patch.object(gvd.lambda_client, "invoke", return_value=vm_response),
    patch.object(
      gvd.asg,
      "complete_lifecycle_action",
      side_effect=lambda **kw: asg_calls.append(kw) or {},
    ),
  ):
    gvd.handler(_make_lifecycle_event(instance_id), context=None)

  assert asg_calls[0]["LifecycleActionResult"] == "ABANDON"


def test_detach_cleans_instance_registry(gvd):
  """The terminating instance's row must be deleted from instance-registry."""
  instance_id, volume_id = _create_attached_volume()
  _seed_instance_registry(instance_id)

  vm_response = MagicMock()
  vm_response["Payload"].read.return_value = json.dumps({"statusCode": 200}).encode()

  def fake_invoke(**kwargs):
    # Pass InstanceId explicitly: moto's detach_volume crashes on len(None)
    # when InstanceId is omitted, even though real AWS allows it.
    boto3.client("ec2", region_name="us-east-1").detach_volume(
      VolumeId=volume_id, InstanceId=instance_id
    )
    return vm_response

  with (
    patch.object(gvd.lambda_client, "invoke", side_effect=fake_invoke),
    patch.object(gvd.asg, "complete_lifecycle_action", return_value={}),
  ):
    gvd.handler(_make_lifecycle_event(instance_id), context=None)

  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  item = ddb.Table("robosystems-graph-test-instance-registry").get_item(
    Key={"instance_id": instance_id}
  )
  assert "Item" not in item, "terminated instance should be swept from registry"


def test_detach_handles_instance_with_only_root_volume(gvd):
  """An instance with no data volumes should still complete cleanly."""
  ec2 = boto3.client("ec2", region_name="us-east-1")
  amis = ec2.describe_images(Owners=["amazon"])["Images"]
  resp = ec2.run_instances(
    ImageId=amis[0]["ImageId"], MinCount=1, MaxCount=1, InstanceType="m7g.large"
  )
  instance_id = resp["Instances"][0]["InstanceId"]
  _seed_instance_registry(instance_id)

  asg_calls: list[dict] = []
  with patch.object(
    gvd.asg,
    "complete_lifecycle_action",
    side_effect=lambda **kw: asg_calls.append(kw) or {},
  ):
    gvd.handler(_make_lifecycle_event(instance_id), context=None)

  # No data volumes → all_detached stays True → CONTINUE
  assert asg_calls[0]["LifecycleActionResult"] == "CONTINUE"
