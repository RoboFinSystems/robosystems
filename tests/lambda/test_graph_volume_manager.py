"""Tests for the graph volume manager Lambda's race-aware attach path.

Covers the four robustness fixes from the 2026-05-08 incident:
  1. Volume-available waiter runs before attach_volume.
  2. attach_volume retries on VolumeInUse (was previously only IncorrectState).
  3. The registry's `databases` field is preserved (not wiped) when the volume
     already has graphs allocated to it.
  4. graph-registry routing update uses the preserved databases list, so
     replacement instances reroute previously-allocated graphs automatically.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import boto3
import pytest
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_test_instance() -> str:
  """Spin up a moto EC2 instance and return its id (us-east-1c, m7g.large)."""
  ec2 = boto3.client("ec2", region_name="us-east-1")
  # moto auto-creates an AMI; just pass a known one.
  amis = ec2.describe_images(Owners=["amazon"])["Images"]
  ami_id = amis[0]["ImageId"]
  resp = ec2.run_instances(
    ImageId=ami_id,
    MinCount=1,
    MaxCount=1,
    InstanceType="m7g.large",
    Placement={"AvailabilityZone": "us-east-1c"},
  )
  return resp["Instances"][0]["InstanceId"]


def _create_test_volume(size: int = 50) -> str:
  ec2 = boto3.client("ec2", region_name="us-east-1")
  resp = ec2.create_volume(
    AvailabilityZone="us-east-1c",
    Size=size,
    VolumeType="gp3",
    Iops=3000,
    Throughput=125,
    Encrypted=True,
  )
  return resp["VolumeId"]


def _seed_registry(
  table_name: str,
  volume_id: str,
  databases: list[str],
  status: str = "available",
  node_type: str = "writer",
  tier: str = "ladybug-standard",
  instance_id: str = "unattached",
) -> None:
  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  ddb.Table(table_name).put_item(
    Item={
      "volume_id": volume_id,
      "instance_id": instance_id,
      "availability_zone": "us-east-1c",
      "tier": tier,
      "status": status,
      "databases": databases,
      "node_type": node_type,
    }
  )


def _seed_graph_registry(table_name: str, graph_id: str, instance_id: str) -> None:
  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  ddb.Table(table_name).put_item(
    Item={
      "graph_id": graph_id,
      "instance_id": instance_id,
      "private_ip": "10.0.0.99",
      "status": "active",
    }
  )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_attach_waits_for_volume_available_first(gvm):
  """The volume_available waiter must run before attach_volume.

  Without this, EBS detach being asynchronous lets us race the previous
  instance's detach and hit VolumeInUse — exactly today's failure mode.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [])

  call_order: list[str] = []
  real_get_waiter = gvm.ec2.get_waiter

  def tracking_get_waiter(name):
    call_order.append(f"waiter:{name}")
    return real_get_waiter(name)

  real_attach = gvm.ec2.attach_volume

  def tracking_attach(*args, **kwargs):
    call_order.append("attach_volume")
    return real_attach(*args, **kwargs)

  with (
    patch.object(gvm.ec2, "get_waiter", side_effect=tracking_get_waiter),
    patch.object(gvm.ec2, "attach_volume", side_effect=tracking_attach),
  ):
    gvm.attach_and_register_volume(volume_id, instance_id, [])

  # volume_available waiter must come before attach_volume.
  assert "waiter:volume_available" in call_order
  assert call_order.index("waiter:volume_available") < call_order.index("attach_volume")


def test_attach_retries_on_volume_in_use(gvm):
  """VolumeInUse on first attempt must trigger retry, not raise immediately.

  Today's incident: detachment Lambda ack'd, volume-manager called
  attach_volume ~30s later, EC2 returned VolumeInUse, and the old retry guard
  (only IncorrectState) skipped retries → instance launched without a volume.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [])

  vol_in_use = ClientError(
    {
      "Error": {
        "Code": "VolumeInUse",
        "Message": "Volume is already attached to an instance",
      }
    },
    "AttachVolume",
  )
  real_attach = gvm.ec2.attach_volume
  call_count = {"n": 0}

  def flaky_attach(*args, **kwargs):
    call_count["n"] += 1
    if call_count["n"] == 1:
      raise vol_in_use
    return real_attach(*args, **kwargs)

  # Patch sleep to keep the test fast (real path sleeps 30s on VolumeInUse).
  with (
    patch.object(gvm.ec2, "attach_volume", side_effect=flaky_attach),
    patch.object(gvm.time, "sleep") as mock_sleep,
  ):
    result = gvm.attach_and_register_volume(volume_id, instance_id, [])

  assert call_count["n"] == 2, "attach_volume should be called twice (1 fail + 1 OK)"
  assert result["statusCode"] == 200
  # Sleep should have been called with 30s for the VolumeInUse case.
  mock_sleep.assert_called_once_with(30)


def test_attach_raises_after_three_volume_in_use_attempts(gvm):
  """Persistent VolumeInUse should still fail loudly, not silently return null."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [])

  vol_in_use = ClientError(
    {"Error": {"Code": "VolumeInUse", "Message": "still detaching"}}, "AttachVolume"
  )

  with (
    patch.object(gvm.ec2, "attach_volume", side_effect=vol_in_use),
    patch.object(gvm.time, "sleep"),
    pytest.raises(ClientError),
  ):
    gvm.attach_and_register_volume(volume_id, instance_id, [])


def test_attach_preserves_existing_databases(gvm):
  """The registry's `databases` field must NOT be wiped when caller passes [].

  allocation_manager.py maintains this list when graphs are created/dropped.
  Userdata for non-shared writers always passes []; the old code overwrote the
  registry, breaking volume↔graph linkage on every replacement. The 2026-05-08
  incident orphaned kg19dcbe757481af06fc9b through this exact path.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, ["kgexistinggraph123"])

  gvm.attach_and_register_volume(volume_id, instance_id, [])

  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  item = ddb.Table("test-volume-registry").get_item(Key={"volume_id": volume_id})[
    "Item"
  ]
  assert item["databases"] == ["kgexistinggraph123"], (
    "Registry should preserve pre-existing databases list"
  )


def test_attach_adopts_caller_databases_when_registry_empty(gvm):
  """For a genuinely fresh pool volume, the caller's list should be adopted."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [])

  gvm.attach_and_register_volume(volume_id, instance_id, ["sec"])

  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  item = ddb.Table("test-volume-registry").get_item(Key={"volume_id": volume_id})[
    "Item"
  ]
  assert item["databases"] == ["sec"]


def test_attach_signals_instance_with_preserved_databases(gvm):
  """The SSM signal to the instance must include the preserved databases list.

  Without this, the OS-side /tmp/databases.json gets the caller's [] on every
  replacement — re-introducing the same orphaning bug at the OS level even
  though the registry write now preserves the list correctly.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  preserved = ["kgexistinggraph123"]
  _seed_registry("test-volume-registry", volume_id, preserved)

  with patch.object(gvm.ssm, "send_command") as mock_send:
    result = gvm.attach_and_register_volume(volume_id, instance_id, [])

  assert mock_send.called, "ssm.send_command should be invoked to signal the instance"
  call_kwargs = mock_send.call_args.kwargs
  commands = call_kwargs["Parameters"]["commands"]
  # commands[1] is the databases.json write
  assert "kgexistinggraph123" in commands[1], (
    "SSM signal must contain preserved databases, not the caller's empty list"
  )
  assert "[]" not in commands[1], (
    "SSM signal must NOT contain the empty caller list when registry has data"
  )

  # Function return value must also reflect the preserved list (callers may
  # parse it to register graphs).
  assert result["databases"] == preserved, (
    "Return value must use preserved databases list"
  )


def test_attach_reroutes_graph_registry_using_preserved_databases(gvm):
  """graph-registry must be updated with the preserved (not caller-provided) list.

  This is what makes the routing layer self-heal on replacement instances.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  graph_id = "kgpreexisting12345"
  _seed_registry("test-volume-registry", volume_id, [graph_id])
  _seed_graph_registry("test-graph-registry", graph_id, "i-old-dead-instance")

  gvm.attach_and_register_volume(volume_id, instance_id, [])

  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  item = ddb.Table("test-graph-registry").get_item(Key={"graph_id": graph_id})["Item"]
  assert item["instance_id"] == instance_id, (
    "graph-registry should now point at the new instance even though caller "
    "passed databases=[]"
  )


# ---------------------------------------------------------------------------
# Pre-detach snapshot behaviour
# ---------------------------------------------------------------------------


def _attach(volume_id: str, instance_id: str) -> None:
  boto3.client("ec2", region_name="us-east-1").attach_volume(
    VolumeId=volume_id, InstanceId=instance_id, Device="/dev/xvdf"
  )


def _moto_safe_detach_volume(gvm):
  """Wrap gvm.ec2.detach_volume to fill in InstanceId from attachment state.

  Real AWS infers InstanceId from the volume's current attachment, but moto
  crashes (`len(None)` on InvalidInstanceIdError) when InstanceId is omitted.
  This wrapper mirrors the real behavior locally.
  """
  real_detach = gvm.ec2.detach_volume

  def patched(**kwargs):
    if "InstanceId" not in kwargs:
      vol = gvm.ec2.describe_volumes(VolumeIds=[kwargs["VolumeId"]])["Volumes"][0]
      if vol["Attachments"]:
        kwargs["InstanceId"] = vol["Attachments"][0]["InstanceId"]
    return real_detach(**kwargs)

  return patch.object(gvm.ec2, "detach_volume", side_effect=patched)


def _describe_pre_detach_snapshots(volume_id: str) -> list[dict]:
  ec2 = boto3.client("ec2", region_name="us-east-1")
  resp = ec2.describe_snapshots(
    OwnerIds=["self"],
    Filters=[
      {"Name": "tag:Type", "Values": ["pre_detach"]},
      {"Name": "tag:VolumeId", "Values": [volume_id]},
    ],
  )
  return resp["Snapshots"]


def test_detach_creates_snapshot_for_writer_with_data(gvm):
  """Writer volume holding a graph must get a pre-detach snapshot."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _attach(volume_id, instance_id)
  _seed_registry(
    "test-volume-registry",
    volume_id,
    ["kgcustomer1234567"],
    status="attached",
    node_type="writer",
    tier="ladybug-standard",
    instance_id=instance_id,
  )

  with _moto_safe_detach_volume(gvm):
    gvm.detach_volume({"volume_id": volume_id})

  snaps = _describe_pre_detach_snapshots(volume_id)
  assert len(snaps) == 1
  tags = {t["Key"]: t["Value"] for t in snaps[0]["Tags"]}
  assert tags["AutoDelete"] == "true"
  assert tags["RetentionDays"] == "3"
  assert tags["NodeType"] == "writer"
  assert "kgcustomer1234567" in tags["Databases"]


def test_detach_creates_snapshot_for_shared_master(gvm):
  """Shared master holds SEC data — must be snapshotted."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume(size=300)
  _attach(volume_id, instance_id)
  _seed_registry(
    "test-volume-registry",
    volume_id,
    ["sec"],
    status="attached",
    node_type="shared_master",
    tier="ladybug-shared",
    instance_id=instance_id,
  )

  with _moto_safe_detach_volume(gvm):
    gvm.detach_volume({"volume_id": volume_id})

  snaps = _describe_pre_detach_snapshots(volume_id)
  assert len(snaps) == 1
  tags = {t["Key"]: t["Value"] for t in snaps[0]["Tags"]}
  assert tags["NodeType"] == "shared_master"
  assert "sec" in tags["Databases"]


def test_detach_skips_snapshot_for_shared_replica(gvm):
  """Shared replicas hydrate from S3 on boot — must NOT be snapshotted.

  In practice replicas don't even reach this code path (no /dev/xvdf, no
  TerminationLifecycleHook), but defense-in-depth: the filter must reject them
  if a future refactor wires them through.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _attach(volume_id, instance_id)
  _seed_registry(
    "test-volume-registry",
    volume_id,
    ["sec"],  # Even with data — replicas are still skipped.
    status="attached",
    node_type="shared_replica",
    tier="ladybug-shared",
    instance_id=instance_id,
  )

  with _moto_safe_detach_volume(gvm):
    gvm.detach_volume({"volume_id": volume_id})

  snaps = _describe_pre_detach_snapshots(volume_id)
  assert snaps == [], "shared_replica volume must not be snapshotted"


def test_detach_skips_snapshot_for_empty_pool_volume(gvm):
  """Volumes with no databases (spare pool) have nothing worth snapshotting."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _attach(volume_id, instance_id)
  _seed_registry(
    "test-volume-registry",
    volume_id,
    [],  # empty pool volume
    status="attached",
    node_type="writer",
    tier="ladybug-standard",
    instance_id=instance_id,
  )

  with _moto_safe_detach_volume(gvm):
    gvm.detach_volume({"volume_id": volume_id})

  snaps = _describe_pre_detach_snapshots(volume_id)
  assert snaps == [], "empty pool volume must not be snapshotted"


def test_detach_proceeds_when_snapshot_creation_fails(gvm):
  """A snapshot failure must not block the detach itself."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _attach(volume_id, instance_id)
  _seed_registry(
    "test-volume-registry",
    volume_id,
    ["kgcustomer1234567"],
    status="attached",
    node_type="writer",
    tier="ladybug-standard",
    instance_id=instance_id,
  )

  with (
    patch.object(
      gvm.ec2, "create_snapshot", side_effect=RuntimeError("snapshot api down")
    ),
    _moto_safe_detach_volume(gvm),
  ):
    result = gvm.detach_volume({"volume_id": volume_id})

  # Detach must have succeeded despite the snapshot failure.
  assert result["statusCode"] == 200
  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  item = ddb.Table("test-volume-registry").get_item(Key={"volume_id": volume_id})[
    "Item"
  ]
  assert item["status"] == "available"


def test_cleanup_snapshots_honors_retention_days_tag(gvm):
  """A snapshot tagged RetentionDays=3 must be deleted at 3 days, not 7.

  Without the per-snapshot tag, all AutoDelete=true snapshots used the global
  RETENTION_DAYS env (7 days), so pre-detach snapshots would linger beyond
  their intended 3-day window.
  """
  ec2 = boto3.client("ec2", region_name="us-east-1")
  vol_3day = _create_test_volume()
  vol_7day = _create_test_volume()

  s3 = ec2.create_snapshot(
    VolumeId=vol_3day,
    Description="3-day retention",
    TagSpecifications=[
      {
        "ResourceType": "snapshot",
        "Tags": [
          {"Key": "Environment", "Value": "test"},
          {"Key": "AutoDelete", "Value": "true"},
          {"Key": "RetentionDays", "Value": "3"},
        ],
      }
    ],
  )
  s7 = ec2.create_snapshot(
    VolumeId=vol_7day,
    Description="default retention (orphan-cleanup style)",
    TagSpecifications=[
      {
        "ResourceType": "snapshot",
        "Tags": [
          {"Key": "Environment", "Value": "test"},
          {"Key": "AutoDelete", "Value": "true"},
        ],
      }
    ],
  )

  # Move both snapshots' StartTime to 5 days ago.
  fake_now = datetime.now(UTC)
  fake_start = fake_now - timedelta(days=5)

  def fake_describe(*args, **kwargs):
    real = ec2.describe_snapshots(*args, **kwargs)
    for snap in real["Snapshots"]:
      snap["StartTime"] = fake_start
    return real

  with patch.object(gvm.ec2, "describe_snapshots", side_effect=fake_describe):
    result = gvm.cleanup_old_snapshots({})

  deleted = result["deleted_snapshots"]
  # The 3-day snapshot is past retention (5 > 3) → deleted.
  # The 7-day default snapshot is within retention (5 < 7) → kept.
  assert s3["SnapshotId"] in deleted
  assert s7["SnapshotId"] not in deleted
